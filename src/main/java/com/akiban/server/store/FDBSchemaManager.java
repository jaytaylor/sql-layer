/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.server.store;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.SynchronizedNameGenerator;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.validation.AISValidations;
import com.akiban.ais.protobuf.ProtobufReader;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.server.FDBTableStatusCache;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.error.AISTooLargeException;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.util.ReadWriteMap;
import com.akiban.util.GrowableByteBuffer;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.google.inject.Inject;

import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FDBSchemaManager extends AbstractSchemaManager implements Service {
    private static final String SCHEMA_MANAGER_PREFIX = "sm/";
    private static final String AIS_KEY_PREFIX = "by/";
    private static final String AIS_PROTOBUF_PARENT_KEY = AIS_KEY_PREFIX + "PBAIS/";

    // TODO: Proto version?

    private final FDBHolder holder;
    private final FDBTransactionService txnService;
    private FDBTableStatusCache tableStatusCache;
    private RowDefCache rowDefCache;

    // TODO: all needs to go through the DB
    private AtomicInteger generationCounter = new AtomicInteger();
    private NameGenerator nameGenerator;
    private volatile AkibanInformationSchema curAIS;


    @Inject
    public FDBSchemaManager(ConfigurationService config,
                            SessionService sessionService,
                            FDBHolder holder,
                            TransactionService txnService) {
        super(config, sessionService);
        this.holder = holder;
        if(txnService instanceof FDBTransactionService) {
            this.txnService = (FDBTransactionService)txnService;
        } else {
            throw new IllegalStateException("May only be ran with FDBTransactionService");
        }
    }


    //
    // Service
    //

    @Override
    public void start() {
        super.start();

        this.generationCounter = new AtomicInteger();
        this.tableStatusCache = new FDBTableStatusCache(holder.getDatabase(), txnService);
        this.rowDefCache = new RowDefCache(tableStatusCache);

        AkCollatorFactory.setUseKeyCoder(false);

        try(Session session = sessionService.createSession()) {
            transactionally(
                    session,
                    new ThrowingCallable<Void>() {
                        @Override
                        public Void runAndReturn(Session session) {
                            AkibanInformationSchema newAIS = loadAISFromStorage(session);
                            buildRowDefCache(session, newAIS);
                            return null;
                        }
                    }
            );
        }

        this.nameGenerator = SynchronizedNameGenerator.wrap(new DefaultNameGenerator(curAIS));
        this.tableVersionMap = ReadWriteMap.wrapNonFair(new HashMap<Integer,Integer>());
        for(UserTable table : curAIS.getUserTables().values()) {
            tableVersionMap.put(table.getTableId(), table.getVersion());
        }
    }

    @Override
    public void stop() {
        super.stop();
        this.tableStatusCache = null;
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // SchemaManager
    //

    @Override
    protected NameGenerator getNameGenerator() {
        return nameGenerator;
    }

    @Override
    protected void saveAISChangeWithRowDefs(Session session,
                                            AkibanInformationSchema newAIS,
                                            Collection<String> schemaNames) {
        GrowableByteBuffer byteBuffer = newByteBufferForSavingAIS();
        try {
            validateAndFreeze(session, newAIS);
            for(String schema : schemaNames) {
                checkAndSerialize(txnService.getTransaction(session), byteBuffer, newAIS, schema);
            }
            buildRowDefCache(session, newAIS);
            //addCallbacksForAISChange(session);
        } catch(BufferOverflowException e) {
            throw new AISTooLargeException(byteBuffer.getMaxBurstSize());
        }
    }

    @Override
    protected void serializeMemoryTables(Session session, AkibanInformationSchema newAIS) {
        /*
        GrowableByteBuffer byteBuffer = newByteBufferForSavingAIS();
        try {
            saveMemoryTables(txnService.getTransaction(session), byteBuffer, newAIS);
        } catch(BufferOverflowException e) {
            throw new AISTooLargeException(byteBuffer.getMaxBurstSize());
        }
        */
    }

    @Override
    protected void unSavedAISChangeWithRowDefs(Session session, AkibanInformationSchema newAIS) {
        validateAndFreeze(session, newAIS);
        serializeMemoryTables(session, newAIS);
        buildRowDefCache(session, newAIS);
        //addCallbacksForAISChange(session);
    }

    @Override
    protected <V> V transactionally(Session session, ThrowingCallable<V> callable) {
        try(TransactionService.CloseableTransaction txn = txnService.beginCloseableTransaction(session)) {
            V value;
            while (true) {
                value = callable.runAndReturn(session);
                if (!txn.commitOrRetry()) break;
            }
            return value;
        } catch(Exception e) {
            throw new AkibanInternalException("unexpected", e);
        }
    }

    @Override
    protected void deleteTableStatuses(Session session, Collection<Integer> tableIDs) {
        for(Integer id : tableIDs) {
            tableStatusCache.deleteTableStatus(session, id);
        }
    }

    @Override
    public AkibanInformationSchema getAis(Session session) {
        return curAIS;
    }

    @Override
    public boolean treeRemovalIsDelayed() {
        return false;
    }

    @Override
    public void treeWasRemoved(Session session, String schemaName, String treeName) {
        // None
    }

    @Override
    public long getOldestActiveAISGeneration() {
        return curAIS.getGeneration();
    }


    //
    // Helpers
    //

    private GrowableByteBuffer newByteBufferForSavingAIS() {
        int maxSize = maxAISBufferSize == 0 ? Integer.MAX_VALUE : maxAISBufferSize;
        return new GrowableByteBuffer(4096, 4096, maxSize);
    }

    private void validateAndFreeze(Session session, AkibanInformationSchema newAIS) {
        newAIS.validate(AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary(); // TODO: Often redundant, cleanup
        int generation = generationCounter.incrementAndGet();
        newAIS.setGeneration(generation);
        newAIS.freeze();
        // TODO: Something, something, transactional
    }

    private void checkAndSerialize(Transaction txn, GrowableByteBuffer buffer, AkibanInformationSchema newAIS, String schema) {
        if(serializationType == SerializationType.NONE) {
            serializationType = DEFAULT_SERIALIZATION;
        }
        if(serializationType == SerializationType.PROTOBUF) {
            saveProtobuf(txn, buffer, newAIS, schema);
        } else {
            throw new IllegalStateException("Cannot serialize as " + serializationType);
        }
    }

    private void saveProtobuf(Transaction txn,
                              GrowableByteBuffer buffer,
                              AkibanInformationSchema newAIS,
                              final String schema) {
        final ProtobufWriter.WriteSelector selector;
        if(TableName.INFORMATION_SCHEMA.equals(schema) || TableName.SECURITY_SCHEMA.equals(schema)) {
            selector = new ProtobufWriter.SingleSchemaSelector(schema) {
                @Override
                public Columnar getSelected(Columnar columnar) {
                    if(columnar.isTable() && ((UserTable)columnar).hasMemoryTableFactory()) {
                        return null;
                    }
                    return columnar;
                }
            };
        } else if(TableName.SYS_SCHEMA.equals(schema) || TableName.SQLJ_SCHEMA.equals(schema)) {
            selector = new ProtobufWriter.SingleSchemaSelector(schema) {
                @Override
                public boolean isSelected(Routine routine) {
                    return false;
                }
            };
        } else {
            selector = new ProtobufWriter.SingleSchemaSelector(schema);
        }

        Tuple tuple = Tuple.from(SCHEMA_MANAGER_PREFIX, AIS_PROTOBUF_PARENT_KEY, schema);
        byte[] packed = tuple.pack();
        if(newAIS.getSchema(schema) != null) {
            buffer.clear();
            new ProtobufWriter(buffer, selector).save(newAIS);
            buffer.flip();

            byte[] newValue = Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.limit());
            txn.set(packed, newValue);
        } else {
            txn.clear(packed);
        }
    }

    private void saveMemoryTables(Transaction txn, GrowableByteBuffer buffer, AkibanInformationSchema newAIS) {
        // Want *just* non-persisted memory tables and system routines
        final ProtobufWriter.WriteSelector selector = new ProtobufWriter.TableFilterSelector() {
            @Override
            public Columnar getSelected(Columnar columnar) {
                if(columnar.isAISTable() && ((UserTable)columnar).hasMemoryTableFactory()) {
                    return columnar;
                }
                return null;
            }

            @Override
            public boolean isSelected(Sequence sequence) {
                return false;
            }

            @Override
            public boolean isSelected(Routine routine) {
                return TableName.SYS_SCHEMA.equals(routine.getName().getSchemaName()) ||
                        TableName.SQLJ_SCHEMA.equals(routine.getName().getSchemaName()) ||
                        TableName.SECURITY_SCHEMA.equals(routine.getName().getSchemaName());
            }

            @Override
            public boolean isSelected(SQLJJar sqljJar) {
                return false;
            }
        };

        buffer.clear();
        new ProtobufWriter(buffer, selector).save(newAIS);
        buffer.flip();
    }

    private void buildRowDefCache(Session session, AkibanInformationSchema newAIS) {
        tableStatusCache.detachAIS();
        rowDefCache.setAIS(session, newAIS);
        curAIS = newAIS;
    }

    private AkibanInformationSchema loadAISFromStorage(final Session session) {
        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        Transaction txn = txnService.getTransaction(session);

        // User tables
        Tuple tuple = Tuple.from(SCHEMA_MANAGER_PREFIX, AIS_PROTOBUF_PARENT_KEY);
        Iterator<KeyValue> iterator = txn.getRangeStartsWith(tuple.pack()).iterator();
        while(iterator.hasNext()) {
            KeyValue kv = iterator.next();
            //checkAndSetSerialization(typeForVolume);
            loadProtobuf(kv, newAIS);
        }

        // TODO: Memory tables

        for(Map.Entry<TableName,MemoryTableFactory> entry : memoryTableFactories.entrySet()) {
            UserTable table = newAIS.getUserTable(entry.getKey());
            if(table != null) {
                table.setMemoryTableFactory(entry.getValue());
            }
        }

        validateAndFreeze(session, newAIS);
        return newAIS;
    }

    private static void loadProtobuf(KeyValue kv, AkibanInformationSchema newAIS) {
        ProtobufReader reader = new ProtobufReader(newAIS);
        byte[] storedAIS = kv.getValue();
        GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
        reader.loadBuffer(buffer);
        reader.loadAIS();
    }
}
