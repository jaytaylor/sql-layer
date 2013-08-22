/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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
package com.foundationdb.server.store;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.SynchronizedNameGenerator;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.ais.model.validation.AISValidations;
import com.foundationdb.ais.protobuf.ProtobufReader;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.server.FDBTableStatusCache;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.AISTooLargeException;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.rowdata.RowDefCache;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.util.GrowableByteBuffer;
import com.foundationdb.KeyValue;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Keyspace Usage:
 * sm/
 * sm/ais/
 * sm/ais/generation => [current generation number]
 * sm/ais/pb
 * sm/ais/pb/[schema name] => [protobuf data]
 *
 * Transactionality:
 * - All consumers of getAis() do a full read of the sm/ais/generation key to determine the proper version.
 * - All DDL executors increment the generation while making the AIS changes
 * - Whenever a new AIS is read, the name generator and table version map is re-set
 * - Since there can be exactly one change to the generation at a time, all generated names and ids will be unique
 */
public class FDBSchemaManager extends AbstractSchemaManager implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(FDBSchemaManager.class);

    private static final String SM_PREFIX = "sm/";
    private static final String AIS_PREFIX = "ais/";
    private static final String AIS_GENERATION_KEY = "generation";
    private static final String AIS_PB_PREFIX = "pb/";
    private static final byte[] PACKED_GENERATION_KEY = Tuple.from(SM_PREFIX, AIS_PREFIX, AIS_GENERATION_KEY).pack();
    private static final Session.Key<AkibanInformationSchema> SESSION_AIS_KEY = Session.Key.named("AIS_KEY");
    private static final AkibanInformationSchema SENTINEL_AIS = new AkibanInformationSchema(Integer.MIN_VALUE);

    // TODO: versioning?

    private final FDBHolder holder;
    private final FDBTransactionService txnService;
    private final Object AIS_LOCK = new Object();

    private FDBTableStatusCache tableStatusCache;
    private RowDefCache rowDefCache;
    private AkibanInformationSchema curAIS;
    private NameGenerator nameGenerator;
    private AkibanInformationSchema memoryTableAIS;


    @Inject
    public FDBSchemaManager(ConfigurationService config,
                            SessionService sessionService,
                            FDBHolder holder,
                            TransactionService txnService) {
        super(config, sessionService, txnService);
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
        AkCollatorFactory.setUseKeyCoder(false);

        this.memoryTableAIS = new AkibanInformationSchema();
        this.tableStatusCache = new FDBTableStatusCache(holder.getDatabase(), txnService);
        this.rowDefCache = new RowDefCache(tableStatusCache);

        try(Session session = sessionService.createSession()) {
            transactionally(
                    session,
                    new ThrowingCallable<Void>() {
                        @Override
                        public Void runAndReturn(Session session) {
                            AkibanInformationSchema newAIS = loadAISFromStorage(session);
                            buildRowDefCache(session, newAIS);
                            FDBSchemaManager.this.curAIS = newAIS;
                            return null;
                        }
                    }
            );
        }

        this.nameGenerator = SynchronizedNameGenerator.wrap(new DefaultNameGenerator(curAIS));
        mergeNewAIS(curAIS);
    }

    @Override
    public void stop() {
        super.stop();
        this.tableStatusCache = null;
        this.rowDefCache = null;
        this.curAIS = null;
        this.nameGenerator = null;
        this.memoryTableAIS = null;
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
            validateAndFreeze(session, newAIS, true);
            for(String schema : schemaNames) {
                checkAndSerialize(txnService.getTransaction(session), byteBuffer, newAIS, schema);
            }
            buildRowDefCache(session, newAIS);
        } catch(BufferOverflowException e) {
            throw new AISTooLargeException(byteBuffer.getMaxBurstSize());
        }
    }

    @Override
    protected void unSavedAISChangeWithRowDefs(Session session, final AkibanInformationSchema newAIS) {
        //
        // The *after* commit callback below is acceptable because in the real system, this
        // this method is only called during startup or shutdown and those are both single threaded.
        //
        // If that ever changes, that needs adjusted. However, the worst that can happen is a read
        // of the new system table (or routine, etc) after the commit but before the callback fires
        // This does not affect any user data whatsoever.
        //
        //
        // To be more strict and prevent that possibility, checks below would handle it but require test workarounds:
        //
        //ServiceManager.State state = serviceManager.getState();
        //if((state != ServiceManager.State.STARTING) && (state != ServiceManager.State.STOPPING)) {
        //    throw new IllegalStateException("Unexpected unSaved change: " + serviceManager.getState());
        //}

        // A new generation isn't needed as we evict the current copy below and, as above, single threaded startup
        validateAndFreeze(session, newAIS, false);
        buildRowDefCache(session, newAIS);

        txnService.addCallback(session, TransactionService.CallbackType.COMMIT, new TransactionService.Callback() {
            @Override
            public void run(Session session, long timestamp) {
                synchronized(AIS_LOCK) {
                    saveMemoryTables(newAIS);
                    FDBSchemaManager.this.curAIS = SENTINEL_AIS;
                }
            }
        });
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
        } catch(RuntimeException e) {
            throw e;
        } catch(Exception e) {
            throw new AkibanInternalException("Unexpected", e);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
            session.close();
        }
    }

    @Override
    protected void clearTableStatus(Session session, UserTable table) {
        tableStatusCache.clearTableStatus(session, table);
    }

    @Override
    public AkibanInformationSchema getAis(Session session) {
        AkibanInformationSchema localAIS = session.get(SESSION_AIS_KEY);
        if(localAIS != null) {
            return localAIS;
        }
        long generation = getTransactionalGeneration(session);
        localAIS = curAIS;
        if(generation != localAIS.getGeneration()) {
            synchronized(AIS_LOCK) {
                // May have been waiting
                if(generation == curAIS.getGeneration()) {
                    localAIS = curAIS;
                } else {
                    localAIS = loadAISFromStorage(session);
                    buildRowDefCache(session, localAIS);
                    if(localAIS.getGeneration() > curAIS.getGeneration()) {
                        curAIS = localAIS;
                        mergeNewAIS(curAIS);
                    }
                }
            }
        }
        attachToSession(session, localAIS);
        return localAIS;
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

    private void validateAndFreeze(Session session, AkibanInformationSchema newAIS, boolean isNewGeneration) {
        newAIS.validate(AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary(); // TODO: Often redundant, cleanup

        TransactionState txn = txnService.getTransaction(session);
        long generation = getTransactionalGeneration(session);
        if(isNewGeneration) {
            ++generation;
            byte[] packedGen = Tuple.from(generation).pack();
            txn.setBytes(PACKED_GENERATION_KEY, packedGen);
        }

        newAIS.setGeneration(generation);
        newAIS.freeze();
        attachToSession(session, newAIS);
    }

    private void checkAndSerialize(TransactionState txn, GrowableByteBuffer buffer, AkibanInformationSchema newAIS, String schema) {
        saveProtobuf(txn, buffer, newAIS, schema);
    }

    private void saveProtobuf(TransactionState txn,
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

        byte[] packed = makePBTuple().add(schema).pack();
        if(newAIS.getSchema(schema) != null) {
            buffer.clear();
            new ProtobufWriter(buffer, selector).save(newAIS);
            buffer.flip();
            byte[] newValue = Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.limit());
            txn.setBytes(packed, newValue);
        } else {
            txn.getTransaction().clear(packed);
        }
    }

    private void saveMemoryTables(AkibanInformationSchema newAIS) {
        // Want *just* non-persisted memory tables and system routines
        this.memoryTableAIS = AISCloner.clone(newAIS, new ProtobufWriter.TableFilterSelector() {
            @Override
            public Columnar getSelected(Columnar columnar) {
                if(columnar.isTable() && ((UserTable)columnar).hasMemoryTableFactory()) {
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
        });
    }

    private void buildRowDefCache(Session session, AkibanInformationSchema newAIS) {
        tableStatusCache.detachAIS();
        // TODO: set only if greater
        rowDefCache.setAIS(session, newAIS);
    }

    private AkibanInformationSchema loadAISFromStorage(final Session session) {
        // Start with existing memory tables, merge in stored ones
        // TODO: Is this vulnerable to table ID conflicts if another node creates a persisted I_S table?
        final AkibanInformationSchema newAIS = AISCloner.clone(memoryTableAIS);

        TransactionState txn = txnService.getTransaction(session);
        ProtobufReader reader = new ProtobufReader(newAIS);
        Iterator<KeyValue> iterator = txn.getTransaction().getRange(Range.startsWith(makePBTuple().pack())).iterator();
        while(iterator.hasNext()) {
            KeyValue kv = iterator.next();
            byte[] storedAIS = kv.getValue();
            GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
            reader.loadBuffer(buffer);
        }
        reader.loadAIS();

        for(UserTable table : newAIS.getUserTables().values()) {
            // nameGenerator is only needed to generate hidden PK, which shouldn't happen here
            table.endTable(null);
        }

        validateAndFreeze(session, newAIS, false);
        return newAIS;
    }

    private long getTransactionalGeneration(Session session) {
        TransactionState txn = txnService.getTransaction(session);
        long generation = 0;
        byte[] packedGen = txn.getTransaction().get(PACKED_GENERATION_KEY).get();
        if(packedGen != null) {
            generation = Tuple.fromBytes(packedGen).getLong(0);
        }
        return generation;
    }

    private void mergeNewAIS(AkibanInformationSchema newAIS) {
        nameGenerator.mergeAIS(newAIS);
        tableVersionMap.claimExclusive();
        try {
            for(UserTable table : newAIS.getUserTables().values()) {
                int newValue = table.getVersion();
                Integer current = tableVersionMap.get(table.getTableId());
                if(current == null || newValue > current) {
                    tableVersionMap.put(table.getTableId(), newValue);
                } else if(newValue < current) {
                    LOG.warn("Encountered new AIS generation less than current: {} vs {}", newValue, current);
                }
            }
        } finally {
            tableVersionMap.releaseExclusive();
        }
    }

    private void attachToSession(Session session, AkibanInformationSchema ais) {
        AkibanInformationSchema prev = session.put(SESSION_AIS_KEY, ais);
        if(prev == null) {
            txnService.addCallback(session, TransactionService.CallbackType.END, CLEAR_SESSION_KEY_CALLBACK);
        }
    }


    //
    // Static helpers
    //

    private static Tuple makePBTuple() {
        return Tuple.from(SM_PREFIX, AIS_PREFIX, AIS_PB_PREFIX);
    }

    private static final TransactionService.Callback CLEAR_SESSION_KEY_CALLBACK = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            session.remove(SESSION_AIS_KEY);
        }
    };
}
