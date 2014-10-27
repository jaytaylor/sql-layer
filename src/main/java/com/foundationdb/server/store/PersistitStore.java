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

import com.foundationdb.ais.model.*;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.loadableplan.std.PersistitCLILoadablePlan;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.WriteIndexRow;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.storeadapter.indexrow.SpatialColumnHandler;
import com.foundationdb.server.AccumulatorAdapter;
import com.foundationdb.server.AccumulatorAdapter.AccumInfo;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.error.TableVersionChangedException;
import com.foundationdb.server.rowdata.*;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.service.transaction.TransactionService.Callback;
import com.foundationdb.server.service.transaction.TransactionService.CallbackType;
import com.foundationdb.server.service.tree.TreeService;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.PersistitStorageDescription;
import com.foundationdb.server.store.format.protobuf.PersistitProtobufRow;
import com.foundationdb.server.store.format.protobuf.PersistitProtobufValueCoder;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.google.inject.Inject;
import com.persistit.*;
import com.persistit.encoding.CoderManager;
import com.persistit.exception.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PersistitStore extends AbstractStore<PersistitStore,Exchange,PersistitStorageDescription> implements Service
{
    private static final Logger LOG = LoggerFactory.getLogger(PersistitStore.class);
    private static final String WRITE_LOCK_ENABLED_CONFIG = "fdbsql.write_lock_enabled";
    private static final Session.MapKey<Integer,Integer> SESSION_TABLES_KEY = Session.MapKey.mapNamed("WROTE_TABLES");

    private final ConfigurationService config;
    private final TreeService treeService;
    private final TransactionService txnService;
    private final CheckTableVersions checkTableVersionsCallback;

    private boolean writeLockEnabled;
    private RowDataValueCoder rowDataValueCoder;
    private PersistitProtobufValueCoder protobufValueCoder;

    @Inject
    public PersistitStore(TransactionService txnService,
                          TreeService treeService,
                          ConfigurationService config,
                          SchemaManager schemaManager,
                          ListenerService listenerService,
                          TypesRegistryService typesRegistryService,
                          ServiceManager serviceManager) {
        super(txnService, schemaManager, listenerService, typesRegistryService, serviceManager);
        if(!(schemaManager instanceof PersistitStoreSchemaManager)) {
            throw new IllegalArgumentException("PersistitStoreSchemaManager required, found: " + schemaManager.getClass());
        }
        this.treeService = treeService;
        this.config = config;
        this.checkTableVersionsCallback = new CheckTableVersions(schemaManager);
        this.txnService = txnService;
    }

    @Override
    public synchronized void start() {
        CoderManager cm = getDb().getCoderManager();
        cm.registerValueCoder(RowData.class, rowDataValueCoder = new RowDataValueCoder());
        cm.registerValueCoder(PersistitProtobufRow.class, protobufValueCoder = new PersistitProtobufValueCoder(this));
        boolean withConcurrentDML = false;
        if (config != null) {
            writeLockEnabled = Boolean.parseBoolean(config.getProperty(WRITE_LOCK_ENABLED_CONFIG));
            withConcurrentDML = Boolean.parseBoolean(config.getProperty(FEATURE_DDL_WITH_DML_PROP));
        }
        this.constraintHandler = new PersistitConstraintHandler(this, config, typesRegistryService, serviceManager, (PersistitTransactionService)txnService);
        this.onlineHelper = new OnlineHelper(txnService, schemaManager, this, typesRegistryService, constraintHandler, withConcurrentDML);
        listenerService.registerRowListener(onlineHelper);

        // System routine
        NewAISBuilder aisb = AISBBasedBuilder.create(schemaManager.getTypesTranslator());
        aisb.procedure(TableName.SYS_SCHEMA, "persistitcli")
            .language("java", Routine.CallingConvention.LOADABLE_PLAN)
            .paramStringIn("command", 1024)
            .externalName(PersistitCLILoadablePlan.class.getCanonicalName());
        for(Routine proc : aisb.ais().getRoutines().values()) {
            schemaManager.registerSystemRoutine(proc);
        }
    }

    @Override
    public synchronized void stop() {
        getDb().getCoderManager().unregisterValueCoder(RowData.class);
    }

    @Override
    public void crash() {
        stop();
    }

    @Override
    public Key createKey() {
        return new Key(treeService.getDb());
    }
    
    @Override
    public HKey newHKey (com.foundationdb.ais.model.HKey hkey) {
        return null;
    }

    public Persistit getDb() {
        return treeService.getDb();
    }

    public Exchange getExchange(Session session, Group group) {
        return createStoreData(session, group);
    }

    public Exchange getExchange(final Session session, final RowDef rowDef) {
        return createStoreData(session, rowDef.getGroup());
    }

    public Exchange getExchange(final Session session, final Index index) {
        return createStoreData(session, index);
    }

    public void releaseExchange(final Session session, final Exchange exchange) {
        releaseStoreData(session, exchange);
    }

    private void constructIndexRow(Session session,
                                   Exchange exchange,
                                   RowData rowData,
                                   Index index,
                                   Key hKey,
                                   WriteIndexRow indexRow,
                                   SpatialColumnHandler spatialColumnHander,
                                   long zValue,
                                   boolean forInsert) throws PersistitException
    {
        indexRow.resetForWrite(index, exchange.getKey(), exchange.getValue());
        indexRow.initialize(rowData, hKey, spatialColumnHander, zValue);
        indexRow.close(session, this, forInsert);
    }

    public RowDataValueCoder getRowDataValueCoder() {
        return rowDataValueCoder;
    }
    public PersistitProtobufValueCoder getProtobufValueCoder() {
        return protobufValueCoder;
    }

    @Override
    protected Exchange createStoreData(Session session, PersistitStorageDescription storageDescription) {
        return treeService.getExchange(session, storageDescription);
    }

    @Override
    protected void releaseStoreData(Session session, Exchange exchange) {
        treeService.releaseExchange(session, exchange);
    }

    @Override
    PersistitStorageDescription getStorageDescription(Exchange exchange) {
        return (PersistitStorageDescription)exchange.getAppCache();
    }

    @Override
    public IndexRow readIndexRow(Session session,
                                                Index parentPKIndex,
                                                Exchange exchange,
                                                RowDef childRowDef,
                                                RowData childRowData)
    {
        PersistitKeyAppender keyAppender = PersistitKeyAppender.create(exchange.getKey(), parentPKIndex.getIndexName());
        int[] fields = childRowDef.getParentJoinFields();
        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            FieldDef fieldDef = childRowDef.getFieldDef(fields[fieldIndex]);
            keyAppender.append(fieldDef, childRowData);
        }
        try {
            
            PersistitIndexRowBuffer indexRow = null;
            // Method only called if looking up a pk for which there is at least one
            // column missing from child row. Key contains the logical parent of it.
            if(exchange.hasChildren()) {
                exchange.next(true);
                indexRow = new PersistitIndexRowBuffer(this);
                indexRow.resetForRead(parentPKIndex, exchange.getKey(), null);
            }
            return indexRow;
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    // --------------------- Implement Store interface --------------------

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            truncateTree(session, index);
        }
    }

    @Override
    public void writeIndexRow(Session session,
                              TableIndex index,
                              RowData rowData,
                              Key hKey,
                              WriteIndexRow indexRow,
                              SpatialColumnHandler spatialColumnHandler,
                              long zValue,
                              boolean doLock) {
        Exchange iEx = getExchange(session, index);
        try {
            if(doLock) {
                lockKeysForIndex(session, index, rowData);
            }
            constructIndexRow(session, iEx, rowData, index, hKey, indexRow, spatialColumnHandler, zValue, true);
            checkUniqueness(session, rowData, index, iEx);
            iEx.store();
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, iEx);
        }
    }

    private void checkUniqueness(Session session, RowData rowData, Index index, Exchange iEx) throws PersistitException
    {
        if (index.isUnique() && !hasNullIndexSegments(rowData, index)) {
            Key key = iEx.getKey();
            int nColumns = index.getKeyColumns().size();
            final boolean existed;
            if (nColumns < key.getDepth()) {
                KeyState orig = new KeyState(key);
                key.setDepth(nColumns);
                existed = iEx.hasChildren();
                // prevent write skew for concurrent checks
                iEx.lock();
                orig.copyTo(key);
            } else {
                existed = iEx.traverse(Key.Direction.EQ, true, -1);
            }
            if (existed) {
                LOG.debug("Duplicate key for index {}, raw: {}", index.getIndexName(), key);
                String msg = formatIndexRowString(session, rowData, index);
                throw new DuplicateKeyException(index.getIndexName(), msg);
            }
        }
    }

    private void deleteIndexRow(Session session,
                                TableIndex index,
                                Exchange exchange,
                                RowData rowData,
                                Key hKey,
                                WriteIndexRow indexRowBuffer,
                                SpatialColumnHandler spatialColumnHandler,
                                long zValue)
        throws PersistitException
    {
        constructIndexRow(session, exchange, rowData, index, hKey, indexRowBuffer, spatialColumnHandler, zValue, false);
        if(!exchange.remove()) {
            LOG.debug("Index {} had no entry for hkey {}", index, hKey);
        }
    }

    @Override
    public void deleteIndexRow(Session session,
                               TableIndex index,
                               RowData rowData,
                               Key hKey,
                               WriteIndexRow buffer,
                               SpatialColumnHandler spatialColumnHandler,
                               long zValue,
                               boolean doLock) {
        Exchange iEx = getExchange(session, index);
        try {
            if(doLock) {
                lockKeysForIndex(session, index, rowData);
            }
            deleteIndexRow(session, index, iEx, rowData, hKey, buffer, spatialColumnHandler, zValue);
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, iEx);
        }
    }

    @Override
    public void store(Session session, Exchange ex) {
        try {
            ex.store();
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    protected boolean fetch(Session session, Exchange ex) {
        try {
            // ex.isValueDefined() doesn't actually fetch the value
            // ex.fetch() + ex.getValue().isDefined() would give false negatives (i.e. stored key with no value)
            return ex.traverse(Key.Direction.EQ, true, Integer.MAX_VALUE);
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    protected void clear(Session session, Exchange ex) {
        try {
            ex.remove();
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    void resetForWrite(Exchange ex, Index index, WriteIndexRow indexRowBuffer) {
        indexRowBuffer.resetForWrite(index, ex.getKey(), ex.getValue());
    }

    @Override
    protected Iterator<Void> createDescendantIterator(final Session session, final Exchange ex) {
        final Key hKey = ex.getKey();
        final KeyFilter filter = new KeyFilter(hKey, hKey.getDepth() + 1, Integer.MAX_VALUE);
        return new Iterator<Void>() {
            private Boolean lastExNext = null;

            @Override
            public boolean hasNext() {
                if(lastExNext == null) {
                    next();
                }
                return lastExNext;
            }

            @Override
            public Void next() {
                if(lastExNext != null) {
                    lastExNext = null;
                } else {
                    try {
                        lastExNext = ex.next(filter);
                    } catch(PersistitException | RollbackException e) {
                        throw PersistitAdapter.wrapPersistitException(session, e);
                    }
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected void lock(Session session, Exchange storeData, RowDef rowDef, RowData rowData) {
        try {
            lockKeysForTable(rowDef, rowData, storeData);
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    /** Table IDs for every table written to in the current transaction. Checked pre-commit. */
    @Override
    protected void trackTableWrite(Session session, Table table) {
        Map<Integer, Integer> map = session.get(SESSION_TABLES_KEY);
        if(map == null) {
            map = new HashMap<>();
            session.put(SESSION_TABLES_KEY, map);
        }
        if(map.isEmpty()) {
            txnService.addCallback(session, CallbackType.PRE_COMMIT, checkTableVersionsCallback);
            txnService.addCallback(session, CallbackType.END, CLEAR_SESSION_TABLES_CALLBACK);
        }
        map.put(table.getTableId(), null);
    }

    @Override
    protected Key getKey(Session session, Exchange exchange) {
        return exchange.getKey();
    }

    @Override
    public void deleteSequences (Session session, Collection<? extends Sequence> sequences) {
        removeTrees(session, sequences);
    }

    @Override
    public void traverse(Session session, Group group, TreeRecordVisitor visitor) {
        Exchange exchange = getExchange(session, group);
        try {
            exchange.clear().append(Key.BEFORE);
            visitor.initialize(session, this);
            while(exchange.next(true)) {
                RowData rowData = new RowData();
                expandRowData(session, exchange, rowData);
                visitor.visit(exchange.getKey(), rowData);
            }
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, exchange);
        }
    }

    @Override
    public <V extends IndexVisitor<Key,Value>> V traverse(Session session, Index index, V visitor, long scanTimeLimit, long sleepTime) {
        long nextCommitTime = 0;
        if (scanTimeLimit >= 0) {
            nextCommitTime = System.currentTimeMillis() + scanTimeLimit;
        }
        Exchange exchange = getExchange(session, index).append(Key.BEFORE);
        try {
            while (exchange.next(true)) {
                visitor.visit(exchange.getKey(), exchange.getValue());
                if ((scanTimeLimit >= 0) &&
                    (System.currentTimeMillis() >= nextCommitTime)) {
                    txnService.commitTransaction(session);
                    if (sleepTime > 0) {
                        try {
                            Thread.sleep(sleepTime);
                        }
                        catch (InterruptedException ex) {
                            throw new QueryCanceledException(session);
                        }
                    }
                    txnService.beginTransaction(session);
                    nextCommitTime = System.currentTimeMillis() + scanTimeLimit;
                }
            }
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, exchange);
        }
        return visitor;
    }

    @Override
    public void discardOnlineChange(Session session, Collection<ChangeSet> changeSets) {
        // None
    }

    @Override
    public void finishOnlineChange(Session session, Collection<ChangeSet> changeSets) {
        // None
    }

    private void lockKeysForIndex(Session session, TableIndex index, RowData rowData) throws PersistitException {
        Table table = index.getTable();
        Exchange ex = getExchange(session, table.getGroup());
        try {
            lockKeysForTable(table.rowDef(), rowData, ex);
        } finally {
            releaseExchange(session, ex);
        }
    }

    private void lockKeysForTable(RowDef rowDef, RowData rowData, Exchange exchange) throws PersistitException
    {
        // Temporary fix for #1118871 and #1078331 
        // disable the  lock used to prevent write skew for some cases of data loading
        if (!writeLockEnabled) return;
        Table table = rowDef.table();
        // Make fieldDefs big enough to accommodate PK field defs and FK field defs
        FieldDef[] fieldDefs = new FieldDef[table.getColumnsIncludingInternal().size()];
        Key lockKey = createKey();
        PersistitKeyAppender lockKeyAppender = PersistitKeyAppender.create(lockKey, table.getName());
        // Primary key
        List<Column> pkColumns = table.getPrimaryKeyIncludingInternal().getColumns();
        for(int i = 0; i < pkColumns.size(); ++i) {
            fieldDefs[i] = pkColumns.get(i).getFieldDef();
        }
        lockKey(rowData, table, fieldDefs, pkColumns.size(), lockKeyAppender, exchange);
        // Grouping foreign key
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            List<JoinColumn> joinColumns = parentJoin.getJoinColumns();
            for(int i = 0; i < joinColumns.size(); ++i) {
                fieldDefs[i] = joinColumns.get(i).getChild().getFieldDef();
            }
            lockKey(rowData, parentJoin.getParent(), fieldDefs, joinColumns.size(), lockKeyAppender, exchange);
        }
    }

    private void lockKey(RowData rowData,
                         Table lockTable,
                         FieldDef[] fieldDefs,
                         int nFields,
                         PersistitKeyAppender lockKeyAppender,
                         Exchange exchange)
        throws PersistitException
    {
        // Write ordinal id to the lock key
        lockKeyAppender.key().append(lockTable.getOrdinal());
        // Write column values to the lock key
        for (int f = 0; f < nFields; f++) {
            lockKeyAppender.append(fieldDefs[f], rowData);
        }
        exchange.lock(lockKeyAppender.key());
        lockKeyAppender.clear();
    }

    @Override
    public PersistitAdapter createAdapter(Session session, Schema schema) {
        return new PersistitAdapter(schema, this, treeService, session, config);
    }

    @Override
    public boolean treeExists(Session session, StorageDescription storageDescription) {
        return treeService.treeExists(((PersistitStorageDescription)storageDescription).getTreeName());
    }

    @Override
    public void truncateTree(Session session, HasStorage object) {
        Exchange iEx = treeService.getExchange(session, (PersistitStorageDescription)object.getStorageDescription());
        try {
            iEx.removeAll();
        } catch (PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, iEx);
        }
    }

    @Override
    public void removeTree(Session session, HasStorage object) {
        PersistitStorageDescription storageDescription = (PersistitStorageDescription)object.getStorageDescription();
        treeService.treeWasRemoved(session, storageDescription);
        ((PersistitStoreSchemaManager)schemaManager).treeWasRemoved(session, object.getSchemaName(), storageDescription.getTreeName());
    }

    @Override
    public long nextSequenceValue(Session session, Sequence sequence) {
        // Note: Ever increasing, always incremented by 1, rollbacks will leave gaps. See bug1167045 for discussion.
        AccumulatorAdapter accum = getAdapter(sequence);
        long rawSequence = accum.seqAllocate();
        return sequence.realValueForRawNumber(rawSequence);
    }
    
    @Override 
    public long curSequenceValue(Session session, Sequence sequence) {
        AccumulatorAdapter accum = getAdapter(sequence);
        try {
            return sequence.realValueForRawNumber(accum.getSnapshot());
        } catch (PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    private AccumulatorAdapter getAdapter(Sequence sequence)  {
        Tree tree = ((PersistitStorageDescription)sequence.getStorageDescription()).getTreeCache();
        return new AccumulatorAdapter(AccumInfo.SEQUENCE, tree);
    }
    
    @Override
    public String getName() {
        return "Persistit " + Persistit.version();
    }

    @Override
    public Collection<String> getStorageDescriptionNames() {
        return treeService.getAllTreeNames();
    }

    @Override
    public Class<? extends Exception> getOnlineDMLFailureException() {
        return TableVersionChangedException.class;
    }

    private static final Callback CLEAR_SESSION_TABLES_CALLBACK = new Callback() {
        @Override
        public void run(Session session, long timestamp) {
            Map<Integer, Integer> map = session.get(SESSION_TABLES_KEY);
            map.clear();
        }
    };

    private static final class CheckTableVersions implements Callback {
        private final SchemaManager schemaManager;

        private CheckTableVersions(SchemaManager schemaManager) {
            this.schemaManager = schemaManager;
        }

        @Override
        public void run(Session session, long timestamp) {
            Map<Integer, Integer> map = session.get(SESSION_TABLES_KEY);
            for(Integer tid : map.keySet()) {
                if(schemaManager.hasTableChanged(session, tid)) {
                    AkibanInformationSchema ais = schemaManager.getAis(session);
                    throw new TableVersionChangedException(ais.getTable(tid), tid);
                }
            }
        }
    }
}
