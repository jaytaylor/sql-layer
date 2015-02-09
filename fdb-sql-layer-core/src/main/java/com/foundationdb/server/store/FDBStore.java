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

import com.foundationdb.KeyValue;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.GroupsContainsLobsVisitor;
import com.foundationdb.ais.util.TableChange.ChangeType;
import com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.directory.PathUtil;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.WriteIndexRow;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.qp.storeadapter.indexrow.FDBIndexRow;
import com.foundationdb.qp.storeadapter.indexrow.SpatialColumnHandler;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.FDBNotCommittedException;
import com.foundationdb.server.error.LobException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.metrics.LongMetric;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.store.TableChanges.Change;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.util.ReadWriteMap;
import com.foundationdb.tuple.Tuple2;
import com.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Value;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import static com.foundationdb.server.store.FDBStoreDataHelper.*;

public class FDBStore extends AbstractStore<FDBStore,FDBStoreData,FDBStorageDescription> implements Service {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final Session.MapKey<Object, SequenceCache> SEQ_UPDATES_KEY = Session.MapKey.mapNamed("SEQ_UPDATE");

    private final FDBHolder holder;
    private final ConfigurationService configService;
    private final FDBSchemaManager schemaManager;
    private final FDBTransactionService txnService;
    private final MetricsService metricsService;
    private final ReadWriteMap<Object, SequenceCache> sequenceCache;
    private LobService lobService;

    private static final String ROWS_FETCHED_METRIC = "SQLLayerRowsFetched";
    private static final String ROWS_STORED_METRIC = "SQLLayerRowsStored";
    private static final String ROWS_CLEARED_METRIC = "SQLLayerRowsCleared";
    private static final String CONFIG_SEQUENCE_CACHE_SIZE = "fdbsql.fdb.sequence_cache_size";

    private LongMetric rowsFetchedMetric, rowsStoredMetric, rowsClearedMetric;
    private DirectorySubspace rootDir;
    private int sequenceCacheSize;


    @Inject
    public FDBStore(FDBHolder holder,
                    ConfigurationService configService,
                    SchemaManager schemaManager,
                    TransactionService txnService,
                    ListenerService listenerService,
                    TypesRegistryService typesRegistryService,
                    ServiceManager serviceManager,
                    MetricsService metricsService) {
        super(txnService, schemaManager, listenerService, typesRegistryService, serviceManager);
        this.holder = holder;
        this.configService = configService;
        if(schemaManager instanceof FDBSchemaManager) {
            this.schemaManager = (FDBSchemaManager)schemaManager;
        } else {
            throw new IllegalStateException("Only usable with FDBSchemaManager, found: " + txnService);
        }
        if(txnService instanceof FDBTransactionService) {
            this.txnService = (FDBTransactionService)txnService;
        } else {
            throw new IllegalStateException("Only usable with FDBTransactionService, found: " + txnService);
        }
        this.metricsService = metricsService;
        this.sequenceCache = ReadWriteMap.wrapFair(new HashMap<Object, SequenceCache>());
    }

    @Override
    public long nextSequenceValue(Session session, Sequence sequence) {
        Map<Object, SequenceCache> sessionMap = session.get(SEQ_UPDATES_KEY);
        SequenceCache cache = null;
        if(sessionMap != null) {
            cache = sessionMap.get(SequenceCache.cacheKey(sequence));
        }
        if(cache == null) {
            cache = sequenceCache.getOrCreateAndPut(SequenceCache.cacheKey(sequence), SEQUENCE_CACHE_VALUE_CREATOR);
            long readTimestamp = txnService.getTransactionStartTimestamp(session);
            if(readTimestamp < cache.getTimestamp()) {
                cache = null;
            }
        }
        long rawValue = (cache != null) ? cache.nextCacheValue() : -1;
        if(rawValue < 0) {
            rawValue = updateSequenceCache(session, sequence);
        }
        return sequence.realValueForRawNumber(rawValue);
    }

    @Override
    public long curSequenceValue(Session session, Sequence sequence) {
        long rawValue = 0;
        SequenceCache cache = sequenceCache.get(sequence.getStorageUniqueKey());
        if(cache == null) {
            cache = session.get(SEQ_UPDATES_KEY, sequence.getStorageUniqueKey());
        }
        if (cache != null) {
            rawValue = cache.getCurrentValue();
        } else {
            // TODO: Allow FDBStorageDescription to intervene?
            TransactionState txn = txnService.getTransaction(session);
            byte[] byteValue = txn.getValue(prefixBytes(sequence));
            if(byteValue != null) {
                Tuple2 tuple = Tuple2.fromBytes(byteValue);
                rawValue = tuple.getLong(0);
            }
        }
        return sequence.realValueForRawNumber(rawValue);
    }

    public void setRollbackPending(Session session) {
        if(txnService.isTransactionActive(session)) {
            txnService.setRollbackPending(session);
        }
    }


    //
    // Service
    //

    @Override
    public void start() {
        rowsFetchedMetric = metricsService.addLongMetric(ROWS_FETCHED_METRIC);
        rowsStoredMetric = metricsService.addLongMetric(ROWS_STORED_METRIC);
        rowsClearedMetric = metricsService.addLongMetric(ROWS_CLEARED_METRIC);

        rootDir = holder.getRootDirectory();

        boolean withConcurrentDML = Boolean.parseBoolean(configService.getProperty(FEATURE_DDL_WITH_DML_PROP));
        this.sequenceCacheSize = Integer.parseInt(configService.getProperty(CONFIG_SEQUENCE_CACHE_SIZE));
        this.constraintHandler = new FDBConstraintHandler(this, configService, typesRegistryService, serviceManager, txnService);
        this.onlineHelper = new OnlineHelper(txnService, schemaManager, this, typesRegistryService, constraintHandler, withConcurrentDML);
        listenerService.registerRowListener(onlineHelper);
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }


    //
    // Store
    //

    @Override
    public FDBStoreData createStoreData(Session session, FDBStorageDescription storageDescription) {
        return new FDBStoreData(session, storageDescription, createKey());
    }

    @Override
    protected void releaseStoreData(Session session, FDBStoreData storeData) {
        // None
    }

    @Override
    FDBStorageDescription getStorageDescription(FDBStoreData storeData) {
        return storeData.storageDescription;
    }

    @Override
    protected Key getKey(Session session, FDBStoreData storeData) {
        return storeData.persistitKey;
    }

    @Override
    protected void store(Session session, FDBStoreData storeData) {
        packKey(storeData);
        storeData.storageDescription.store(this, session, storeData);
        rowsStoredMetric.increment();
    }

    @Override
    protected boolean fetch(Session session, FDBStoreData storeData) {
        packKey(storeData);
        boolean result = storeData.storageDescription.fetch(this, session, storeData);
        rowsFetchedMetric.increment();
        return result;
    }

    @Override
    protected void clear(Session session, FDBStoreData storeData) {
        packKey(storeData);
        storeData.storageDescription.clear(this, session, storeData);
        rowsClearedMetric.increment();
    }

    @Override
    void resetForWrite(FDBStoreData storeData, Index index, WriteIndexRow indexRowBuffer) {
        if(storeData.persistitValue == null) {
            storeData.persistitValue = new Value((Persistit) null);
        }
        indexRowBuffer.resetForWrite(index, storeData.persistitKey, storeData.persistitValue);
    }

    @Override
    protected Iterator<Void> createDescendantIterator(Session session, final FDBStoreData storeData) {
        groupDescendantsIterator(session, storeData);
        return new Iterator<Void>() {
            @Override
            public boolean hasNext() {
                return storeData.iterator.hasNext();
            }

            @Override
            public Void next() {
                storeData.iterator.next();
                unpackKey(storeData);
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected IndexRow readIndexRow(Session session, Index parentPKIndex, FDBStoreData storeData, Row childRow) {
        Key parentPkKey = storeData.persistitKey;
        PersistitKeyAppender keyAppender = PersistitKeyAppender.create(parentPkKey, parentPKIndex.getIndexName());
         for (Column column : childRow.rowType().table().getParentJoin().getChildColumns()) {
             keyAppender.append(childRow.value(column.getPosition()), column);
         }
        // Only called when child row does not contain full HKey.
        // Key contents are the logical parent of the actual index entry (if it exists).
        byte[] packed = packedTuple(parentPKIndex, parentPkKey);
        byte[] end = packedTuple(parentPKIndex, parentPkKey, Key.AFTER);
        TransactionState txn = txnService.getTransaction(session);
        List<KeyValue> pkValue = txn.getRangeAsValueList(packed, end);
        FDBIndexRow indexRow = null;
        if (!pkValue.isEmpty()) {
            assert pkValue.size() == 1 : parentPKIndex;
            KeyValue kv = pkValue.get(0);
            assert kv.getValue().length == 0 : parentPKIndex + ", " + kv;
            indexRow = new FDBIndexRow(this);
            FDBStoreDataHelper.unpackTuple(parentPKIndex, parentPkKey, kv.getKey());
            indexRow.resetForRead(parentPKIndex, parentPkKey, null);
        }
        return indexRow;
        
    }
    
    @Override
    public void writeIndexRow(Session session,
            TableIndex index,
            Row row, 
            Key hKey,
            WriteIndexRow indexRow,
            SpatialColumnHandler spatialColumnHandler,
            long zValue,
            boolean doLock) {
        TransactionState txn = txnService.getTransaction(session);
        Key indexKey = createKey();
        constructIndexRow(session, indexKey, row, index, hKey, indexRow, spatialColumnHandler, zValue, true);
        checkUniqueness(session, txn, index, row, indexKey);

        byte[] packedKey = packedTuple(index, indexKey);
        txn.setBytes(packedKey, EMPTY_BYTE_ARRAY);
        
    }
    
    @Override
    public void deleteIndexRow(Session session, TableIndex index, Row row, Key hKey, WriteIndexRow indexRow,
            SpatialColumnHandler spatialColumnHandler, long zValue, boolean doLock) {
        TransactionState txn = txnService.getTransaction(session);
        Key indexKey = createKey();
        constructIndexRow(session, indexKey, row, index, hKey, indexRow, spatialColumnHandler, zValue, false);
        byte[] packed = packedTuple(index, indexKey);
        txn.clearKey(packed);
    }
    
    @Override
    protected void lock (Session session, FDBStoreData storeData, Row row) {
        // None
    }
    
    @Override
    protected void lock(Session session, Row row) {
        // None
    }
    
    @Override
    protected void trackTableWrite(Session session, Table table) {
        // None
    }


    @Override
    public void truncateTree(Session session, HasStorage object) {
        TransactionState txn = txnService.getTransaction(session);
        txn.clearRange(Range.startsWith(prefixBytes(object)));
    }

    @Override
    public void deleteIndexes(Session session, Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            removeIfExists(session, rootDir, FDBNameGenerator.dataPath(index));
        }
    }

    @Override
    public void dropGroup(final Session session, Group group) {
        deleteLobs(session, group);
        group.getRoot().visit(new AbstractVisitor() {
            @Override
            public void visit(Table table) {
                removeTrees(session, table);
            }
        });
    }

    @Override
    public void removeTrees(Session session, Table table) {
        // Table and indexes (and group and group indexes if root table)
        removeIfExists(session, rootDir, FDBNameGenerator.dataPath(table.getName()));
        // Sequence
        if(table.getIdentityColumn() != null) {
            deleteSequences(session, Collections.singleton(table.getIdentityColumn().getIdentityGenerator()));
        }
    }

    @Override
    public void removeTrees(Session session, com.foundationdb.ais.model.Schema schema) {
        removeIfExists(session, rootDir, FDBNameGenerator.dataPathSchemaTable(schema.getName()));
        removeIfExists(session, rootDir, FDBNameGenerator.dataPathSchemaSequence(schema.getName()));
    }

    @Override
    public void removeTree(Session session, HasStorage object) {
        deleteLobs(session, object);
        truncateTree(session, object);
    }

    private void deleteLobs(Session session, HasStorage object) {
        if (object instanceof com.foundationdb.ais.model.Group) {
            Group group = (Group)object;
            GroupsContainsLobsVisitor visitor = new GroupsContainsLobsVisitor();
            group.visit(visitor);
            if (visitor.containsLob()) {
                deleteLobsChecked(session, group);
            }
        }
    }
    
    private void deleteLobsChecked(Session session, Group group) {
        FDBStoreData storeData = createStoreData(session, group);
        groupIterator(session, storeData);
        while (storeData.next()) {
            Row row = expandGroupData(session, storeData, SchemaCache.globalSchema(group.getAIS()));
            deleteLobs(row);
        }
    }
    
    @Override
    public void dropAllLobs(Session session) {
        getLobService().clearAllLobs();
    }
    
    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            truncateTree(session, index);
        }
    }

    @Override
    public void deleteSequences(Session session, Collection<? extends Sequence> sequences) {
        for (Sequence sequence : sequences) {
            session.remove(SEQ_UPDATES_KEY, SequenceCache.cacheKey(sequence));
            sequenceCache.remove(sequence.getStorageUniqueKey());
            removeIfExists(session, rootDir, FDBNameGenerator.dataPath(sequence));
        }
    }

    @Override
    public FDBAdapter createAdapter(Session session) {
        return new FDBAdapter(this, session, txnService, configService);
    }

    @Override
    public boolean treeExists(Session session, StorageDescription storageDescription) {
        TransactionState txn = txnService.getTransaction(session);
        return txn.getRangeExists(Range.startsWith(prefixBytes((FDBStorageDescription) storageDescription)), 1);
    }

    @Override
    public void discardOnlineChange(Session session, Collection<ChangeSet> changeSets) {
        for(ChangeSet cs : changeSets) {
            TableName newName = new TableName(cs.getNewSchema(), cs.getNewName());
            removeIfExists(session, rootDir, FDBNameGenerator.onlinePath(newName));
            for(Change c : cs.getIdentityChangeList()) {
                switch(ChangeType.valueOf(c.getChangeType())) {
                    case ADD:
                        removeIfExists(session, rootDir, FDBNameGenerator.onlinePathSequence(newName.getSchemaName(), c.getNewName()));
                    break;
                    case DROP:
                        // None
                    break;
                    default:
                        throw new IllegalStateException(c.getChangeType());
                }

            }
        }
    }

    @Override
    public void finishOnlineChange(Session session, Collection<ChangeSet> changeSets) {
        TransactionState txnState = txnService.getTransaction(session);
        Transaction txn = txnState.getTransaction();
        for(ChangeSet cs : changeSets) {
            TableName oldName = new TableName(cs.getOldSchema(), cs.getOldName());
            TableName newName = new TableName(cs.getNewSchema(), cs.getNewName());

            for(Change c : cs.getIdentityChangeList()) {
                List<String> seqOldDataPath = FDBNameGenerator.dataPathSequence(oldName.getSchemaName(), c.getOldName());
                List<String> seqNewDataPath = FDBNameGenerator.dataPathSequence(newName.getSchemaName(), c.getNewName());
                List<String> seqOnlinePath = FDBNameGenerator.onlinePathSequence(newName.getSchemaName(), c.getNewName());
                switch(ChangeType.valueOf(c.getChangeType())) {
                    case ADD:
                        try {
                            rootDir.removeIfExists(txn, seqOldDataPath).get();
                            // Due to schema currently being create on demand
                            rootDir.createOrOpen(txn, PathUtil.popBack(seqNewDataPath)).get();
                            rootDir.move(txn, seqOnlinePath, seqNewDataPath).get();
                        } catch (RuntimeException e) {
                            throw FDBAdapter.wrapFDBException(session, e);
                        }
                        break;
                    case DROP:
                        try {
                            rootDir.removeIfExists(txn, seqOldDataPath).get();
                        } catch (RuntimeException e) {
                            throw FDBAdapter.wrapFDBException(session, e);
                        }
                        break;
                    default:
                        throw new IllegalStateException(cs.getChangeLevel());
                }
            }

            List<String> dataPath = FDBNameGenerator.dataPath(oldName);
            List<String> onlinePath = FDBNameGenerator.onlinePath(newName);
            // - move renamed directories
            if(!oldName.equals(newName)) {
                schemaManager.renamingTable(session, oldName, newName);
                dataPath = FDBNameGenerator.dataPath(newName);
            }
            if (!directoryExists(txnState, rootDir, onlinePath)) {
                continue;
            }
            switch(ChangeLevel.valueOf(cs.getChangeLevel())) {
                case NONE:
                    // None
                break;
                case METADATA:
                case METADATA_CONSTRAINT:
                case INDEX:
                case INDEX_CONSTRAINT:
                    // - Move everything from dataOnline/foo/ to data/foo/
                    // - remove dataOnline/foo/
                    try {
                        for(String subPath : rootDir.list(txn, onlinePath).get()) {
                            List<String> subDataPath = PathUtil.extend(dataPath, subPath);
                            List<String> subOnlinePath = PathUtil.extend(onlinePath, subPath);
                            rootDir.removeIfExists(txn, subDataPath).get();
                            rootDir.move(txn, subOnlinePath, subDataPath).get();
                        }
                        rootDir.remove(txn, onlinePath).get();
                    } catch (RuntimeException e) {
                        throw FDBAdapter.wrapFDBException(session, e);
                    }
                break;
                case TABLE:
                case GROUP:
                    // - move unaffected from data/foo/ to dataOnline/foo/
                    // - remove data/foo
                    // - move dataOnline/foo to data/foo/
                    try {
                        if (rootDir.exists(txn, dataPath).get()) {
                            executeLobOnlineDelete(session, oldName.getSchemaName(), oldName.getTableName());
                            for(String subPath : rootDir.list(txn, dataPath).get()) {
                                List<String> subDataPath = PathUtil.extend(dataPath, subPath);
                                List<String> subOnlinePath = PathUtil.extend(onlinePath, subPath);
                                if(!rootDir.exists(txn, subOnlinePath).get()) {
                                    rootDir.move(txn, subDataPath, subOnlinePath).get();
                                }
                            }
                            rootDir.remove(txn, dataPath).get();
                        }
                        rootDir.move(txn, onlinePath, dataPath).get();
                    } catch (RuntimeException e) {
                        throw FDBAdapter.wrapFDBException(session, e);
                    }
                break;
                default:
                    throw new IllegalStateException(cs.getChangeLevel());
            }
        }
    }

    @Override
    public void traverse(Session session, Group group, TreeRecordVisitor visitor) {
        visitor.initialize(session, this);
        FDBStoreData storeData = createStoreData(session, group);
        groupIterator(session, storeData);
        while (storeData.next()) {
            Row row = expandGroupData(session, storeData, SchemaCache.globalSchema(group.getAIS()));
            visitor.visit(storeData.persistitKey, row);
        }
    }

    public Row expandGroupData(Session session, FDBStoreData storeData, Schema schema) {
        unpackKey(storeData);
        return expandRow(session, storeData, schema);
    }
    
    @Override
    public <V extends IndexVisitor<Key, Value>> V traverse(Session session, Index index, V visitor, long scanTimeLimit, long sleepTime) {
        FDBStoreData storeData = createStoreData(session, index);
        storeData.persistitValue = new Value((Persistit)null);
        TransactionState txn = txnService.getTransaction(session);
        FDBScanTransactionOptions transactionOptions;
        if (scanTimeLimit > 0) {
            transactionOptions = new FDBScanTransactionOptions(true, -1,
                                                               scanTimeLimit, sleepTime);
        }
        else {
            transactionOptions = FDBScanTransactionOptions.SNAPSHOT;
        }
        indexIterator(session, storeData, false, false, false, transactionOptions);
        while(storeData.next()) {
            // Key
            unpackKey(storeData);

            // Value
            unpackValue(storeData);

            // Visit
            visitor.visit(storeData.persistitKey, storeData.persistitValue);
        }
        return visitor;
    }

    @Override
    public String getName() {
        return "FoundationDB APIv" + holder.getAPIVersion();
    }

    @Override
    public Collection<String> getStorageDescriptionNames() {
        final List<List<String>> dataDirs = Arrays.asList(
            Arrays.asList(FDBNameGenerator.DATA_PATH_NAME, FDBNameGenerator.TABLE_PATH_NAME),
            Arrays.asList(FDBNameGenerator.DATA_PATH_NAME, FDBNameGenerator.SEQUENCE_PATH_NAME),
            Arrays.asList(FDBNameGenerator.ONLINE_PATH_NAME, FDBNameGenerator.TABLE_PATH_NAME),
            Arrays.asList(FDBNameGenerator.ONLINE_PATH_NAME, FDBNameGenerator.SEQUENCE_PATH_NAME)
        );
        return txnService.runTransaction(new Function<Transaction, Collection<String>>() {
            @Override
            public Collection<String> apply(Transaction txn) {
                Set<String> pathSet = new TreeSet<>();
                for(List<String> dataPath : dataDirs) {
                    if(rootDir.exists(txn, dataPath).get()) {
                        for(String schemaName : rootDir.list(txn, dataPath).get()) {
                            List<String> schemaPath = PathUtil.extend(dataPath, schemaName);
                            for(String o : rootDir.list(txn, schemaPath).get()) {
                                pathSet.add(PathUtil.extend(schemaPath, o).toString());
                            }
                        }
                    }
                }
                return pathSet;
            }
        });
    }

    @Override
    public Class<? extends Exception> getOnlineDMLFailureException() {
        return FDBNotCommittedException.class;
    }

    
    @Override
    protected Row storeLobs(Row row) {
        RowType rowType = row.rowType();
        OverlayingRow resRow = new OverlayingRow(row);
        Boolean changedRow = false;

        for ( int i = 0; i < rowType.nFields(); i++ ) {
            if (rowType.typeAt(i).equalsExcludingNullable(AkBlob.INSTANCE.instance(true))) {
                int tableId = rowType.table().getTableId();
                BlobRef blobRefInit = (BlobRef)row.value(i).getObject();
                String allowedLobFormat = configService.getProperty(AkBlob.BLOB_ALLOWED_FORMAT);
                
                if (blobRefInit == null) {
                    continue;
                }
                BlobRef.LeadingBitState state;
                BlobRef.LobType type;
                byte[] value = blobRefInit.getValue();
                
                // The contract: 
                //    if blobReturnMode == SIMPLE 
                //        - all BlobRef value contains no leading bit 
                //        - data is always the data to be stored, never GUID of the blob
                //        - all output to users is data only
                //    if blobReturnMode == ADVANCED
                //        - all input contains a leading bit
                //        - if leading bit == LONG_BLOB
                //              data is GUID of the blob
                //          else data is content of the blob (which can be long or short blob)
                // 
                // all output of this function contains a leading bit of a type depending on the storage
                // 
                //
                
                if (isBlobReturnModeSimple()) {
                    state = BlobRef.LeadingBitState.NO;
                } else {
                    state = BlobRef.LeadingBitState.YES;
                }
                // ensure correct state of the leading bit
                BlobRef blobRefTmp = new BlobRef(value, state, blobRefInit.getLobType(), blobRefInit.getRequestedLobType());
                
                if (blobRefTmp.isLongLob() && !isBlobReturnModeSimple()) { // only set in ADVANCED mode
                    if (allowedLobFormat.equalsIgnoreCase(AkBlob.SHORT_BLOB)) {
                        throw new LobException("Long lob storage format not allowed");
                    }
                    type = BlobRef.LobType.LONG_LOB;
                } else if (blobRefTmp.isShortLob() && !isBlobReturnModeSimple()) { // only set in ADVANCED mode
                    if (allowedLobFormat.equalsIgnoreCase(AkBlob.LONG_BLOB)) {
                        throw new LobException("Short lob storage format not allowed");
                    }
                    if (allowedLobFormat.equalsIgnoreCase(AkBlob.SHORT_BLOB) && 
                            (blobRefTmp.getBytes().length >= AkBlob.LOB_SWITCH_SIZE)) {
                        throw new LobException("Lob too large to store as SHORT_LOB");
                    }
                    if (blobRefTmp.getBytes().length >= AkBlob.LOB_SWITCH_SIZE) {
                        UUID id = UUID.randomUUID();
                        getLobService().createNewLob(id.toString());
                        getLobService().writeBlob(id.toString(), 0, blobRefTmp.getBytes());
                        value = updateValue(id);
                        type = BlobRef.LobType.LONG_LOB;
                    } else {
                        type = BlobRef.LobType.SHORT_LOB;
                    }
                } else { // only in SIMPLE mode, value needs updating, adding leading bit and correct value content (guid)
                    // first verify if specific requested format is allowed --> should be done in functions.
                    if (blobRefTmp.getRequestedLobType() == BlobRef.LobType.SHORT_LOB) {
                        if (allowedLobFormat.equalsIgnoreCase(AkBlob.LONG_BLOB)) {
                            throw new LobException("Short lob storage format not allowed");
                        }
                    }
                    else if (blobRefTmp.getRequestedLobType() == BlobRef.LobType.LONG_LOB ) {
                        if (allowedLobFormat.equalsIgnoreCase(AkBlob.SHORT_BLOB)) {
                            throw new LobException("Long lob storage format not allowed");
                        }
                    }
                    
                    if (blobRefTmp.getRequestedLobType() == BlobRef.LobType.LONG_LOB || 
                            allowedLobFormat.equalsIgnoreCase(AkBlob.LONG_BLOB) ||
                            blobRefTmp.getBytes().length >= AkBlob.LOB_SWITCH_SIZE) {
                        if (allowedLobFormat.equalsIgnoreCase(AkBlob.SHORT_BLOB)) {
                            throw new LobException("Long lob storage format not allowed");
                        }
                        if (blobRefInit.isReturnedBlobInSimpleMode()){
                            type = BlobRef.LobType.LONG_LOB;
                            value = updateValue(blobRefInit.getId());
                        } else {
                            UUID id = UUID.randomUUID();
                            getLobService().createNewLob(id.toString());
                            getLobService().writeBlob(id.toString(), 0, blobRefTmp.getBytes());
                            type = BlobRef.LobType.LONG_LOB;
                            value = updateValue(id);
                        }
                    }
                    else {
                        type = BlobRef.LobType.SHORT_LOB;
                        value = updateValue(blobRefTmp.getBytes());
                    }
                }
                BlobRef blobRef = new BlobRef(value, BlobRef.LeadingBitState.YES, type, BlobRef.LobType.UNKNOWN);
                if (blobRef.isLongLob()) {
                    getLobService().linkTableBlob(blobRef.getId().toString(), tableId);
                }
                resRow.overlay(i, blobRef);
                changedRow = true;
            }
        }
        return changedRow ? resRow : row;
    }
    
    
    
    private byte[] updateValue(UUID id) {
        byte[] res = new byte[17];
        System.arraycopy(AkGUID.uuidToBytes(id), 0, res, 1, 16);
        res[0] = BlobRef.LONG_LOB;
        return res;
    }

    private byte[] updateValue(byte[] data) {
        byte[] res = new byte[data.length+1];
        System.arraycopy(data, 0, res, 1, data.length);
        res[0] = BlobRef.SHORT_LOB;
        return res;
    }
    
    public byte[] getBlobData(BlobRef blob) {
        if (blob.isShortLob()) {
            return blob.getBytes();
        } else if (blob.isLongLob()) {
            return getLobService().readBlob(blob.getId().toString());
        } else {
            throw new LobException("Type of lob not available");
        }
    }
    
    public boolean isBlobReturnModeSimple() {
        return configService.getProperty(AkBlob.BLOB_RETURN_MODE).equalsIgnoreCase(AkBlob.SIMPLE) ? true : false;
    }
    
    @Override
    protected void deleteLobs(Row row) {
        RowType rowType = row.rowType();
        for( int i = 0; i < rowType.nFields(); i++ ) {
            if (rowType.typeAt(i).equalsExcludingNullable(AkBlob.INSTANCE.instance(true))) {
                BlobRef blobRef = (BlobRef)row.value(i).getObject();
                if (blobRef == null) {
                    continue;
                }
                if (blobRef.isLongLob()) {
                    getLobService().deleteLob(blobRef.getId().toString());
                }
            }
        }        
    }
    
    private LobService getLobService() {
        if (lobService == null)
            lobService = serviceManager.getServiceByClass(LobService.class);
        return lobService;
    }
    
    @Override
    protected void registerLobForOnlineDelete(Session session, String schemaName, String tableRootName, UUID lobId) {
        List<String> path = FDBNameGenerator.onlineLobPath(schemaName, tableRootName);
        TransactionState tr = txnService.getTransaction(session);
        DirectorySubspace dir = rootDir.createOrOpen(tr.getTransaction(), path).get();
        byte[] key = dir.pack(AkGUID.uuidToBytes(lobId));
        tr.setBytes(key, EMPTY_BYTE_ARRAY);
    }
    
    @Override
    protected void executeLobOnlineDelete(Session session, String schemaName, String tableName) {
        List<String> onlineLobPath = FDBNameGenerator.onlineLobPath(schemaName, tableName);
        TransactionState tr = txnService.getTransaction(session);
        if (rootDir.exists(tr.getTransaction(), onlineLobPath).get()) {
            DirectorySubspace dir = rootDir.createOrOpen(tr.getTransaction(), onlineLobPath).get();
            AsyncIterator<KeyValue> it = tr.getRangeIterator(dir.range(), Integer.MAX_VALUE).iterator();
            while(it.hasNext()) {
                KeyValue kv = it.next();
                Tuple t = dir.unpack(kv.getKey());
                getLobService().deleteLob(AkGUID.bytesToUUID(t.getBytes(0), 0).toString());
            }
            rootDir.removeIfExists(tr.getTransaction(), onlineLobPath).get();
        }
    }
    
    //
    // KeyCreator
    //

    @Override
    public Key createKey() {
        return new Key(null, 2047);
    }

    //
    // Storage iterators
    //

    public TransactionState getTransaction(Session session, FDBStoreData storeData) {
        return txnService.getTransaction(session);
    }

    public enum GroupIteratorBoundary { 
        START, END, KEY, NEXT_KEY, 
        FIRST_DESCENDANT, LAST_DESCENDANT
    }

    /** Iterate over the whole group. */
    public void groupIterator(Session session, FDBStoreData storeData) {
        groupIterator(session, storeData, 
                      GroupIteratorBoundary.START, GroupIteratorBoundary.END, 
                      Transaction.ROW_LIMIT_UNLIMITED, FDBScanTransactionOptions.NORMAL);
    }

    /** Iterate over just <code>storeData.persistitKey</code>, if present. */
    public void groupKeyIterator(Session session, FDBStoreData storeData, FDBScanTransactionOptions transactionOptions) {
        // NOTE: Caller checks whether key returned matches.
        groupIterator(session, storeData, 
                      GroupIteratorBoundary.KEY, GroupIteratorBoundary.NEXT_KEY,
                      1, transactionOptions);
    }

    /** Iterate over <code>storeData.persistitKey</code>'s descendants. */
    public void groupDescendantsIterator(Session session, FDBStoreData storeData) {
        groupIterator(session, storeData, 
                      GroupIteratorBoundary.FIRST_DESCENDANT, GroupIteratorBoundary.LAST_DESCENDANT,
                      Transaction.ROW_LIMIT_UNLIMITED, FDBScanTransactionOptions.NORMAL);
    }

    /** Iterate over <code>storeData.persistitKey</code>'s descendants. */
    public void groupKeyAndDescendantsIterator(Session session, FDBStoreData storeData, FDBScanTransactionOptions transactionOptions) {
        groupIterator(session, storeData, 
                      GroupIteratorBoundary.KEY, GroupIteratorBoundary.LAST_DESCENDANT,
                      Transaction.ROW_LIMIT_UNLIMITED, transactionOptions);
    }

    public void groupIterator(Session session, FDBStoreData storeData, FDBScanTransactionOptions transactionOptions) {
        groupIterator(session, storeData, 
                      GroupIteratorBoundary.START, GroupIteratorBoundary.END, 
                      Transaction.ROW_LIMIT_UNLIMITED, transactionOptions);
    }

    public void groupIterator(Session session, FDBStoreData storeData,
                              GroupIteratorBoundary left, GroupIteratorBoundary right,
                              int limit, FDBScanTransactionOptions transactionOptions) {
        storeData.storageDescription.groupIterator(this, session, storeData,
                                                   left, right, limit,
                                                   transactionOptions);
    }

    /** Iterate over the whole index. */
    public void indexIterator(Session session, FDBStoreData storeData,
                              FDBScanTransactionOptions transactionOptions) {
        indexIterator(session, storeData, false, false, false, transactionOptions);
    }

    public void indexIterator(Session session, FDBStoreData storeData,
                              boolean key, boolean inclusive, boolean reverse,
                              FDBScanTransactionOptions transactionOptions) {
        storeData.storageDescription.indexIterator(this, session, storeData,
                                                   key, inclusive, reverse,
                                                   transactionOptions);
    }

    //
    // Internal
    //

    private void constructIndexRow(Session session,
                                    Key indexKey,
                                    Row row,
                                    Index index,
                                    Key hKey,
                                    WriteIndexRow indexRow,
                                    SpatialColumnHandler spatialColumnHandler,
                                    long zValue,
                                    boolean forInsert) {
        indexKey.clear();
        indexRow.resetForWrite(index, indexKey);
        indexRow.initialize(row, hKey, spatialColumnHandler, zValue);
        indexRow.close(session, forInsert);
    }
    
    private void checkUniqueness(Session session, TransactionState txn, Index index, Row row, Key key) {
        if(index.isUnique() && !hasNullIndexSegments(row, index)) {
            int realSize = key.getEncodedSize();
            key.setDepth(index.getKeyColumns().size());
            try {
                checkKeyDoesNotExistInIndex(session, txn, row, index, key);
            } finally {
                key.setEncodedSize(realSize);
            }
        }
        
    }

    private void checkKeyDoesNotExistInIndex(Session session, TransactionState txn, Row row, Index index, Key key) {
        assert index.isUnique() : index;
        FDBPendingIndexChecks.PendingCheck<?> check =
            FDBPendingIndexChecks.keyDoesNotExistInIndexCheck(session, txn, index, key);
        if (txn.getForceImmediateForeignKeyCheck() ||
            txn.getIndexChecks(false) == null) {
            check.blockUntilReady(txn);
            if (!check.check(session, txn, index)) {
                // Using RowData, can give better error than check.throwException().
                String msg = formatIndexRowString(session, row, index);
                throw new DuplicateKeyException(index.getIndexName(), msg);
            }
        }
        else {
            txn.getIndexChecks(false).add(session, txn, index, check);
        }
        
    }

    private void removeIfExists(Session session, DirectorySubspace dir, List<String> dirs) {
        try {
            Transaction txn = txnService.getTransaction(session).getTransaction();
            dir.removeIfExists(txn, dirs).get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(session, e);
        }
    }
    
    private boolean directoryExists (TransactionState txn, DirectorySubspace dir, List<String> dirs) {
        try {
            return dir.exists(txn.getTransaction(), dirs).get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(txn.session, e);
        }

    }

    private long updateSequenceCache(Session session, Sequence s) {
        Transaction tr = txnService.getTransaction(session).getTransaction();
        byte[] prefixBytes = prefixBytes(s);
        byte[] byteValue = tr.get(prefixBytes).get();
        final long rawValue;
        if(byteValue != null) {
            Tuple2 tuple = Tuple2.fromBytes(byteValue);
            rawValue = tuple.getLong(0);
        } else {
            rawValue = 1;
        }
        tr.set(prefixBytes, Tuple2.from(rawValue + sequenceCacheSize).pack());

        Map<Object, SequenceCache> sessionMap = session.get(SEQ_UPDATES_KEY);
        if(sessionMap == null) {
            txnService.addCallback(session, TransactionService.CallbackType.COMMIT, SEQUENCE_UPDATES_PUT_CALLBACK);
            txnService.addCallback(session, TransactionService.CallbackType.END, SEQUENCE_UPDATES_CLEAR_CALLBACK);
        }
        SequenceCache newCache = SequenceCache.newLocal(rawValue, sequenceCacheSize);
        session.put(SEQ_UPDATES_KEY, SequenceCache.cacheKey(s), newCache);
        return rawValue;
    }


    private static final ReadWriteMap.ValueCreator<Object, SequenceCache> SEQUENCE_CACHE_VALUE_CREATOR =
        new ReadWriteMap.ValueCreator<Object, SequenceCache>() {
            public SequenceCache createValueForKey (Object key) {
                return SequenceCache.newEmpty();
            }
        };

    private static final TransactionService.Callback SEQUENCE_UPDATES_CLEAR_CALLBACK = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            session.remove(SEQ_UPDATES_KEY);
        }
    };

    private final TransactionService.Callback SEQUENCE_UPDATES_PUT_CALLBACK = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            Map<Object, SequenceCache> map = session.get(SEQ_UPDATES_KEY);
            for(Entry<Object, SequenceCache> entry : map.entrySet()) {
                SequenceCache global = SequenceCache.newGlobal(timestamp, entry.getValue());
                sequenceCache.put(entry.getKey(), global);
            }
        }
    };
}
