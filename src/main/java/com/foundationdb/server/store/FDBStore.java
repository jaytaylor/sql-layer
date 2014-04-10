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
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.directory.PathUtil;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.FDBAdapterException;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.metrics.LongMetric;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.util.ReadWriteMap;
import com.foundationdb.FDBException;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

import static com.foundationdb.server.store.FDBStoreDataHelper.*;

/**
 * Directory usage:
 * <pre>
 * root_dir/
 *   indexCount/
 *   indexNull/
 * </pre>
 *
 * <p>
 *     The above directories are used in servicing per-group index row counts
 *     and NULL-able UNIQUE index separator values, respectively. The former
 *     is stored as a little endian long, for {@link Transaction#mutate} usage,
 *     and the latter is stored as {@link Tuple} encoded long. The keys for
 *     each entry are created by pre-pending the directory prefix onto the
 *     prefix of the given index.
 * </p>
 */
public class FDBStore extends AbstractStore<FDBStore,FDBStoreData,FDBStorageDescription> implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(FDBStore.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final FDBHolder holder;
    private final ConfigurationService configService;
    private final FDBSchemaManager schemaManager;
    private final FDBTransactionService txnService;
    private final MetricsService metricsService;

    private static final String ROWS_FETCHED_METRIC = "SQLLayerRowsFetched";
    private static final String ROWS_STORED_METRIC = "SQLLayerRowsStored";
    private static final String ROWS_CLEARED_METRIC = "SQLLayerRowsCleared";
    private LongMetric rowsFetchedMetric, rowsStoredMetric, rowsClearedMetric;

    private DirectorySubspace rootDir;


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
    }

    @Override
    public long nextSequenceValue(Session session, Sequence sequence) {
        long rawValue = 0;
        SequenceCache cache = sequenceCache.getOrCreateAndPut(sequence.getStorageUniqueKey(), SEQUENCE_CACHE_VALUE_CREATOR);
        cache.cacheLock();
        try {
            rawValue = cache.nextCacheValue();
            if (rawValue < 0) {
                rawValue = updateCacheFromServer (session, cache, sequence);
            }
        } finally {
            cache.cacheUnlock();
        }
        return sequence.realValueForRawNumber(rawValue);
    }

    // insert or update the sequence value from the server.
    // Works only under the cache lock from nextSequenceValue. 
    private long updateCacheFromServer (Session session, final SequenceCache cache, final Sequence sequence) {
        final long [] rawValue = new long[1];
        
        try {
            txnService.runTransaction(new Function<Transaction,Void> (){
                @Override 
                public Void apply (Transaction tr) {
                    byte[] prefixBytes = prefixBytes(sequence);
                    byte[] byteValue = tr.get(prefixBytes).get();
                    if(byteValue != null) {
                        Tuple tuple = Tuple.fromBytes(byteValue);
                        rawValue[0] = tuple.getLong(0);
                    } else {
                        rawValue[0] = 1;
                    }
                    tr.set(prefixBytes, Tuple.from(rawValue[0] + sequence.getCacheSize()).pack());
                    return null;
                }
            });
        } catch (Exception e) {
            throw FDBAdapter.wrapFDBException(session, e);
        }
        cache.updateCache(rawValue[0], sequence.getCacheSize());
        return rawValue[0];
    }
    
    @Override
    public long curSequenceValue(Session session, Sequence sequence) {
        long rawValue = 0;
        
        SequenceCache cache = sequenceCache.get(sequence.getStorageUniqueKey());
        if (cache != null) {
            rawValue = cache.currentValue();
        } else {
            // TODO: Allow FDBStorageDescription to intervene?
            TransactionState txn = txnService.getTransaction(session);
            byte[] byteValue = txn.getTransaction().get(prefixBytes(sequence)).get();
            if(byteValue != null) {
                Tuple tuple = Tuple.fromBytes(byteValue);
                rawValue = tuple.getLong(0);
            }
        }
        return sequence.realValueForRawNumber(rawValue);
    }

    private final ReadWriteMap<Object, SequenceCache> sequenceCache = ReadWriteMap.wrapFair(new HashMap<Object, SequenceCache>());

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
        this.constraintHandler = new FDBConstraintHandler(this, configService, typesRegistryService, serviceManager, txnService);
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
        return new FDBStoreData(storageDescription, createKey());
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
    void resetForWrite(FDBStoreData storeData, Index index, PersistitIndexRowBuffer indexRowBuffer) {
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
    protected PersistitIndexRowBuffer readIndexRow(Session session,
                                                   Index parentPKIndex,
                                                   FDBStoreData storeData,
                                                   RowDef childRowDef,
                                                   RowData childRowData) {
        Key parentPkKey = storeData.persistitKey;
        PersistitKeyAppender keyAppender = PersistitKeyAppender.create(parentPkKey, parentPKIndex.getIndexName());
        int[] fields = childRowDef.getParentJoinFields();
        for(int field : fields) {
            FieldDef fieldDef = childRowDef.getFieldDef(field);
            keyAppender.append(fieldDef, childRowData);
        }

        // Only called when child row does not contain full HKey.
        // Key contents are the logical parent of the actual index entry (if it exists).
        byte[] packed = packedTuple(parentPKIndex, parentPkKey);
        byte[] end = packedTuple(parentPKIndex, parentPkKey, Key.AFTER);
        TransactionState txn = txnService.getTransaction(session);
        List<KeyValue> pkValue = txn.getTransaction().getRange(packed, end).asList().get();
        PersistitIndexRowBuffer indexRow = null;
        if (!pkValue.isEmpty()) {
            assert pkValue.size() == 1 : parentPKIndex;
            KeyValue kv = pkValue.get(0);
            assert kv.getValue().length == 0 : parentPKIndex + ", " + kv;
            indexRow = new PersistitIndexRowBuffer(this);
            FDBStoreDataHelper.unpackTuple(parentPKIndex, parentPkKey, kv.getKey());
            indexRow.resetForRead(parentPKIndex, parentPkKey, null);
        }
        return indexRow;
    }

    @Override
    public void writeIndexRow(Session session,
                              TableIndex index,
                              RowData rowData,
                              Key hKey,
                              PersistitIndexRowBuffer indexRow,
                              boolean doLock) {
        TransactionState txn = txnService.getTransaction(session);
        Key indexKey = createKey();
        constructIndexRow(session, indexKey, rowData, index, hKey, indexRow, true);
        checkUniqueness(session, txn, index, rowData, indexKey);

        byte[] packedKey = packedTuple(index, indexKey);
        assert indexRow.getValue() == null : index;
        txn.setBytes(packedKey, EMPTY_BYTE_ARRAY);
    }

    @Override
    public void deleteIndexRow(Session session,
                               TableIndex index,
                               RowData rowData,
                               Key hKey,
                               PersistitIndexRowBuffer indexRow,
                               boolean doLock) {
        TransactionState txn = txnService.getTransaction(session);
        Key indexKey = createKey();
        constructIndexRow(session, indexKey, rowData, index, hKey, indexRow, false);
        byte[] packed = packedTuple(index, indexKey);
        txn.getTransaction().clear(packed);
    }

    @Override
    protected void lock(Session session, FDBStoreData storeData, RowDef rowDef, RowData rowData) {
        // None
    }

    @Override
    protected void lock(Session session, Row row) {
        // None
    }

    @Override
    public void truncateTree(Session session, HasStorage object) {
        TransactionState txn = txnService.getTransaction(session);
        txn.getTransaction().clear(Range.startsWith(prefixBytes(object)));
    }

    @Override
    public void deleteIndexes(Session session, Collection<? extends Index> indexes) {
        Transaction txn = txnService.getTransaction(session).getTransaction();
        for(Index index : indexes) {
            rootDir.removeIfExists(txn, FDBNameGenerator.dataPath(index)).get();
        }
    }

    @Override
    public void removeTrees(Session session, Table table) {
        Transaction txn = txnService.getTransaction(session).getTransaction();
        // Table and indexes (and group and group indexes if root table)
        rootDir.removeIfExists(txn, FDBNameGenerator.dataPath(table.getName())).get();
        // Sequence
        if(table.getIdentityColumn() != null) {
            deleteSequences(session, Collections.singleton(table.getIdentityColumn().getIdentityGenerator()));
        }
    }

    @Override
    public void removeTree(Session session, HasStorage object) {
        truncateTree(session, object);
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            truncateTree(session, index);
        }
    }

    @Override
    public void deleteSequences(Session session, Collection<? extends Sequence> sequences) {
        TransactionState txn = txnService.getTransaction(session);
        for (Sequence sequence : sequences) {
            sequenceCache.remove(sequence.getStorageUniqueKey());
            txn.getTransaction().clear(prefixBytes(sequence));
            rootDir.removeIfExists(
                txn.getTransaction(),
                FDBNameGenerator.dataPath(sequence)
            ).get();
        }
    }

    @Override
    public FDBAdapter createAdapter(Session session, Schema schema) {
        return new FDBAdapter(this, schema, session, txnService, configService);
    }

    @Override
    public boolean treeExists(Session session, StorageDescription storageDescription) {
        TransactionState txn = txnService.getTransaction(session);
        return txn.getTransaction().getRange(Range.startsWith(prefixBytes((FDBStorageDescription)storageDescription)), 1).iterator().hasNext();
    }

    @Override
    public boolean isRetryableException(Throwable t) {
        if(t instanceof FDBAdapterException) {
            t = t.getCause();
        }
        if(t instanceof FDBException) {
            int code = ((FDBException)t).getCode();
            // not_committed || commit_unknown_result
            return (code == 1020) || (code == 1021);
        }
        return false;
    }

    @Override
    public void finishOnlineChange(Session session, Collection<ChangeSet> changeSets) {
        Transaction txn = txnService.getTransaction(session).getTransaction();
        for(ChangeSet cs : changeSets) {
            TableName oldName = new TableName(cs.getOldSchema(), cs.getOldName());
            TableName newName = new TableName(cs.getNewSchema(), cs.getNewName());

            List<String> dataPath = FDBNameGenerator.dataPath(oldName);
            List<String> onlinePath = FDBNameGenerator.onlinePath(newName);
            // - move renamed directories
            if(!oldName.equals(newName)) {
                schemaManager.renamingTable(session, oldName, newName);
                dataPath = FDBNameGenerator.dataPath(newName);
            }
            if(!rootDir.exists(txn, onlinePath).get()) {
                continue;
            }
            switch(ChangeLevel.valueOf(cs.getChangeLevel())) {
                case NONE:
                    // None
                break;
                case METADATA:
                case METADATA_NOT_NULL:
                case INDEX:
                    // - Move everything from dataOnline/foo/ to data/foo/
                    // - remove dataOnline/foo/
                    for(String subPath : rootDir.list(txn, onlinePath).get()) {
                        List<String> subDataPath = PathUtil.extend(dataPath, subPath);
                        List<String> subOnlinePath = PathUtil.extend(onlinePath, subPath);
                        rootDir.removeIfExists(txn, subDataPath).get();
                        rootDir.move(txn, subOnlinePath, subDataPath).get();
                    }
                    rootDir.remove(txn, onlinePath).get();
                break;
                case TABLE:
                case GROUP:
                    // - move unaffected from data/foo/ to dataOnline/foo/
                    // - remove data/foo
                    // - move dataOnline/foo to data/foo/
                    if(rootDir.exists(txn, dataPath).get()) {
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
            RowData rowData = new RowData();
            expandGroupData(session, storeData, rowData);
            visitor.visit(storeData.persistitKey, rowData);
        }
    }

    public void expandGroupData(Session session, FDBStoreData storeData, RowData rowData) {
        unpackKey(storeData);
        expandRowData(session, storeData, rowData);
    }

    @Override
    public <V extends IndexVisitor<Key, Value>> V traverse(Session session, Index index, V visitor, long scanTimeLimit, long sleepTime) {
        FDBStoreData storeData = createStoreData(session, index);
        storeData.persistitValue = new Value((Persistit)null);
        TransactionState txn = txnService.getTransaction(session);
        long nextCommitTime = 0;
        if (scanTimeLimit >= 0) {
            nextCommitTime = txn.getStartTime() + scanTimeLimit;
        }
        indexIterator(session, storeData, false);
        while(storeData.next()) {
            // Key
            unpackKey(storeData);

            // Value
            unpackValue(storeData);

            // Visit
            visitor.visit(storeData.persistitKey, storeData.persistitValue);

            if ((scanTimeLimit >= 0) &&
                (System.currentTimeMillis() >= nextCommitTime)) {
                storeData.closeIterator();
                txn.getTransaction().commit().get();
                if (sleepTime > 0) {
                    try {
                        Thread.sleep(sleepTime);
                    }
                    catch (InterruptedException ex) {
                        throw new QueryCanceledException(session);
                    }
                }
                txn.reset();
                txn.getTransaction().reset();
                nextCommitTime = txn.getStartTime() + scanTimeLimit;
                indexIterator(session, storeData, false, false);
            }            
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
            Arrays.asList(FDBNameGenerator.DATA_PATH_NAME, FDBNameGenerator.SEQUENCE_PATH_NAME)
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
                      Transaction.ROW_LIMIT_UNLIMITED);
    }

    /** Resume iteration after <code>storeData.persistitKey</code>. */
    public void groupIterator(Session session, FDBStoreData storeData,
                              boolean restart, int limit) {
        groupIterator(session, storeData, 
                 restart ? GroupIteratorBoundary.NEXT_KEY : GroupIteratorBoundary.START, GroupIteratorBoundary.END, 
                 limit);
    }
    
    /** Iterate over just <code>storeData.persistitKey</code>, if present. */
    public void groupKeyIterator(Session session, FDBStoreData storeData) {
        // NOTE: Caller checks whether key returned matches.
        groupIterator(session, storeData, 
                      GroupIteratorBoundary.KEY, GroupIteratorBoundary.END,
                      1);
    }

    /** Iterate over <code>storeData.persistitKey</code>'s descendants. */
    public void groupDescendantsIterator(Session session, FDBStoreData storeData) {
        groupIterator(session, storeData, 
                      GroupIteratorBoundary.FIRST_DESCENDANT, GroupIteratorBoundary.LAST_DESCENDANT,
                      Transaction.ROW_LIMIT_UNLIMITED);
    }

    /** Iterate over <code>storeData.persistitKey</code>'s descendants. */
    public void groupKeyAndDescendantsIterator(Session session, FDBStoreData storeData) {
        groupIterator(session, storeData, 
                      GroupIteratorBoundary.KEY, GroupIteratorBoundary.LAST_DESCENDANT,
                      Transaction.ROW_LIMIT_UNLIMITED);
    }

    public void groupIterator(Session session, FDBStoreData storeData,
                              GroupIteratorBoundary left, GroupIteratorBoundary right,
                              int limit) {
        storeData.storageDescription.groupIterator(this, session, storeData,
                                                   left, right, limit);
    }

    /** Iterate over the whole index. */
    public void indexIterator(Session session, FDBStoreData storeData, 
                              boolean reverse) {
        indexIterator(session, storeData, 
                      false, false, reverse);
    }

    /** Iterate starting at current key. */
    public void indexIterator(Session session, FDBStoreData storeData,
                              boolean inclusive, boolean reverse) {
        indexIterator(session, storeData, 
                      true, inclusive, reverse);
    }

    public void indexIterator(Session session, FDBStoreData storeData,
                              boolean key, boolean inclusive, boolean reverse) {
        storeData.storageDescription.indexIterator(this, session, storeData,
                                                   key, inclusive, reverse);
    }

    //
    // Internal
    //

    private void constructIndexRow(Session session,
                                   Key indexKey,
                                   RowData rowData,
                                   Index index,
                                   Key hKey,
                                   PersistitIndexRowBuffer indexRow,
                                   boolean forInsert) {
        indexKey.clear();
        indexRow.resetForWrite(index, indexKey, null);
        indexRow.initialize(rowData, hKey);
        indexRow.close(session, this, forInsert);
    }

    private void checkUniqueness(Session session, TransactionState txn, Index index, RowData rowData, Key key) {
        if(index.isUnique() && !hasNullIndexSegments(rowData, index)) {
            int realSize = key.getEncodedSize();
            key.setDepth(index.getKeyColumns().size());
            try {
                checkKeyDoesNotExistInIndex(session, txn, rowData, index, key);
            } finally {
                key.setEncodedSize(realSize);
            }
        }
    }

    private void checkKeyDoesNotExistInIndex(Session session, TransactionState txn, RowData rowData, Index index, Key key) {
        assert index.isUnique() : index;
        FDBPendingIndexChecks.PendingCheck<?> check =
            FDBPendingIndexChecks.keyDoesNotExistInIndexCheck(session, txn, index, key);
        if (txn.getIndexChecks(false) == null) {
            check.blockUntilReady(txn);
            if (!check.check(session, txn, index)) {
                // Using RowData, can give better error than check.throwException().
                String msg = formatIndexRowString(session, rowData, index);
                throw new DuplicateKeyException(index.getIndexName(), msg);
            }
        }
        else {
            txn.getIndexChecks(false).add(session, txn, index, check);
        }
    }

    private static final ReadWriteMap.ValueCreator<Object, SequenceCache> SEQUENCE_CACHE_VALUE_CREATOR =
            new ReadWriteMap.ValueCreator<Object, SequenceCache>() {
                public SequenceCache createValueForKey (Object key) {
                    return new SequenceCache();
                }
            };

    private static class SequenceCache {
        private long value; 
        private long cacheSize;
        private final ReentrantLock cacheLock;

        
        public SequenceCache() {
            this(0L, 1L);
        }
        
        public SequenceCache(long startValue, long cacheSize) {
            this.value = startValue;
            this.cacheSize = startValue + cacheSize; 
            this.cacheLock = new ReentrantLock(false);
        }
        
        public void updateCache (long startValue, long cacheSize) {
            this.value = startValue;
            this.cacheSize = startValue + cacheSize;
        }
        
        public long nextCacheValue() {
            if (++value == cacheSize) {
                // ensure the next call to nextCacheValue also fails
                // and will do so until the updateCache() is called. 
                --value;
                return -1;
            }
            return value;
        }
        public long currentValue() {
            return value;
        }
        
        public void cacheLock() {
            cacheLock.lock();
        }
        
        public void cacheUnlock() {
            cacheLock.unlock();
        }
    }
}
