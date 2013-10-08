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

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import com.foundationdb.qp.persistitadapter.FDBAdapter;
import com.foundationdb.qp.persistitadapter.PersistitHKey;
import com.foundationdb.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.FDBTableStatusCache;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.FDBAdapterException;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.IndexDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.lock.LockService;
import com.foundationdb.server.service.metrics.LongMetric;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.service.tree.TreeLink;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.util.ReadWriteMap;
import com.foundationdb.FDBException;
import com.foundationdb.KeySelector;
import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.ReadTransaction;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;
import com.foundationdb.util.layers.DirectorySubspace;
import com.google.inject.Inject;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Value;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Directory usage:
 * <pre>
 * &lt;root_dir&gt;/
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
public class FDBStore extends AbstractStore<FDBStoreData> implements Service {
    private static final Tuple INDEX_COUNT_DIR_PATH = Tuple.from("indexCount");
    private static final Tuple INDEX_NULL_DIR_PATH = Tuple.from("indexNull");
    private static final Logger LOG = LoggerFactory.getLogger(FDBStore.class);

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
    private byte[] packedIndexCountPrefix;
    private byte[] packedIndexNullPrefix;


    @Inject
    public FDBStore(FDBHolder holder,
                    ConfigurationService configService,
                    SchemaManager schemaManager,
                    TransactionService txnService,
                    LockService lockService,
                    ListenerService listenerService,
                    MetricsService metricsService) {
        super(lockService, schemaManager, listenerService);
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

    public Iterator<KeyValue> groupIterator(Session session, Group group) {
        TransactionState txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(group);
        return txn.getTransaction().getRange(Range.startsWith(packedPrefix)).iterator();
    }

    // TODO: Creates range for hKey and descendants, add another API to specify
    public Iterator<KeyValue> groupIterator(Session session, Group group, Key hKey) {
        TransactionState txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(group, hKey);
        Key after = createKey();
        hKey.copyTo(after);
        after.append(Key.AFTER);
        byte[] packedAfter = packedTuple(group, after);
        return txn.getTransaction().getRange(packedPrefix, packedAfter).iterator();
    }

    public Iterator<KeyValue> groupIterator(Session session, Group group,
                                            int limit, KeyValue restart) {
        TransactionState txn = txnService.getTransaction(session);
        KeySelector begin, end;
        byte[] packedPrefix = packedTuple(group);
        if (restart == null)
            begin = KeySelector.firstGreaterOrEqual(packedPrefix);
        else
            begin = KeySelector.firstGreaterThan(restart.getKey());
        end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(packedPrefix));
        return txn.getTransaction().getRange(begin, end, limit).iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, boolean reverse) {
        TransactionState txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(index);
        return txn.getTransaction().getRange(
            Range.startsWith(packedPrefix),
            Transaction.ROW_LIMIT_UNLIMITED,
            reverse
        ).iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, Key key, boolean inclusive, boolean reverse) {
        TransactionState txn = txnService.getTransaction(session);
        byte[] packedEdge = packedTuple(index);
        byte[] packedKey = packedTuple(index, key);

        // begin and end always need to be ordered properly (i.e begin less than end).
        // End values are *always* exclusive and KeySelector just picks which key ends up there (note strinc on edges).
        final KeySelector begin, end;
        if(inclusive) {
            if(reverse) {
                begin = KeySelector.firstGreaterThan(packedEdge);
                end = KeySelector.firstGreaterThan(packedKey);
            } else {
                begin = KeySelector.firstGreaterOrEqual(packedKey);
                end = KeySelector.firstGreaterThan(ByteArrayUtil.strinc(packedEdge));
            }
        } else {
            if(reverse) {
                begin = KeySelector.firstGreaterThan(packedEdge);
                end = KeySelector.firstGreaterOrEqual(packedKey);
            } else {
                begin = KeySelector.firstGreaterThan(packedKey);
                end = KeySelector.firstGreaterThan(ByteArrayUtil.strinc(packedEdge));
            }
        }

        return txn.getTransaction().getRange(begin, end, Transaction.ROW_LIMIT_UNLIMITED, reverse).iterator();
    }

    @Override
    public long nextSequenceValue(Session session, Sequence sequence) {
        long rawValue = 0;
        SequenceCache cache = sequenceCache.getOrCreateAndPut(sequence.getTreeName(), SEQUENCE_CACHE_VALUE_CREATOR);
        cache.cacheLock();
        try {
            rawValue = cache.nextCacheValue();
            if (rawValue < 0) {
                rawValue = updateCacheFromServer (session, cache, sequence);
            }
        } finally {
            cache.cacheUnlock();
        }
        long outValue = sequence.realValueForRawNumber(rawValue);
        return outValue;
    }

    // insert or update the sequence value from the server.
    // Works only under the cache lock from nextSequenceValue. 
    private long updateCacheFromServer (Session session, final SequenceCache cache, final Sequence sequence) {
        final long [] rawValue = new long[1];
        
        try {
            txnService.runTransaction(new Function<Transaction,Void> (){
                @Override 
                public Void apply (Transaction tr) {
                    byte[] packedTuple = packedTuple(sequence);
                    byte[] byteValue = tr.get(packedTuple).get();
                    if(byteValue != null) {
                        Tuple tuple = Tuple.fromBytes(byteValue);
                        rawValue[0] = tuple.getLong(0);
                    } else {
                        rawValue[0] = 1;
                    }
                    tr.set(packedTuple, Tuple.from(rawValue[0] + sequence.getCacheSize()).pack());
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
        
        SequenceCache cache = sequenceCache.get(sequence.getTreeName());
        if (cache != null) {
            rawValue = cache.currentValue();
        } else {
            TransactionState txn = txnService.getTransaction(session);
            byte[] byteValue = txn.getTransaction().get(packedTuple(sequence)).get();
            if(byteValue != null) {
                Tuple tuple = Tuple.fromBytes(byteValue);
                rawValue = tuple.getLong(0);
            }
        }
        return sequence.realValueForRawNumber(rawValue);
    }

    private final ReadWriteMap<String, SequenceCache> sequenceCache = 
            ReadWriteMap.wrapFair(new TreeMap<String, SequenceCache>());

    public long getGICount(Session session, GroupIndex index) {
        TransactionState txn = txnService.getTransaction(session);
        return getGICountInternal(txn.getTransaction(), index);
    }

    public long getGICountApproximate(Session session, GroupIndex index) {
        TransactionState txn = txnService.getTransaction(session);
        return getGICountInternal(txn.getTransaction().snapshot(), index);
    }

    public static void expandRowData(RowData rowData, KeyValue kv, boolean copyBytes) {
        expandRowData(rowData, kv.getValue(), copyBytes);
    }

    public static void expandRowData(RowData rowData, byte[] value, boolean copyBytes) {
        if(copyBytes) {
            byte[] rowBytes = rowData.getBytes();
            if((rowBytes == null) || (rowBytes.length < value.length)) {
                rowBytes = Arrays.copyOf(value, value.length);
                rowData.reset(rowBytes);
            } else {
                System.arraycopy(value, 0, rowBytes, 0, value.length);
                rowData.reset(0, value.length);
            }
        } else {
            rowData.reset(value);
        }
        rowData.prepareRow(0);
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
        packedIndexCountPrefix = holder.getDatabase().run(new Function<Transaction, byte[]>()
        {
            @Override
            public byte[] apply(Transaction txn) {
                DirectorySubspace dirSub = holder.getRootDirectory().createOrOpen(txn, INDEX_COUNT_DIR_PATH);
                return dirSub.pack();
            }
        });
        packedIndexNullPrefix = holder.getDatabase().run(new Function<Transaction, byte[]>()
        {
            @Override
            public byte[] apply(Transaction txn) {
                DirectorySubspace dirSub = holder.getRootDirectory().createOrOpen(txn, INDEX_NULL_DIR_PATH);
                return dirSub.pack();
            }
        });
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
    protected FDBStoreData createStoreData(Session session, TreeLink treeLink) {
        return new FDBStoreData(treeLink, createKey());
    }

    @Override
    protected void releaseStoreData(Session session, FDBStoreData storeData) {
        // None
    }

    @Override
    protected Key getKey(Session session, FDBStoreData storeData) {
        return storeData.key;
    }

    @Override
    protected void store(Session session, FDBStoreData storeData) {
        TransactionState txn = txnService.getTransaction(session);
        byte[] packedKey = packedTuple(storeData.link, storeData.key);
        byte[] value;
        if(storeData.persistitValue != null) {
            value = Arrays.copyOf(storeData.persistitValue.getEncodedBytes(), storeData.persistitValue.getEncodedSize());
        } else {
            value = storeData.value;
        }
        txn.setBytes(packedKey, value);
        rowsStoredMetric.increment();
    }

    @Override
    protected boolean fetch(Session session, FDBStoreData storeData) {
        TransactionState txn = txnService.getTransaction(session);
        byte[] packedKey = packedTuple(storeData.link, storeData.key);
        storeData.value = txn.getTransaction().get(packedKey).get();
        rowsFetchedMetric.increment();
        return (storeData.value != null);
    }

    @Override
    protected boolean clear(Session session, FDBStoreData storeData) {
        TransactionState txn = txnService.getTransaction(session);
        byte[] packed = packedTuple(storeData.link, storeData.key);
        // TODO: Remove get when clear() API changes
        boolean existed = (txn.getTransaction().get(packed).get() != null);
        txn.getTransaction().clear(packed);
        rowsClearedMetric.increment();
        return existed;
    }

    @Override
    void resetForWrite(FDBStoreData storeData, Index index, PersistitIndexRowBuffer indexRowBuffer) {
        if(storeData.persistitValue == null) {
            storeData.persistitValue = new Value((Persistit) null);
        }
        indexRowBuffer.resetForWrite(index, storeData.key, storeData.persistitValue);
    }

    @Override
    protected void expandRowData(FDBStoreData storeData, RowData rowData) {
        expandRowData(rowData, storeData.value, true);
    }

    @Override
    protected void packRowData(FDBStoreData storeData, RowData rowData) {
        storeData.value = Arrays.copyOfRange(rowData.getBytes(), rowData.getRowStart(), rowData.getRowEnd());
    }

    @Override
    protected Iterator<Void> createDescendantIterator(Session session, final FDBStoreData storeData) {
        TransactionState txn = txnService.getTransaction(session);
        int prevDepth = storeData.key.getDepth();
        storeData.key.append(Key.BEFORE);
        byte[] packedBegin = packedTuple(storeData.link, storeData.key);
        storeData.key.to(Key.AFTER);
        byte[] packedEnd = packedTuple(storeData.link, storeData.key);
        storeData.key.setDepth(prevDepth);
        storeData.it = txn.getTransaction().getRange(packedBegin, packedEnd).iterator();
        return new Iterator<Void>() {
            @Override
            public boolean hasNext() {
                return storeData.it.hasNext();
            }

            @Override
            public Void next() {
                KeyValue kv = storeData.it.next();
                unpackTuple(storeData.key, kv.getKey());
                storeData.value = kv.getValue();
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected void sumAddGICount(Session session, FDBStoreData storeData, GroupIndex index, int count) {
        TransactionState txn = txnService.getTransaction(session);
        txn.getTransaction().mutate(
            MutationType.ADD,
            packedTupleGICount(index),
            FDBTableStatusCache.packForAtomicOp(count)
        );
    }

    @Override
    protected PersistitIndexRowBuffer readIndexRow(Session session,
                                                   Index parentPKIndex,
                                                   FDBStoreData storeData,
                                                   RowDef childRowDef,
                                                   RowData childRowData) {
        Key parentPkKey = storeData.key;
        PersistitKeyAppender keyAppender = PersistitKeyAppender.create(parentPkKey);
        int[] fields = childRowDef.getParentJoinFields();
        for(int field : fields) {
            FieldDef fieldDef = childRowDef.getFieldDef(field);
            keyAppender.append(fieldDef, childRowData);
        }

        TransactionState txn = txnService.getTransaction(session);
        byte[] pkValue = txn.getTransaction().get(packedTuple(parentPKIndex, parentPkKey)).get();
        PersistitIndexRowBuffer indexRow = null;
        if (pkValue != null) {
            Value value = new Value((Persistit)null);
            value.putEncodedBytes(pkValue, 0, pkValue.length);
            indexRow = new PersistitIndexRowBuffer(this);
            indexRow.resetForRead(parentPKIndex, parentPkKey, value);
        }
        return indexRow;
    }

    @Override
    protected void writeIndexRow(Session session,
                                 Index index,
                                 RowData rowData,
                                 Key hKey,
                                 PersistitIndexRowBuffer indexRow) {
        TransactionState txn = txnService.getTransaction(session);
        Key indexKey = createKey();
        constructIndexRow(session, indexKey, rowData, index, hKey, indexRow, true);
        checkUniqueness(txn, index, rowData, indexKey);

        byte[] packedKey = packedTuple(index, indexRow.getPKey());
        byte[] packedValue = Arrays.copyOf(indexRow.getValue().getEncodedBytes(), indexRow.getValue().getEncodedSize());
        txn.setBytes(packedKey, packedValue);
    }

    @Override
    protected void deleteIndexRow(Session session,
                                  Index index,
                                  RowData rowData,
                                  Key hKey,
                                  PersistitIndexRowBuffer indexRowBuffer) {
        TransactionState txn = txnService.getTransaction(session);
        Key indexKey = createKey();
        // See big note in PersistitStore#deleteIndexRow() about format.
        if(index.isUniqueAndMayContainNulls()) {
            // IndexRow is used below, use these as intermediates.
            Key spareKey = indexRowBuffer.getPKey();
            Value spareValue = indexRowBuffer.getValue();
            if(spareKey == null) {
                spareKey = createKey();
            }
            if(spareValue == null) {
                spareValue = new Value((Persistit)null);
            }
            // Can't use a PIRB, because we need to get the hkey. Need a PersistitIndexRow.
            FDBAdapter adapter = createAdapter(session, SchemaCache.globalSchema(getAIS(session)));
            IndexRowType indexRowType = adapter.schema().indexRowType(index);
            PersistitIndexRow indexRow = adapter.takeIndexRow(indexRowType);
            constructIndexRow(session, indexKey, rowData, index, hKey, indexRow, false);
            // indexKey contains index values + 0 for null sep. Start there and iterate until we find hkey.
            Range r = new Range(packedTuple(index, indexKey), ByteArrayUtil.strinc(packedTuple(index)));
            for(KeyValue kv : txn.getTransaction().getRange(r)) {
                // Key
                unpackTuple(spareKey, kv.getKey());
                // Value
                byte[] valueBytes = kv.getValue();
                spareValue.clear();
                spareValue.putEncodedBytes(valueBytes, 0, valueBytes.length);
                // Delicate: copyFromKeyValue initializes the key returned by hKey
                indexRow.copyFrom(spareKey, spareValue);
                PersistitHKey rowHKey = (PersistitHKey)indexRow.hKey();
                if(rowHKey.key().compareTo(hKey) == 0) {
                    txn.getTransaction().clear(kv.getKey());
                    break;
                }
            }
            adapter.returnIndexRow(indexRow);
        } else {
            constructIndexRow(session, indexKey, rowData, index, hKey, indexRowBuffer, false);
            txn.getTransaction().clear(packedTuple(index, indexKey));
        }
    }

    @Override
    protected void preWrite(Session session, FDBStoreData storeData, RowDef rowDef, RowData rowData) {
        // None
    }

    @Override
    public void truncateTree(Session session, TreeLink treeLink) {
        TransactionState txn = txnService.getTransaction(session);
        txn.getTransaction().clear(Range.startsWith(packedTuple(treeLink)));
    }

    @Override
    public void deleteIndexes(Session session, Collection<? extends Index> indexes) {
        Transaction txn = txnService.getTransaction(session).getTransaction();
        for(Index index : indexes) {
            rootDir.removeIfExists(txn, FDBNameGenerator.dataPath(index));
            if(index.isGroupIndex()) {
                txn.clear(packedTupleGICount((GroupIndex)index));
            }
        }
    }

    @Override
    public void removeTrees(Session session, UserTable table) {
        Transaction txn = txnService.getTransaction(session).getTransaction();
        // Table and indexes (and group and group indexes if root table)
        rootDir.removeIfExists(txn, FDBNameGenerator.dataPath(table.getName()));
        // Sequence
        if(table.getIdentityColumn() != null) {
            deleteSequences(session, Collections.singleton(table.getIdentityColumn().getIdentityGenerator()));
        }
    }

    @Override
    public void removeTree(Session session, TreeLink treeLink) {
        if(!schemaManager.treeRemovalIsDelayed()) {
            truncateTree(session, treeLink);
            if(treeLink instanceof IndexDef) {
                Index index = ((IndexDef)treeLink).getIndex();
                if(index.isGroupIndex()) {
                    TransactionState txn = txnService.getTransaction(session);
                    txn.getTransaction().clear(packedTupleGICount((GroupIndex)index));
                }
            }
        }
        schemaManager.treeWasRemoved(session, treeLink.getSchemaName(), treeLink.getTreeName());
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        TransactionState txn = txnService.getTransaction(session);
        for(Index index : indexes) {
            truncateTree(session, index.indexDef());
            if(index.isGroupIndex()) {
                txn.setBytes(packedTupleGICount((GroupIndex)index), FDBTableStatusCache.packForAtomicOp(0));
            }
        }
    }

    @Override
    public void deleteSequences(Session session, Collection<? extends Sequence> sequences) {
        for (Sequence sequence : sequences) {
            sequenceCache.remove(sequence.getTreeName());
            rootDir.removeIfExists(
                txnService.getTransaction(session).getTransaction(),
                FDBNameGenerator.dataPath(sequence)
            );
        }
    }

    @Override
    public FDBAdapter createAdapter(Session session, Schema schema) {
        return new FDBAdapter(this, schema, session, txnService, configService);
    }

    @Override
    public boolean treeExists(Session session, String schemaName, String treeName) {
        TransactionState txn = txnService.getTransaction(session);
        return txn.getTransaction().getRange(Range.startsWith(packTreeName(treeName)), 1).iterator().hasNext();
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

    // TODO: A little ugly and slow, but unique indexes will get refactored soon and need for separator goes away.
    @Override
    public long nullIndexSeparatorValue(Session session, final Index index) {
        // New txn to avoid spurious conflicts
        return holder.getDatabase().run(new Function<Transaction,Long>() {
            @Override
            public Long apply(Transaction txn) {
                byte[] keyBytes = ByteArrayUtil.join(packedIndexNullPrefix, packTreeName(index.getTreeName()));
                byte[] valueBytes = txn.get(keyBytes).get();
                long outValue = 1;
                if(valueBytes != null) {
                    outValue += Tuple.fromBytes(valueBytes).getLong(0);
                }
                txn.set(keyBytes, Tuple.from(outValue).pack());
                return outValue;
            }
        });
    }

    @Override
    public void finishedAlter(Session session, Map<TableName, TableName> tableNames, ChangeLevel changeLevel) {
        if(changeLevel == ChangeLevel.NONE) {
            return;
        }

        Transaction txn = txnService.getTransaction(session).getTransaction();
        for(Entry<TableName, TableName> entry : tableNames.entrySet()) {
            TableName oldName = entry.getKey();
            TableName newName = entry.getValue();

            Tuple dataPath = FDBNameGenerator.dataPath(oldName);
            Tuple alterPath = FDBNameGenerator.alterPath(newName);

            switch(changeLevel) {
                case METADATA:
                case METADATA_NOT_NULL:
                    // - move renamed directories
                    if(!oldName.equals(newName)) {
                        schemaManager.renamingTable(session, oldName, newName);
                    }
                break;
                case INDEX:
                    if(!rootDir.exists(txn, alterPath)) {
                        continue;
                    }
                    // - Move everything from dataAltering/foo/ to data/foo/
                    // - remove dataAltering/foo/
                    for(Object subPath : rootDir.list(txn, alterPath)) {
                        Tuple subDataPath = dataPath.addObject(subPath);
                        Tuple subAlterPath = alterPath.addObject(subPath);
                        rootDir.move(txn, subAlterPath, subDataPath);
                    }
                    rootDir.remove(txn, alterPath);
                break;
                case TABLE:
                case GROUP:
                    if(!rootDir.exists(txn, alterPath)) {
                        continue;
                    }
                    // - move everything from data/foo/ to dataAltering/foo/
                    // - remove data/foo
                    // - move dataAltering/foo to data/foo/
                    if(rootDir.exists(txn, dataPath)) {
                        for(Object subPath : rootDir.list(txn, dataPath)) {
                            Tuple subDataPath = dataPath.addObject(subPath);
                            Tuple subAlterPath = alterPath.addObject(subPath);
                            if(!rootDir.exists(txn, subAlterPath)) {
                                rootDir.move(txn, subDataPath, subAlterPath);
                            }
                        }
                        rootDir.remove(txn, dataPath);
                    }
                    rootDir.move(txn, alterPath, dataPath);
                break;
                default:
                    throw new AkibanInternalException("Unexpected ChangeLevel: " + changeLevel);
            }
        }
    }

    @Override
    public void traverse(Session session, Group group, TreeRecordVisitor visitor) {
        TransactionState txn = txnService.getTransaction(session);
        visitor.initialize(session, this);
        Key key = createKey();
        for(KeyValue kv : txn.getTransaction().getRange(Range.startsWith(packedTuple(group)))) {
            // Key
            unpackTuple(key, kv.getKey());
            // Value
            RowData rowData = new RowData();
            expandRowData(rowData, kv, true);
            // Visit
            visitor.visit(key, rowData);
        }
    }

    @Override
    public <V extends IndexVisitor<Key, Value>> V traverse(Session session, Index index, V visitor, long scanTimeLimit, long sleepTime) {
        Key key = createKey();
        Value value = new Value((Persistit)null);
        TransactionState txn = txnService.getTransaction(session);
        long nextCommitTime = 0;
        if (scanTimeLimit >= 0) {
            nextCommitTime = txn.getStartTime() + scanTimeLimit;
        }
        byte[] packedPrefix = packedTuple(index);
        KeySelector start = KeySelector.firstGreaterOrEqual(packedPrefix);
        KeySelector end = KeySelector.firstGreaterThan(ByteArrayUtil.strinc(packedPrefix));
        Iterator<KeyValue> it = txn.getTransaction().getRange(start, end).iterator();
        while(it.hasNext()) {
            KeyValue kv = it.next();

            // Key
            unpackTuple(key, kv.getKey());

            // Value
            value.clear();
            byte[] valueBytes = kv.getValue();
            value.putEncodedBytes(valueBytes, 0, valueBytes.length);
            // Visit
            visitor.visit(key, value);

            if ((scanTimeLimit >= 0) &&
                (System.currentTimeMillis() >= nextCommitTime)) {
                ((AsyncIterator)it).dispose();
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
                start = KeySelector.firstGreaterThan(kv.getKey());
                it = txn.getTransaction().getRange(start, end).iterator();
            }            
        }
        return visitor;
    }

    @Override
    public String getName() {
        return "FoundationDB APIv" + holder.getAPIVersion();
    }


    //
    // KeyCreator
    //

    @Override
    public Key createKey() {
        return new Key(null, 2047);
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
        indexRow.resetForWrite(index, indexKey, new Value((Persistit) null));
        indexRow.initialize(rowData, hKey);
        indexRow.close(session, this, forInsert);
    }

    private void checkUniqueness(TransactionState txn, Index index, RowData rowData, Key key) {
        if(index.isUnique() && !hasNullIndexSegments(rowData, index)) {
            int segmentCount = index.indexDef().getIndexKeySegmentCount();
            // An index that isUniqueAndMayContainNulls has the extra null-separating field.
            if (index.isUniqueAndMayContainNulls()) {
                segmentCount++;
            }
            key.setDepth(segmentCount);
            checkKeyDoesNotExistInIndex(txn, index, key);
        }
    }

    private void checkKeyDoesNotExistInIndex(TransactionState txn, Index index, Key key) {
        assert index.isUnique() : index;
        byte[] bkey = packedTuple(index, key);
        Future<byte[]> future = txn.getTransaction().get(bkey);
        if (txn.getUniquenessChecks() == null) {
            long startNanos = System.nanoTime();
            future.blockUntilReady();
            long endNanos = System.nanoTime();
            txn.uniquenessTime += (endNanos - startNanos);
            if (future.get() != null) {
                throw new DuplicateKeyException(index.getIndexName().getName(), key);
            }
        }
        else {
            txn.getUniquenessChecks().add(txn, index, bkey, future);
        }
    }


    //
    // Static
    //

    public static void unpackTuple(Key key, byte[] tupleBytes) {
        Tuple t = Tuple.fromBytes(tupleBytes);
        byte[] keyBytes = t.getBytes(t.size() - 1);
        key.clear();
        if(key.getMaximumSize() < keyBytes.length) {
            key.setMaximumSize(keyBytes.length);
        }
        System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
        key.setEncodedSize(keyBytes.length);
    }

    private static byte[] packedTuple(Index index) {
        return packedTuple(index.indexDef());
    }

    private static byte[] packedTuple(Index index, Key key) {
        return packedTuple(index.indexDef(), key);
    }

    private static byte[] packedTuple(TreeLink treeLink) {
        return packTreeName(treeLink.getTreeName());
    }

    private static byte[] packedTuple(TreeLink treeLink, Key key) {
        return packedTuple(treeLink.getTreeName(), key);
    }

    public static byte[] packTreeName(String treeName) {
        return Base64.decodeBase64(treeName);
    }

    private static byte[] packedTuple(String treeName, Key key) {
        byte[] treeBytes = packTreeName(treeName);
        byte[] keyBytes = Arrays.copyOf(key.getEncodedBytes(), key.getEncodedSize());
        return ByteArrayUtil.join(treeBytes, Tuple.from(keyBytes).pack());
    }

    private byte[] packedTupleGICount(GroupIndex index) {
        return ByteArrayUtil.join(packedIndexCountPrefix, packTreeName(index.indexDef().getTreeName()));
    }

    private long getGICountInternal(ReadTransaction txn, GroupIndex index) {
        byte[] key = packedTupleGICount(index);
        byte[] value = txn.get(key).get();
        return FDBTableStatusCache.unpackForAtomicOp(value);
    }

    private static final ReadWriteMap.ValueCreator<String, SequenceCache> SEQUENCE_CACHE_VALUE_CREATOR =
            new ReadWriteMap.ValueCreator<String, SequenceCache>() {
                public SequenceCache createValueForKey (String treeName) {
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
