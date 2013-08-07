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

import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.qp.persistitadapter.FDBAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.FDBTableStatusCache;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.FDBAdapterException;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.listener.ListenerService;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.util.ReadWriteMap;
import com.foundationdb.FDBException;
import com.foundationdb.KeySelector;
import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.ReadTransaction;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

public class FDBStore extends AbstractStore<FDBStoreData> implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(FDBStore.class.getName());

    private final FDBHolder holder;
    private final ConfigurationService configService;
    private final SchemaManager schemaManager;
    private final FDBTransactionService txnService;

    @Inject
    public FDBStore(FDBHolder holder,
                    ConfigurationService configService,
                    SchemaManager schemaManager,
                    TransactionService txnService,
                    LockService lockService,
                    ListenerService listenerService) {
        super(lockService, schemaManager, listenerService);
        this.holder = holder;
        this.configService = configService;
        this.schemaManager = schemaManager;
        if(txnService instanceof FDBTransactionService) {
            this.txnService = (FDBTransactionService)txnService;
        } else {
            throw new IllegalStateException("Only usable with FDBTransactionService, found: " + txnService);
        }
    }

    public Iterator<KeyValue> groupIterator(Session session, Group group) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(group);
        return txn.getRange(Range.startsWith(packedPrefix)).iterator();
    }

    // TODO: Creates range for hKey and descendants, add another API to specify
    public Iterator<KeyValue> groupIterator(Session session, Group group, Key hKey) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(group, hKey);
        Key after = createKey();
        hKey.copyTo(after);
        after.append(Key.AFTER);
        byte[] packedAfter = packedTuple(group, after);
        return txn.getRange(packedPrefix, packedAfter).iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, boolean reverse) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(index);
        return txn.getRange(Range.startsWith(packedPrefix), Transaction.ROW_LIMIT_UNLIMITED, reverse).iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, Key key, boolean inclusive, boolean reverse) {
        Transaction txn = txnService.getTransaction(session);
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

        return txn.getRange(begin, end, Transaction.ROW_LIMIT_UNLIMITED, reverse).iterator();
    }

    @Override
    public long nextSequenceValue(Session session, Sequence sequence) {
        long rawValue = 0;
        SequenceCache cache = sequenceCache.getOrCreateAndPut(sequence.getTreeName(), SEQUENCE_CACHE_VALUE_CREATOR);
        cache.cacheLock();
        try {
            rawValue = cache.nextCacheValue();
            if (rawValue < 0) {
                rawValue = updateCacheFromServer (cache, sequence);
            }
        } finally {
            cache.cacheUnlock();
        }
        long outValue = sequence.realValueForRawNumber(rawValue);
        return outValue;
    }

    // insert or update the sequence value from the server.
    // Works only under the cache lock from nextSequenceValue. 
    private long updateCacheFromServer (final SequenceCache cache, final Sequence sequence) {
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
        } catch (Throwable e) {
            throw new FDBAdapterException(e);
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
            Transaction txn = txnService.getTransaction(session);
            byte[] byteValue = txn.get(packedTuple(sequence)).get();
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
        Transaction txn = txnService.getTransaction(session);
        return getGICountInternal(txn, index);
    }

    public long getGICountApproximate(Session session, GroupIndex index) {
        Transaction txn = txnService.getTransaction(session);
        return getGICountInternal(txn.snapshot(), index);
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
        Transaction txn = txnService.getTransaction(session);
        byte[] packedKey = packedTuple(storeData.link, storeData.key);
        byte[] value;
        if(storeData.persistitValue != null) {
            value = Arrays.copyOf(storeData.persistitValue.getEncodedBytes(), storeData.persistitValue.getEncodedSize());
        } else {
            value = storeData.value;
        }
        txn.set(packedKey, value);
    }

    @Override
    protected boolean fetch(Session session, FDBStoreData storeData) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedKey = packedTuple(storeData.link, storeData.key);
        storeData.value = txn.get(packedKey).get();
        return (storeData.value != null);
    }

    @Override
    protected boolean clear(Session session, FDBStoreData storeData) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packed = packedTuple(storeData.link, storeData.key);
        // TODO: Remove get when clear() API changes
        boolean existed = (txn.get(packed).get() != null);
        txn.clear(packed);
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
        Transaction txn = txnService.getTransaction(session);
        int prevDepth = storeData.key.getDepth();
        storeData.key.append(Key.BEFORE);
        byte[] packedBegin = packedTuple(storeData.link, storeData.key);
        storeData.key.to(Key.AFTER);
        byte[] packedEnd = packedTuple(storeData.link, storeData.key);
        storeData.key.setDepth(prevDepth);
        storeData.it = txn.getRange(packedBegin, packedEnd).iterator();
        return new Iterator<Void>() {
            @Override
            public boolean hasNext() {
                return storeData.it.hasNext();
            }

            @Override
            public Void next() {
                KeyValue kv = storeData.it.next();
                Tuple tuple = Tuple.fromBytes(kv.getKey());

                byte[] keyBytes = tuple.getBytes(2);
                System.arraycopy(keyBytes, 0, storeData.key.getEncodedBytes(), 0, keyBytes.length);
                storeData.key.setEncodedSize(keyBytes.length);
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
        Transaction txn = txnService.getTransaction(session);
        txn.mutate(MutationType.ADD, packedTupleGICount(index), FDBTableStatusCache.packForAtomicOp(count));
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

        Transaction txn = txnService.getTransaction(session);
        byte[] pkValue = txn.get(packedTuple(parentPKIndex, parentPkKey)).get();
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
        Transaction txn = txnService.getTransaction(session);
        Key indexKey = createKey();
        constructIndexRow(session, indexKey, rowData, index, hKey, indexRow, true);
        checkUniqueness(txn, index, rowData, indexKey);

        byte[] packedKey = packedTuple(index, indexRow.getPKey());
        byte[] packedValue = Arrays.copyOf(indexRow.getValue().getEncodedBytes(), indexRow.getValue().getEncodedSize());
        txn.set(packedKey, packedValue);
    }

    @Override
    protected void deleteIndexRow(Session session,
                                  Index index,
                                  RowData rowData,
                                  Key hKey,
                                  PersistitIndexRowBuffer indexRowBuffer) {
        Transaction txn = txnService.getTransaction(session);
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
            for(KeyValue kv : txn.getRange(Range.startsWith(packedTuple(index, indexKey)))) {
                // Key
                byte[] keyBytes = Tuple.fromBytes(kv.getKey()).getBytes(2);
                spareKey.setEncodedSize(keyBytes.length);
                System.arraycopy(keyBytes, 0, spareKey.getEncodedBytes(), 0, keyBytes.length);
                // Value
                byte[] valueBytes = kv.getValue();
                spareValue.clear();
                spareValue.putEncodedBytes(valueBytes, 0, valueBytes.length);
                // Delicate: copyFromKeyValue initializes the key returned by hKey
                indexRow.copyFrom(spareKey, spareValue);
                PersistitHKey rowHKey = (PersistitHKey)indexRow.hKey();
                if(rowHKey.key().compareTo(hKey) == 0) {
                    txn.clear(kv.getKey());
                    break;
                }
            }
            adapter.returnIndexRow(indexRow);
        } else {
            constructIndexRow(session, indexKey, rowData, index, hKey, indexRowBuffer, false);
            txn.clear(packedTuple(index, indexKey));
        }
    }

    @Override
    protected void preWrite(Session session, FDBStoreData storeData, RowDef rowDef, RowData rowData) {
        // None
    }

    @Override
    public void truncateTree(Session session, TreeLink treeLink) {
        Transaction txn = txnService.getTransaction(session);
        txn.clearRangeStartsWith(packedTuple(treeLink));
    }

    @Override
    public void removeTree(Session session, TreeLink treeLink) {
        if(!schemaManager.treeRemovalIsDelayed()) {
            truncateTree(session, treeLink);
            if(treeLink instanceof IndexDef) {
                Index index = ((IndexDef)treeLink).getIndex();
                if(index.isGroupIndex()) {
                    Transaction txn = txnService.getTransaction(session);
                    txn.clear(packedTupleGICount((GroupIndex)index));
                }
            }
        }
        schemaManager.treeWasRemoved(session, treeLink.getSchemaName(), treeLink.getTreeName());
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        Transaction txn = txnService.getTransaction(session);
        for(Index index : indexes) {
            truncateTree(session, index.indexDef());
            if(index.isGroupIndex()) {
                txn.set(packedTupleGICount((GroupIndex)index), FDBTableStatusCache.packForAtomicOp(0));
            }
        }
    }

    @Override
    public void deleteSequences(Session session, Collection<? extends Sequence> sequences) {
        for (Sequence sequence : sequences) {
            sequenceCache.remove(sequence.getTreeName());
        }
        removeTrees(session, sequences);
    }

    @Override
    public FDBAdapter createAdapter(Session session, Schema schema) {
        return new FDBAdapter(this, schema, session, configService);
    }

    @Override
    public boolean treeExists(Session session, String schemaName, String treeName) {
        Transaction txn = txnService.getTransaction(session);
        return txn.getRange(Range.startsWith(packedTuple(treeName)), 1).iterator().hasNext();
    }

    @Override
    public boolean isRetryableException(Throwable t) {
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
        final long[] value = { 1 };
        try {
            // New txn to avoid spurious conflicts
            holder.getDatabase().run(new Function<Transaction,Void>() {
                @Override
                public Void apply(Transaction txn) {
                    byte[] keyBytes = Tuple.from("indexNull", index.indexDef().getTreeName()).pack();
                    byte[] valueBytes = txn.get(keyBytes).get();
                    value[0] = 1;
                    if(valueBytes != null) {
                        value[0] += Tuple.fromBytes(valueBytes).getLong(0);
                    }
                    txn.set(keyBytes, Tuple.from(value[0]).pack());
                    return null;
                }
            });
        } catch(Throwable t) {
            throw new AkibanInternalException("Unexpected", t);
        }
        return value[0];
    }

    @Override
    public void traverse(Session session, Group group, TreeRecordVisitor visitor) {
        Transaction txn = txnService.getTransaction(session);
        visitor.initialize(session, this);
        Key key = createKey();
        for(KeyValue kv : txn.getRange(Range.startsWith(packedTuple(group)))) {
            // Key
            key.clear();
            byte[] keyBytes = Tuple.fromBytes(kv.getKey()).getBytes(2);
            key.setEncodedSize(keyBytes.length);
            System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
            // Value
            RowData rowData = new RowData();
            expandRowData(rowData, kv, true);
            // Visit
            visitor.visit(key, rowData);
        }
    }

    @Override
    public <V extends IndexVisitor<Key, Value>> V traverse(Session session, Index index, V visitor) {
        Key key = createKey();
        Value value = new Value((Persistit)null);
        Transaction txn = txnService.getTransaction(session);
        Iterator<KeyValue> it = txn.getRange(Range.startsWith(packedTuple(index))).iterator();
        while(it.hasNext()) {
            KeyValue kv = it.next();

            // Key
            key.clear();
            byte[] keyBytes = Tuple.fromBytes(kv.getKey()).getBytes(2);
            System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
            key.setEncodedSize(keyBytes.length);

            // Value
            value.clear();
            byte[] valueBytes = kv.getValue();
            value.putEncodedBytes(valueBytes, 0, valueBytes.length);
            // Visit
            visitor.visit(key, value);
        }
        return visitor;
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

    private void checkUniqueness(Transaction txn, Index index, RowData rowData, Key key) {
        if(index.isUnique() && !hasNullIndexSegments(rowData, index)) {
            int segmentCount = index.indexDef().getIndexKeySegmentCount();
            // An index that isUniqueAndMayContainNulls has the extra null-separating field.
            if (index.isUniqueAndMayContainNulls()) {
                segmentCount++;
            }
            key.setDepth(segmentCount);
            if(keyExistsInIndex(txn, index, key)) {
                throw new DuplicateKeyException(index.getIndexName().getName(), key);
            }
        }
    }

    private boolean keyExistsInIndex(Transaction txn, Index index, Key key) {
        assert index.isUnique() : index;
        return txn.get(packedTuple(index, key)).get() != null;
    }


    //
    // Static
    //

    private static byte[] packedTuple(Index index) {
        return packedTuple(index.indexDef());
    }

    private static byte[] packedTuple(Index index, Key key) {
        return packedTuple(index.indexDef(), key);
    }

    private static byte[] packedTuple(TreeLink treeLink) {
        return packedTuple(treeLink.getTreeName());
    }

    private static byte[] packedTuple(TreeLink treeLink, Key key) {
        return packedTuple(treeLink.getTreeName(), key);
    }

    private static byte[] packedTuple(String treeName) {
        return Tuple.from(treeName, "/").pack();
    }

    private static byte[] packedTuple(String treeName, Key key) {
        byte[] keyBytes = Arrays.copyOf(key.getEncodedBytes(), key.getEncodedSize());
        return Tuple.from(treeName, "/", keyBytes).pack();
    }

    private static byte[] packedTupleGICount(GroupIndex index) {
        return Tuple.from("indexCount", index.indexDef().getTreeName()).pack();
    }

    private static long getGICountInternal(ReadTransaction txn, GroupIndex index) {
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
