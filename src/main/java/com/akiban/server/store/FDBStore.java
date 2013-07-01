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
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.FDBAdapter;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.FDBAdapterException;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.util.ReadWriteMap;
import com.akiban.util.FDBCounter;
import com.foundationdb.KeySelector;
import com.foundationdb.KeyValue;
import com.foundationdb.RangeQuery;
import com.foundationdb.Retryable;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.ArrayUtil;
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
                    LockService lockService) {
        super(lockService, schemaManager);
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
        //print("Group scan: ", packedPrefix);
        return txn.getRangeStartsWith(packedPrefix).iterator();
    }

    // TODO: Creates range for hKey and descendants, add another API to specify
    public Iterator<KeyValue> groupIterator(Session session, Group group, Key hKey) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(group, hKey);
        Key after = createKey();
        hKey.copyTo(after);
        after.append(Key.AFTER);
        byte[] packedAfter = packedTuple(group, after);
        //print("Group scan: [", packedPrefix, ",", packedAfter, ")");
        return txn.getRange(packedPrefix, packedAfter).iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, boolean reverse) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(index);
        //print("    begin: ", packedPrefix);
        //print("  reverse: ", reverse);
        RangeQuery range = txn.getRangeStartsWith(packedPrefix);
        if(reverse) {
            range = range.reverse();
        }
        return range.iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, Key startsWith, boolean inclusive, boolean reverse) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedIndex = packedTuple(index);
        byte[] packedStart = packedTuple(index, startsWith);

        final KeySelector begin, end;
        if(inclusive) {
            if(reverse) {
                begin = KeySelector.firstGreaterThan(packedIndex);
                end = KeySelector.firstGreaterThan(packedStart);
            } else {
                begin = KeySelector.firstGreaterOrEqual(packedStart);
                end = KeySelector.firstGreaterThan(ArrayUtil.strinc(packedIndex));
            }
        } else {
            if(reverse) {
                begin = KeySelector.firstGreaterThan(packedIndex);
                end = KeySelector.firstGreaterOrEqual(packedStart);
            } else {
                begin = KeySelector.firstGreaterThan(packedStart);
                end = KeySelector.firstGreaterThan(ArrayUtil.strinc(packedIndex));
            }
        }

        //print("    begin: ", begin.getKey());
        //print("      end: ", end.getKey());
        //print("  reverse:", reverse);
        //print("inclusive:", inclusive);
        RangeQuery range = txn.getRange(begin, end);
        if(reverse) {
            range = range.reverse();
        }
        return range.iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, Key start, Key end, boolean reverse) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedStart = packedTuple(index, start);
        byte[] packedEnd = packedTuple(index, end);
        print("Index scan: [", packedStart, packedEnd, ") reverse: ", reverse);
        RangeQuery range = txn.getRange(packedStart, packedEnd);
        if(reverse) {
            range = range.reverse();
        }
        return range.iterator();
    }

    @Override
    public long nextSequenceValue(Session session, Sequence sequence) {
        long rawValue = 0;

        SequenceCache cache = sequenceCache.getOrCreateAndPut(
                sequence.getTreeName(), new ReadWriteMap.ValueCreator<String, SequenceCache>()
        {
            public SequenceCache createValueForKey(String treeName) {
                return getEmptyCache();
            }
        }
        );
       
        cache.cacheLock();
        try {
            rawValue = cache.nextCacheValue();
            if (rawValue < 0) {
                rawValue = updateCacheFromServer (cache, sequence);
            }
        } finally {
            cache.cacheUnlock();
        }
        long outValue = sequence.nextValueRaw(rawValue);
        return outValue;
    }

    // insert or update the sequence value from the server.
    // Works only under the cache lock from nextSequenceValue. 
    private long updateCacheFromServer (final SequenceCache cache, final Sequence sequence) {
        final long [] rawValue = new long[1];
        
        try {
            txnService.runTransaction(new Retryable (){
                @Override 
                public void attempt (Transaction tr) {
                    byte[] packedTuple = packedTuple(sequence);
                    byte[] byteValue = tr.get(packedTuple).get();
                    if(byteValue != null) {
                        Tuple tuple = Tuple.fromBytes(byteValue);
                        rawValue[0] = tuple.getLong(0);
                    } else {
                        rawValue[0] = 1;
                    }
                    tr.set(packedTuple, Tuple.from(rawValue[0] + sequence.getCacheSize()).pack());
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
        return sequence.currentValueRaw(rawValue);
    }

    private final ReadWriteMap<String, SequenceCache> sequenceCache = 
            ReadWriteMap.wrapFair(new TreeMap<String, SequenceCache>());

    public long getGICount(Session session, GroupIndex index) {
        Transaction txn = txnService.getTransaction(session);
        return cachedGICounter(session, index).getTransactional(txn);
    }

    public long getGICountApproximate(Session session, GroupIndex index) {
        Transaction txn = txnService.getTransaction(session);
        // Conflict free, but not faster than transactional
        return cachedGICounter(session, index).getSnapshot(txn);
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
        rowData.reset(storeData.value);
        rowData.prepareRow(0);
    }

    @Override
    protected void packRowData(FDBStoreData storeData, RowData rowData) {
        storeData.value = Arrays.copyOfRange(rowData.getBytes(), rowData.getBufferStart(), rowData.getBufferEnd());
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
        FDBCounter counter = cachedGICounter(session, index);
        Transaction txn = txnService.getTransaction(session);
        counter.add(txn, count);
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
            value.putByteArray(pkValue);
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
        constructIndexRow(indexKey, rowData, index, hKey, indexRow, true);
        checkUniqueness(txn, index, rowData, indexKey);

        byte[] packedKey = packedTuple(index, indexRow.getPKey());
        byte[] packedValue = Arrays.copyOf(
                indexRow.getPValue().getEncodedBytes(),
                indexRow.getPValue().getEncodedSize()
        );
        txn.set(packedKey, packedValue);
    }

    @Override
    protected void deleteIndexRow(Session session,
                                  Index index,
                                  RowData rowData,
                                  Key hKey,
                                  PersistitIndexRowBuffer indexRowBuffer) {
        Transaction txn = txnService.getTransaction(session);
        if (index.isUniqueAndMayContainNulls()) {
            // TODO: Is PersistitStore's broken w.r.t indexRow.hKey()?
            throw new UnsupportedOperationException("Can't delete unique index with nulls");
        } else {
            Key indexKey = createKey();
            constructIndexRow(indexKey, rowData, index, hKey, indexRowBuffer, false);
            txn.clear(packedTuple(index, indexKey));
        }
    }

    @Override
    protected void preWrite(Session session, FDBStoreData storeData, RowDef rowDef, RowData rowData) {
        // None
    }

    @Override
    protected void addChangeFor(Session session, UserTable table, Key hKey) {
        // None
    }

    @Override
    public void truncateTree(Session session, TreeLink treeLink) {
        Transaction txn = txnService.getTransaction(session);
        txn.clearRangeStartsWith(packedTuple(treeLink));
    }

    @Override
    public PersistitStore getPersistitStore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeTree(Session session, TreeLink treeLink) {
        if(!schemaManager.treeRemovalIsDelayed()) {
            truncateTree(session, treeLink);
            if(treeLink instanceof IndexDef) {
                Index index = ((IndexDef)treeLink).getIndex();
                if(index.isGroupIndex()) {
                    FDBCounter counter = cachedGICounter(session, (GroupIndex) index);
                    Transaction txn = txnService.getTransaction(session);
                    counter.clearState(txn);
                }
            }
        }
        schemaManager.treeWasRemoved(session, treeLink.getSchemaName(), treeLink.getTreeName());
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        super.truncateIndexes(session, indexes);
        Transaction txn = txnService.getTransaction(session);
        for(Index index : indexes) {
            if(index.isGroupIndex()) {
                cachedGICounter(session, (GroupIndex) index).set(txn, 0);
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
    public <V extends IndexVisitor<Key, Value>> V traverse(Session session, Index index, V visitor) {
        Key key = createKey();
        Value value = new Value((Persistit)null);
        Transaction txn = txnService.getTransaction(session);
        Iterator<KeyValue> it = txn.getRangeStartsWith(packedTuple(index)).iterator();
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
            System.arraycopy(valueBytes, 0, value.getEncodedBytes(), 0, valueBytes.length);
            value.setEncodedSize(valueBytes.length);

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

    private static void constructIndexRow(Key indexKey,
                                          RowData rowData,
                                          Index index,
                                          Key hKey,
                                          PersistitIndexRowBuffer indexRow,
                                          boolean forInsert) {
        indexKey.clear();
        indexRow.resetForWrite(index, indexKey, new Value((Persistit) null));
        indexRow.initialize(rowData, hKey);
        indexRow.close(forInsert);
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
        return txn.getRangeStartsWith(packedTuple(index, key)).iterator().hasNext();
    }

    private FDBCounter cachedGICounter(Session session, final GroupIndex index) {
        AkibanInformationSchema ais = getAIS(session);
        FDBCounter counter = ais.getCachedValue(index, null);
        if(counter == null) {
            // AIS attached is OK:
            // Multiple instances use the same prefix and any truncate/drop touches counter + entry keys
            counter = ais.getCachedValue(index, new CacheValueGenerator<FDBCounter>() {
                @Override
                public FDBCounter valueFor(AkibanInformationSchema ais) {
                    byte[] prefix = Tuple.from(index.indexDef().getTreeName(), "counter").pack();
                    return new FDBCounter(holder.getDatabase(), prefix, 0);
                }
            });
        }
        return counter;
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
        return Tuple.from(treeLink.getTreeName(), "/").pack();
    }

    private static byte[] packedTuple(TreeLink treeLink, Key key) {
        if(key.getEncodedSize() == 0) {
            return Tuple.from(treeLink.getTreeName(), "/").pack();
        } else {
            byte[] keyBytes = Arrays.copyOf(key.getEncodedBytes(), key.getEncodedSize());
            return Tuple.from(treeLink.getTreeName(), "/", keyBytes).pack();
        }
    }

    private SequenceCache getEmptyCache () {
        return new SequenceCache ();
    }
    
    private class SequenceCache {
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

    @SuppressWarnings("unused")
    private void print(Object... objects) {
        for(Object o : objects) {
            if(o instanceof byte[]) {
                Tuple t = Tuple.fromBytes((byte[])o);
                boolean f2 = true;
                System.out.print('(');
                for(Object v : t) {
                    if(!f2) {
                        System.out.print(", ");
                    }
                    f2 = false;
                    if(v instanceof byte[]) {
                        System.out.print(ArrayUtil.printable((byte[])v));
                    } else {
                        System.out.print(v);
                    }
                }
                System.out.print(')');
            } else {
                System.out.print(o);
            }
        }
        System.out.println();
    }
}
