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
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.FDBAdapter;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.service.tree.TreeLink;
import com.foundationdb.KeyValue;
import com.foundationdb.RangeQuery;
import com.foundationdb.Transaction;
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

public class FDBStore extends AbstractStore<FDBStoreData> implements KeyCreator, Service {
    private static final Logger LOG = LoggerFactory.getLogger(FDBStore.class.getName());

    private final ConfigurationService configService;
    private final SchemaManager schemaManager;
    private final FDBTransactionService txnService;

    @Inject
    public FDBStore(ConfigurationService configService,
                    SchemaManager schemaManager,
                    TransactionService txnService,
                    LockService lockService) {
        super(lockService, schemaManager);
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

    // TODO: Creates range for hKey and descendents, add another API to specify
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
        //print("Index scan: ", packedPrefix, "reverse: ", reverse);
        RangeQuery range = txn.getRangeStartsWith(packedPrefix);
        if(reverse) {
            range = range.reverse();
        }
        return range.iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, Key start, Key end, boolean reverse) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedStart = packedTuple(index, start);
        byte[] packedEnd = packedTuple(index, end);
        //print("Index scan: [", packedStart, ",", packedEnd, ")", " reverse:", reverse);
        RangeQuery range = txn.getRange(packedStart, packedEnd);
        if(reverse) {
            range = range.reverse();
        }
        return range.iterator();
    }

    public long nextSequenceValue(Session session, Sequence sequence) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedTuple = packedTuple(sequence);
        long rawValue = 0;
        byte[] byteValue = txn.get(packedTuple).get();
        if(byteValue != null) {
            Tuple tuple = Tuple.fromBytes(byteValue);
            rawValue = tuple.getLong(0);
        }
        rawValue += 1;
        long outValue = sequence.nextValueRaw(rawValue);
        txn.set(packedTuple, Tuple.from(rawValue).pack());
        return outValue;
    }

    public long curSequenceValue(Session session, Sequence sequence) {
        Transaction txn = txnService.getTransaction(session);
        long rawValue = 0;
        byte[] byteValue = txn.get(packedTuple(sequence)).get();
        if(byteValue != null) {
            Tuple tuple = Tuple.fromBytes(byteValue);
            rawValue = tuple.getLong(0);
        }
        return sequence.currentValueRaw(rawValue);
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
        // TODO: Remove when API changes
        boolean existed = (txn.get(packed) != null);
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
        // TODO
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
        byte[] packedValue = Arrays.copyOf(indexRow.getPValue().getEncodedBytes(), indexRow.getPValue().getEncodedSize());
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
        }
        schemaManager.treeWasRemoved(session, treeLink.getSchemaName(), treeLink.getTreeName());
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        super.truncateIndexes(session, indexes);
        // TODO: GI row counts
    }

    @Override
    public void deleteSequences(Session session, Collection<? extends Sequence> sequences) {
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
        byte[] keyBytes = Arrays.copyOf(key.getEncodedBytes(), key.getEncodedSize());
        return Tuple.from(treeLink.getTreeName(), "/", keyBytes).pack();
    }

    @SuppressWarnings("unused")
    private static void print(Object... objects) {
        for(Object o : objects) {
            if(o instanceof byte[]) {
                byte[] packed = (byte[])o;
                System.out.print("'");
                for(byte b : packed) {
                    int c = 0xFF & b;
                    if(c < 32 || c > 126) {
                        System.out.printf("\\x%02x", c);
                    } else {
                        System.out.printf("%c", (char)c);
                    }
                }
                System.out.print("'");
            } else{
                System.out.print(o);
            }
        }
        System.out.println();
    }
}
