/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.WriteIndexRow;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.storeadapter.MemoryAdapter;
import com.foundationdb.qp.storeadapter.indexrow.SpatialColumnHandler;
import com.foundationdb.qp.storeadapter.indexrow.MemoryIndexRow;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.*;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.MemoryStorageDescription;
import com.foundationdb.server.types.aksql.aktypes.*;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.util.Strings;
import com.google.inject.Inject;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Value;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;

public class MemoryStore extends AbstractStore<MemoryStore, MemoryStoreData, MemoryStorageDescription> implements Service
{
    static final byte[] BYTES_EMPTY = new byte[0];
    static final byte[] BYTES_00 = { (byte)0x00 };
    static final byte[] BYTES_FF = { (byte)0xFF };

    private final ConfigurationService configService;
    private final MemoryTransactionService txnService;

    @Inject
    public MemoryStore(ConfigurationService configService,
                       TransactionService txnService,
                       SchemaManager schemaManager,
                       ListenerService listenerService,
                       TypesRegistryService typesRegistryService,
                       ServiceManager serviceManager) {
        super(txnService, schemaManager, listenerService, typesRegistryService, serviceManager);
        this.configService = configService;
        this.txnService = (MemoryTransactionService)txnService;
    }

    //
    // Service
    //

    @Override
    public void start() {
        this.constraintHandler = new MemoryConstraintHandler(this,
                                                              txnService,
                                                              configService,
                                                              typesRegistryService,
                                                              serviceManager);
        boolean withConcurrentDML = Boolean.parseBoolean(configService.getProperty(FEATURE_DDL_WITH_DML_PROP));
        this.onlineHelper = new OnlineHelper(txnService,
                                             schemaManager,
                                             this,
                                             typesRegistryService,
                                             constraintHandler,
                                             withConcurrentDML);
        listenerService.registerRowListener(onlineHelper);
    }

    @Override
    public void stop() {
        // None
    }

    @Override
    public void crash() {
        stop();
    }

    //
    // AbstractStore
    //

    @Override
    MemoryStoreData createStoreData(Session session, MemoryStorageDescription storageDescription) {
        return new MemoryStoreData(session, storageDescription, createKey(), createKey());
    }

    @Override
    void releaseStoreData(Session session, MemoryStoreData storeData) {
        // None
    }

    @Override
    MemoryStorageDescription getStorageDescription(MemoryStoreData storeData) {
        return storeData.storageDescription;
    }

    @Override
    Key getKey(Session session, MemoryStoreData storeData) {
        return storeData.persistitKey;
    }

    @Override
    void store(Session session, MemoryStoreData storeData) {
        MemoryTransaction txn = getTransaction(session);
        packKey(storeData);
        if(storeData.persistitValue != null) {
            storeData.rawValue = Arrays.copyOf(storeData.persistitValue.getEncodedBytes(),
                                               storeData.persistitValue.getEncodedSize());
        }
        txn.set(storeData.rawKey, storeData.rawValue);
    }

    @Override
    boolean fetch(Session session, MemoryStoreData storeData) {
        MemoryTransaction txn = getTransaction(session);
        packKey(storeData);
        storeData.rawValue = txn.get(storeData.rawKey);
        return (storeData.rawValue != null);
    }

    @Override
    void clear(Session session, MemoryStoreData storeData) {
        MemoryTransaction txn = getTransaction(session);
        packKey(storeData);
        txn.clear(storeData.rawKey);
    }

    @Override
    void resetForWrite(MemoryStoreData storeData, Index index, WriteIndexRow indexRowBuffer) {
        if(storeData.persistitValue == null) {
            storeData.persistitValue = new Value((Persistit) null);
        }
        indexRowBuffer.resetForWrite(index, storeData.persistitKey, storeData.persistitValue);
    }

    @Override
    protected Iterator<Void> createDescendantIterator(Session session, final MemoryStoreData storeData) {
        groupDescendantsIterator(session, storeData);
        return new Iterator<Void>() {
            @Override
            public boolean hasNext() {
                return storeData.iterator.hasNext();
            }

            @Override
            public Void next() {
                Entry<byte[], byte[]> entry = storeData.iterator.next();
                storeData.rawKey = entry.getKey();
                unpackKey(storeData);
                // Unpacked by caller
                storeData.rawValue = entry.getValue();
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected IndexRow readIndexRow(Session session, Index parentPKIndex, MemoryStoreData storeData, Row childRow) {
        MemoryTransaction txn = getTransaction(session);
        Key parentPkKey = storeData.persistitKey;
        PersistitKeyAppender keyAppender = PersistitKeyAppender.create(parentPkKey, parentPKIndex.getIndexName());
        for(Column column : childRow.rowType().table().getParentJoin().getChildColumns()) {
            keyAppender.append(childRow.value(column.getPosition()), column);
        }
        // Only called when child row does not contain full HKey.
        // Key contents are the logical parent of the actual index entry (if it exists).
        byte[] key = packKey(parentPKIndex, parentPkKey);
        byte[] begin = join(key, BYTES_00);
        byte[] end = join(key, BYTES_FF);
        Iterator<Entry<byte[], byte[]>> it = txn.getRange(begin, end);
        MemoryIndexRow indexRow = null;
        if (it.hasNext()) {
            Entry<byte[], byte[]> entry = it.next();
            assert !it.hasNext() : parentPKIndex;
            assert entry.getValue().length == 0 : parentPKIndex + ", " + Strings.hex(entry.getValue());
            indexRow = new MemoryIndexRow(this);
            unpackKey(parentPKIndex, entry.getKey(), parentPkKey);
            indexRow.resetForRead(parentPKIndex, parentPkKey, null);
        }
        return indexRow;
    }

    @Override
    protected void lock(Session session, MemoryStoreData storeData, Row row) {
        MemoryTransaction txn = getTransaction(session);
        packKey(storeData);
        txn.get(storeData.rawKey);
    }

    @Override
    protected void trackTableWrite(Session session, Table table) {
        // Needed?
    }

    //
    // Store
    //

    @Override
    public void writeIndexRow(Session session,
                              TableIndex index,
                              Row row,
                              Key hKey,
                              WriteIndexRow indexRow,
                              SpatialColumnHandler spatialColumnHandler,
                              long zValue,
                              boolean doLock) {
        MemoryTransaction txn = getTransaction(session);
        Key indexKey = createKey();
        constructIndexRow(session, indexKey, row, index, hKey, indexRow, spatialColumnHandler, zValue, true);
        checkUniqueness(session, txn, index, row, indexKey);
        byte[] rawKey = packKey(index, indexKey);
        txn.set(rawKey, BYTES_EMPTY);
    }

    @Override
    public void deleteIndexRow(Session session,
                               TableIndex index,
                               Row row,
                               Key hKey,
                               WriteIndexRow indexRow,
                               SpatialColumnHandler spatialColumnHandler,
                               long zValue,
                               boolean doLock) {
        MemoryTransaction txn = getTransaction(session);
        Key indexKey = createKey();
        constructIndexRow(session, indexKey, row, index, hKey, indexRow, spatialColumnHandler, zValue, false);
        byte[] packed = packKey(index, indexKey);
        txn.clear(packed);
    }

    @Override
    public long nextSequenceValue(Session session, Sequence sequence) {
        MemoryTransaction txn = getTransaction(session);
        byte[] key = packKey(sequence);
        byte[] value = txn.get(key);
        long nextVal = 1;
        if(value != null) {
            nextVal += unpackLong(value);
        }
        txn.set(key, packLong(nextVal));
        return sequence.realValueForRawNumber(nextVal);
    }

    @Override
    public long curSequenceValue(Session session, Sequence sequence) {
        MemoryTransaction txn = getTransaction(session);
        byte[] value = txn.get(packKey(sequence));
        long seqValue = (value != null) ? unpackLong(value) : 0;
        return sequence.realValueForRawNumber(seqValue);
    }

    @Override
    public void deleteSequences(Session session, Collection<? extends Sequence> sequences) {
        MemoryTransaction txn = getTransaction(session);
        for(Sequence s : sequences) {
            txn.clear(packKey(s));
        }
    }

    @Override
    public void removeTree(Session session, HasStorage object) {
        MemoryTransaction txn = getTransaction(session);
        byte[] key = packKey(object);
        txn.clearRange(key, join(key, BYTES_FF));
    }

    @Override
    public void truncateTree(Session session, HasStorage object) {
        removeTree(session, object);
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        for(Index i : indexes) {
            truncateTree(session, i);
        }
    }

    @Override
    public StoreAdapter createAdapter(Session session) {
        return new MemoryAdapter(session, configService, this);
    }

    @Override
    public boolean treeExists(Session session, StorageDescription storageDescription) {
        MemoryTransaction txn = getTransaction(session);
        byte[] key = packKey(storageDescription);
        byte[] begin = join(key, BYTES_00);
        byte[] end = join(key, BYTES_FF);
        return txn.getRange(begin, end).hasNext();
    }

    @Override
    public void traverse(Session session, Group group, TreeRecordVisitor visitor) {
        visitor.initialize(session, this);
        MemoryStoreData storeData = createStoreData(session, group);
        groupIterator(session, storeData);
        while(storeData.next()) {
            Row row = expandGroupData(session, storeData, SchemaCache.globalSchema(group.getAIS()));
            visitor.visit(storeData.persistitKey, row);
        }
        releaseStoreData(session, storeData);
    }

    @Override
    public <V extends IndexVisitor<Key, Value>> V traverse(Session session,
                                                           Index index,
                                                           V visitor,
                                                           long scanTimeLimit,
                                                           long sleepTime) {
        MemoryStoreData storeData = createStoreData(session, index);
        storeData.persistitValue = new Value((Persistit)null);
        indexIterator(session, storeData, false);
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
    public void discardOnlineChange(Session session, Collection<ChangeSet> changeSets) {
        // TODO: Find all table and sequences being modified, clear their prefix
    }

    @Override
    public void finishOnlineChange(Session session, Collection<ChangeSet> changeSets) {
        // None
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public Collection<String> getStorageDescriptionNames(final Session session) {
        return txnService.run(session, new Callable<Collection<String>>() {
            @Override
            public Collection<String> call() throws Exception {
                MemoryTransaction txn = txnService.getTransaction(session);
                Set<String> names = new TreeSet<>();
                /* TODO: This found a bug in ALTER processing (that isn't symptomatic with FDBStore).
                 Pretend there are no storage names to ignore for now.
                Iterator<Entry<byte[], byte[]>> it = txn.getRange(BYTES_00, BYTES_FF);
                while(it.hasNext()) {
                    Entry<byte[], byte[]> entry = it.next();
                    UUID uuid = unpackUUID(entry.getKey());
                    names.add(uuid.toString());
                }*/
                return names;
            }
        });
    }

    @Override
    public Class<? extends Exception> getOnlineDMLFailureException() {
        return LockTimeoutException.class;
    }
    
    @Override
    public Row storeLobs(Session session, Row row){
        // lobs not supported
        if (AkBlob.containsBlob(row.rowType())) {
            throw new LobException("MemoryStore does not support blobs");
        }
        return row;
    }

    @Override
    void deleteLobs(Session session, Row row) {
        // lobs not supported
    }

    @Override
    public void dropAllLobs(Session session) {
        // lobs not supported
    }

    @Override
    protected void registerLobForOnlineDelete(Session session, TableName table, UUID uuid) {
        // lobs not supported
    }

    @Override
    protected void executeLobOnlineDelete(Session session, TableName table) {
        // lobs not supported
    }
    
    @Override
    public boolean isRestartable() {
        return false;
    }

    //
    // KeyCreator
    //

    @Override
    public Key createKey() {
        return new Key(null, 2047);
    }

    //
    // TreeMapStore
    //

    public Row expandGroupData(Session session, MemoryStoreData storeData, Schema schema) {
        unpackKey(storeData);
        return expandRow(session, storeData, schema);
    }

    /** Iterate over the whole group. */
    public void groupIterator(Session session, MemoryStoreData storeData) {
        assert storeData.storageDescription.getObject() instanceof Group : storeData.storageDescription;
        MemoryTransaction txn = getTransaction(session);
        byte[] uuidBytes = storeData.storageDescription.getUUIDBytes();
        storeData.iterator = txn.getRange(uuidBytes, join(uuidBytes, BYTES_FF));
    }

    /** Iterator over *just* storeData.persistitKey */
    public void groupKeyIterator(Session session, MemoryStoreData storeData) {
        assert storeData.storageDescription.getObject() instanceof Group : storeData.storageDescription;
        MemoryTransaction txn = getTransaction(session);
        final byte[] key = packKey(storeData.storageDescription, storeData.persistitKey);
        final byte[] value = txn.get(key);
        storeData.iterator = new Iterator<Entry<byte[], byte[]>>() {
            boolean hasReturned = false;

            @Override
            public boolean hasNext() {
                return !hasReturned && (value != null);
            }

            @Override
            public Entry<byte[], byte[]> next() {
                if(!hasNext()) {
                    throw new NoSuchElementException();
                }
                hasReturned = true;
                return new Entry<byte[], byte[]>()  {
                    @Override
                    public byte[] getKey() {
                        return key;
                    }

                    @Override
                    public byte[] getValue() {
                        return value;
                    }

                    @Override
                    public byte[] setValue(byte[] value) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** Iterate over key *and* all descendants. */
    public void groupKeyAndDescendantsIterator(Session session, MemoryStoreData storeData) {
        assert storeData.storageDescription.getObject() instanceof Group : storeData.storageDescription;
        MemoryTransaction txn = getTransaction(session);
        packKey(storeData);
        byte[] begin = storeData.rawKey;
        byte[] end = join(storeData.rawKey, BYTES_FF);
        storeData.iterator = txn.getRange(begin, end);
    }

    /** Iterate over *just* the descendants of storeDate.persistitKey */
    public void groupDescendantsIterator(Session session, MemoryStoreData storeData) {
        assert storeData.storageDescription.getObject() instanceof Group : storeData.storageDescription;
        MemoryTransaction txn = getTransaction(session);
        packKey(storeData);
        byte[] begin = join(storeData.rawKey, BYTES_00);
        byte[] end = join(storeData.rawKey, BYTES_FF);
        storeData.iterator = txn.getRange(begin, end);
    }

    public void indexIterator(Session session, MemoryStoreData storeData, boolean reverse) {
        assert storeData.storageDescription.getObject() instanceof Index : storeData.storageDescription;
        MemoryTransaction txn = getTransaction(session);
        if(reverse) {
            byte[] begin = packKey(storeData.storageDescription);
            byte[] end = packKey(storeData.storageDescription, storeData.persistitKey);
            storeData.iterator = txn.getRange(begin, end, true);
        } else {
            byte[] begin = packKey(storeData.storageDescription, storeData.persistitKey);
            byte[] end = join(packKey(storeData.storageDescription), BYTES_FF);
            storeData.iterator = txn.getRange(begin, end, false);
        }
    }

    public void setRollbackPending(Session session) {
        txnService.setRollbackPending(session);
    }

    //
    // Internal
    //

    private void checkUniqueness(Session session, MemoryTransaction txn, Index index, Row row, Key key) {
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

    private void checkKeyDoesNotExistInIndex(Session session, MemoryTransaction txn, Row row, Index index, Key key) {
        assert index.isUnique() : index;
        byte[] begin = packKey(index, key);
        byte[] end = join(begin, BYTES_FF);
        Iterator<Entry<byte[], byte[]>> it = txn.getRange(begin, end);
        if(it.hasNext()) {
            // Using RowData, can give better error than check.throwException().
            String msg = formatIndexRowString(session, row, index);
            throw new DuplicateKeyException(index.getIndexName(), msg);
        }
    }

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

    private MemoryTransaction getTransaction(Session session) {
        return txnService.getTransaction(session);
    }

    //
    // Static
    //

    public static byte[] join(byte[]... arrays) {
        int totalLen = 0;
        for(byte[] a : arrays) {
            totalLen += a.length;
        }
        byte[] joined = new byte[totalLen];
        int curOffset = 0;
        for(byte[] a : arrays) {
            System.arraycopy(a, 0, joined, curOffset, a.length);
            curOffset += a.length;
        }
        return joined;
    }

    public static byte[] packLong(long l) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(l);
        return bb.array();
    }

    public static byte[] packUUID(UUID uuid) {
        return join(packLong(uuid.getMostSignificantBits()), packLong(uuid.getLeastSignificantBits()));
    }

    /** rawKey = join(uuidBytes, persistitKey.getEncodedBytes()) */
    public static void packKey(MemoryStoreData storeData) {
        storeData.rawKey = packKey(storeData.storageDescription, storeData.persistitKey);
    }

    public static byte[] packKey(HasStorage hasStorage) {
        return packKey(hasStorage.getStorageDescription());
    }

    public static byte[] packKey(StorageDescription storageDescription) {
        return ((MemoryStorageDescription)storageDescription).getUUIDBytes();
    }

    public static byte[] packKey(HasStorage hasStorage, Key key) {
        return packKey(hasStorage.getStorageDescription(), key);
    }

    public static byte[] packKey(StorageDescription storageDescription, Key key) {
        byte[] uuidBytes = packKey(storageDescription);
        byte[] keyBytes = Arrays.copyOfRange(key.getEncodedBytes(),
                                             0,
                                             key.getEncodedSize());
        return join(uuidBytes, keyBytes);
    }

    public static UUID unpackUUID(byte[] key) {
        assert key.length >= 16;
        long most = unpackLong(key, 0);
        long least = unpackLong(key, 8);
        return new UUID(most, least);
    }

    /** persistitKey from rawKey */
    public static void unpackKey(MemoryStoreData storeData) {
        unpackKey(storeData.storageDescription, storeData.rawKey, storeData.persistitKey);
    }

    /** key from rawKey */
    public static void unpackKey(HasStorage hasStorage, byte[] rawKey, Key key) {
        unpackKey(hasStorage.getStorageDescription(), rawKey, key);
    }

    /** key from rawKey */
    public static void unpackKey(StorageDescription storageDescription, byte[] rawKey, Key key) {
        assert storageDescription instanceof MemoryStorageDescription : storageDescription;
        // At least as long as UUID bytes
        assert rawKey != null;
        assert rawKey.length > 16 : rawKey.length;

        key.clear();
        System.arraycopy(rawKey,
                         16,
                         key.getEncodedBytes(),
                         0,
                         rawKey.length - 16);
        key.setEncodedSize(rawKey.length - 16);
    }

    public static long unpackLong(byte[] bytes) {
        return unpackLong(bytes, 0);
    }

    public static long unpackLong(byte[] bytes, int offset) {
        assert bytes.length - offset >= 8;
        ByteBuffer bb = ByteBuffer.wrap(bytes, offset, bytes.length - offset);
        return bb.getLong();
    }

    public static void unpackValue(MemoryStoreData storeData) {
        assert storeData.persistitValue != null;
        storeData.persistitValue.clear();
        storeData.persistitValue.putEncodedBytes(storeData.rawValue, 0, storeData.rawValue.length);
    }
}
