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

package com.foundationdb.server.store.format;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.FDBScanTransactionOptions;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.FDBStoreDataHelper;
import com.foundationdb.server.store.FDBStoreDataKeyValueIterator;
import com.foundationdb.server.store.FDBStoreDataSingleKeyValueIterator;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.store.StoreStorageDescription;
import com.foundationdb.KeySelector;
import com.foundationdb.Transaction;
import com.foundationdb.async.Future;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;
import com.google.protobuf.ByteString;
import com.persistit.Key;

import java.util.Arrays;

import static com.foundationdb.server.store.FDBStoreDataHelper.*;

/** Storage using the FDB directory layer.
 * As a result, there is no possibility of duplicate names and no need
 * of name generation.
*/
public class FDBStorageDescription extends StoreStorageDescription<FDBStore,FDBStoreData>
{
    private byte[] prefixBytes;

    public FDBStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public FDBStorageDescription(HasStorage forObject, byte[] prefixBytes, String storageFormat) {
        super(forObject, storageFormat);
        this.prefixBytes = prefixBytes;
    }

    public FDBStorageDescription(HasStorage forObject, FDBStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.prefixBytes = other.prefixBytes;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new FDBStorageDescription(forObject, this, storageFormat);
    }
    
    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        return new FDBStorageDescription(forObject, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        if(prefixBytes != null) {
            builder.setExtension(FDBProtobuf.prefixBytes, ByteString.copyFrom(prefixBytes));
        }
        writeUnknownFields(builder);
    }

    public byte[] getPrefixBytes() {
        return prefixBytes;
    }

    protected void setPrefixBytes(byte[] prefixBytes) {
        this.prefixBytes = prefixBytes;
    }

    /** Compare prefix byte-by-byte for uniqueness. */
    final class UniqueKey {
        byte[] getPrefixBytes() {
            return FDBStorageDescription.this.prefixBytes;
        }

        @Override
        public boolean equals(Object other) {
            return (other instanceof UniqueKey) && 
                Arrays.equals(prefixBytes, ((UniqueKey)other).getPrefixBytes());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(prefixBytes);
        }

        @Override
        public String toString() {
            return getNameString();
        }
    }

    @Override
    public Object getUniqueKey() {
        if (prefixBytes == null)
            return null;
        return new UniqueKey();
    }

    @Override
    public String getNameString() {
        return (prefixBytes != null) ? ByteArrayUtil.printable(prefixBytes) : null;
    }

    @Override
    public void validate(AISValidationOutput output) {
        if (prefixBytes == null) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is missing prefix bytes")));
        }
    }
    
    @Override
    public Row expandRow (FDBStore store, Session session, FDBStoreData storeData, Schema schema) {
        return FDBStoreDataHelper.expandRow(schema, storeData);
    }
    
    @Override
    public void packRow (FDBStore store, Session session, 
                            FDBStoreData storeData, Row row) {
        FDBStoreDataHelper.packRow(row, storeData);
    }

    /** Convert Persistit <code>Key</code> into raw key. */
    public byte[] getKeyBytes(Key key) {
        return getKeyBytes(key, null);
    }

    public byte[] getKeyBytes(Key key, FDBStoreData.NudgeDir nudged) {
        byte[] keyBytes = Arrays.copyOf(key.getEncodedBytes(), key.getEncodedSize());
        return Tuple2.from(keyBytes).pack();        
    }
    
    /** Converted decoded key <code>Tuple</code> into Persistit <code>Key</code>. */
    public void getTupleKey(Tuple2 t, Key key) {
        assert (t.size() == 1) : t;
        byte[] keyBytes = t.getBytes(0);
        key.clear();
        if(key.getMaximumSize() < keyBytes.length) {
            key.setMaximumSize(keyBytes.length);
        }
        System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
        key.setEncodedSize(keyBytes.length);
    }

    /** Store contents of <code>storeData</code> into database.  
     * Usually, key comes from <code>storeData.rawKey</code> via {@link getKeyBytes}
     * and value comes from <code>storeData.rawValue</code> via {@link #packRowData}.
     */
    public void store(FDBStore store, Session session, FDBStoreData storeData) {
        if (storeData.persistitValue != null) {
            storeData.rawValue = Arrays.copyOf(storeData.persistitValue.getEncodedBytes(),
                                               storeData.persistitValue.getEncodedSize());
        }
        store.getTransaction(session, storeData)
            .setBytes(storeData.rawKey, storeData.rawValue);
    }

    /** Fetch contents of database into <code>storeData</code>.
     * Usually, key comes from <code>storeData.rawKey</code> via {@link getKeyBytes}
     * and value goes into <code>storeData.rawValue</code> for {@link #expandRowData}.
     */
    public boolean fetch(FDBStore store, Session session, FDBStoreData storeData) {
        storeData.rawValue = store.getTransaction(session, storeData).getValue(storeData.rawKey);
        return (storeData.rawValue != null);
    }

    /** Clear contents of database based on <code>storeData</code>.
     * Usually, key comes from <code>storeData.rawKey</code> via {@link getKeyBytes}.
     */
    public void clear(FDBStore store, Session session, FDBStoreData storeData) {
        TransactionState txn = store.getTransaction(session, storeData);
        txn.clearKey(storeData.rawKey);
    }

    /** Set up <code>storeData.iterator</code> to iterate over group within the given
     * boundaries.
     */
    public void groupIterator(FDBStore store, Session session, FDBStoreData storeData,
                              FDBStore.GroupIteratorBoundary left, FDBStore.GroupIteratorBoundary right,
                              int limit, FDBScanTransactionOptions transactionOptions) {
        TransactionState txnState = store.getTransaction(session, storeData);
        if ((left == FDBStore.GroupIteratorBoundary.KEY) &&
            (right == FDBStore.GroupIteratorBoundary.NEXT_KEY) &&
            (limit == 1)) {
            byte[] key = packKey(storeData);
            Future<byte[]> future = txnState.getFuture(key, transactionOptions);
            storeData.iterator = new FDBStoreDataSingleKeyValueIterator(storeData, key,
                                                                        future);
            return;
        }
        KeySelector ksLeft, ksRight;
        switch (left) {
        case START:
            ksLeft = KeySelector.firstGreaterOrEqual(prefixBytes(storeData));
            break;
        case KEY:
            ksLeft = KeySelector.firstGreaterOrEqual(packKey(storeData));
            break;
        case NEXT_KEY:
            ksLeft = KeySelector.firstGreaterThan(packKey(storeData));
            break;
        case FIRST_DESCENDANT:
            ksLeft = KeySelector.firstGreaterOrEqual(packKey(storeData, Key.BEFORE));
            break;
        default:
            throw new IllegalArgumentException(left.toString());
        }
        switch (right) {
        case END:
            ksRight = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefixBytes(storeData)));
            break;
        case LAST_DESCENDANT:
            ksRight = KeySelector.firstGreaterOrEqual(packKey(storeData, Key.AFTER));
            break;
        default:
            throw new IllegalArgumentException(right.toString());
        }
        storeData.iterator = new FDBStoreDataKeyValueIterator(storeData,
            txnState.getRangeIterator(ksLeft, ksRight, limit, false, transactionOptions));
    }

    /** Set up <code>storeData.iterator</code> to iterate over index.
     * @param key Start at <code>storeData.persistitKey</code>
     * @param inclusive Include key itself in result.
     * @param reverse Iterate in reverse.
     * @param snapshot Snapshot range scan
     */
    public void indexIterator(FDBStore store, Session session, FDBStoreData storeData,
                              boolean key, boolean inclusive, boolean reverse,
                              FDBScanTransactionOptions transactionOptions) {
        KeySelector ksLeft, ksRight;
        byte[] prefixBytes = prefixBytes(storeData);
        if (!key) {
            ksLeft = KeySelector.firstGreaterOrEqual(prefixBytes);
            ksRight = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefixBytes));
        }
        else if (inclusive) {
            if (reverse) {
                ksLeft = KeySelector.firstGreaterThan(prefixBytes);
                ksRight = KeySelector.firstGreaterThan(packKey(storeData));
            } 
            else {
                ksLeft = KeySelector.firstGreaterOrEqual(packKey(storeData));
                ksRight = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefixBytes));
            }
        }
        else {
            if (reverse) {
                ksLeft = KeySelector.firstGreaterThan(prefixBytes);
                ksRight = KeySelector.firstGreaterOrEqual(packKey(storeData));
            } 
            else {
                ksLeft = KeySelector.firstGreaterThan(packKey(storeData));
                ksRight = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefixBytes));
            }
        }
        TransactionState txnState = store.getTransaction(session, storeData);
        storeData.iterator = new FDBStoreDataKeyValueIterator(storeData,
            txnState.getRangeIterator(ksLeft, ksRight, Transaction.ROW_LIMIT_UNLIMITED, reverse, transactionOptions));
    }
}
