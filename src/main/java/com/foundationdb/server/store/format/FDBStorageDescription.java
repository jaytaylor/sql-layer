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
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.StoreStorageDescription;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;

import com.google.protobuf.ByteString;
import com.persistit.Key;
import java.util.Arrays;

/** Storage using the FDB directory layer.
 * As a result, there is no possibility of duplicate names and no need
 * of name generation.
*/
public class FDBStorageDescription extends StoreStorageDescription<FDBStore,FDBStoreData>
{
    private byte[] prefixBytes;

    public FDBStorageDescription(HasStorage forObject) {
        super(forObject);
    }

    public FDBStorageDescription(HasStorage forObject, byte[] prefixBytes) {
        super(forObject);
        this.prefixBytes = prefixBytes;
    }

    public FDBStorageDescription(HasStorage forObject, FDBStorageDescription other) {
        super(forObject, other);
        this.prefixBytes = other.prefixBytes;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new FDBStorageDescription(forObject, this);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(FDBProtobuf.prefixBytes, ByteString.copyFrom(prefixBytes));
        writeUnknownFields(builder);
    }

    public byte[] getPrefixBytes() {
        return prefixBytes;
    }

    protected void setPrefixBytes(byte[] prefixBytes) {
        this.prefixBytes = prefixBytes;
    }

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
    public void expandRowData(FDBStore store, FDBStoreData storeData, RowData rowData) {
        FDBStore.expandRowData(rowData, storeData.value, true);
    }

    @Override
    public void packRowData(FDBStore store, FDBStoreData storeData, RowData rowData) {
        storeData.value = Arrays.copyOfRange(rowData.getBytes(), rowData.getRowStart(), rowData.getRowEnd());
    }

    public byte[] getKeyBytes(Key key) {
        byte[] keyBytes = Arrays.copyOf(key.getEncodedBytes(), key.getEncodedSize());
        return Tuple.from(keyBytes).pack();
    }

    public void getTupleKey(Tuple t, Key key) {
        assert (t.size() == 1) : t;
        byte[] keyBytes = t.getBytes(0);
        key.clear();
        if(key.getMaximumSize() < keyBytes.length) {
            key.setMaximumSize(keyBytes.length);
        }
        System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
        key.setEncodedSize(keyBytes.length);
    }

}
