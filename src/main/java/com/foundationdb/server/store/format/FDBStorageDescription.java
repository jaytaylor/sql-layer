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
import com.foundationdb.tuple.ByteArrayUtil;

import com.google.protobuf.ByteString;
import java.util.Arrays;

public class FDBStorageDescription extends StorageDescription
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
        super(forObject);
        this.prefixBytes = other.prefixBytes;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new FDBStorageDescription(forObject, this);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(FDBProtobuf.prefixBytes, ByteString.copyFrom(prefixBytes));
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

}
