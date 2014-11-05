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
import com.foundationdb.ais.protobuf.TestProtobuf;
import com.foundationdb.server.error.StorageDescriptionInvalidException;

public class TestStorageDescriptionExtended extends TestStorageDescription
{
    private String extension;

    public TestStorageDescriptionExtended(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public TestStorageDescriptionExtended(HasStorage forObject, TestStorageDescriptionExtended other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.extension = other.extension;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new TestStorageDescriptionExtended(forObject, this, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        super.writeProtobuf(builder);
        builder.setExtension(TestProtobuf.storageExtension, extension);
        writeUnknownFields(builder);
    }

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }
}
