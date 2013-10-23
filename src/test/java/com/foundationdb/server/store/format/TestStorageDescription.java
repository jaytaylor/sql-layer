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

public class TestStorageDescription extends StorageDescription
{
    private String value;

    public TestStorageDescription(HasStorage forObject) {
        super(forObject);
    }

    public TestStorageDescription(HasStorage forObject, String value) {
        super(forObject);
        this.value = value;
    }

    public TestStorageDescription(HasStorage forObject, TestStorageDescription other) {
        super(forObject);
        this.value = other.value;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new TestStorageDescription(forObject, this);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(TestProtobuf.test, value);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public Object getUniqueKey() {
        return value;
    }

    @Override
    public String getNameString() {
        return value;
    }

    @Override
    public void validate(AISValidationOutput output) {
        if (value == null) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is missing test value")));
        }
    }

}
