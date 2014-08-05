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
import com.foundationdb.ais.protobuf.TestProtobuf.TestMessage;
import com.foundationdb.server.error.StorageDescriptionInvalidException;

public class TestPersistitStorageDescription extends PersistitStorageDescription
{
    private String name, option;

    public TestPersistitStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public TestPersistitStorageDescription(HasStorage forObject, TestPersistitStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.name = other.name;
        this.option = other.option;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new TestPersistitStorageDescription(forObject, this, storageFormat);
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getOption() {
        return option;
    }
    public void setOption(String option) {
        this.option = option;
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        super.writeProtobuf(builder);
        TestMessage.Builder mbuilder = TestMessage.newBuilder();
        mbuilder.setName(name);
        if (option != null) {
            mbuilder.setOption(option);
        }
        builder.setExtension(TestMessage.msg, mbuilder.build());
        writeUnknownFields(builder);
    }

    @Override
    public void validate(AISValidationOutput output) {
        super.validate(output);
        if (name == null) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is missing test name")));
        }
    }

}
