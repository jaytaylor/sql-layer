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
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.server.error.StorageDescriptionInvalidException;

/** An in-memory <code>INFORMATION_SCHEMA</code> table. */
public class MemoryTableStorageDescription extends StorageDescription
{
    MemoryTableFactory memoryTableFactory;

    public MemoryTableStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public MemoryTableStorageDescription(HasStorage forObject, MemoryTableFactory memoryTableFactory, String storageFormat) {
        super(forObject, storageFormat);
        assert memoryTableFactory != null;
        this.memoryTableFactory = memoryTableFactory;
    }

    public MemoryTableStorageDescription(HasStorage forObject, MemoryTableStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.memoryTableFactory = other.memoryTableFactory;
    }

    public MemoryTableFactory getMemoryTableFactory() {
        return memoryTableFactory;
    }

    public void setMemoryTableFactory(MemoryTableFactory memoryTableFactory) {
        this.memoryTableFactory = memoryTableFactory;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new MemoryTableStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(CommonProtobuf.memoryTable, 
                             CommonProtobuf.MemoryTableType.MEMORY_TABLE_FACTORY);
        writeUnknownFields(builder);
    }

    @Override
    public Object getUniqueKey() {
        return memoryTableFactory;
    }

    @Override
    public String getNameString() {
        return null;
    }

    @Override
    public boolean isMemoryTableFactory() {
        return true;
    }

    @Override
    public void validate(AISValidationOutput output) {
        if (memoryTableFactory == null) {
            throw new StorageDescriptionInvalidException(object, "is missing factory");
        }
    }

}
