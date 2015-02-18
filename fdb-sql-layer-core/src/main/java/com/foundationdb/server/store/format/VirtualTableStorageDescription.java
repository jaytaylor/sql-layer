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
import com.foundationdb.qp.virtual.VirtualScanFactory;
import com.foundationdb.server.error.StorageDescriptionInvalidException;

/** An in-memory system table. */
public class VirtualTableStorageDescription extends StorageDescription
{
    VirtualScanFactory virtualScanFactory;

    public VirtualTableStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public VirtualTableStorageDescription(HasStorage forObject,
                                          VirtualScanFactory virtualScanFactory,
                                          String storageFormat) {
        super(forObject, storageFormat);
        assert virtualScanFactory != null;
        this.virtualScanFactory = virtualScanFactory;
    }

    public VirtualTableStorageDescription(HasStorage forObject,
                                          VirtualTableStorageDescription other,
                                          String storageFormat) {
        super(forObject, other, storageFormat);
        this.virtualScanFactory = other.virtualScanFactory;
    }

    public VirtualScanFactory getVirtualScanFactory() {
        return virtualScanFactory;
    }

    public void setVirtualScanFactory(VirtualScanFactory virtualScanFactory) {
        this.virtualScanFactory = virtualScanFactory;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new VirtualTableStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        return new VirtualTableStorageDescription(forObject, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(CommonProtobuf.virtualTable,
                             CommonProtobuf.VirtualTableType.VIRTUAL_SCAN_FACTORY);
        writeUnknownFields(builder);
    }

    @Override
    public Object getUniqueKey() {
        return virtualScanFactory;
    }

    @Override
    public String getNameString() {
        return null;
    }

    @Override
    public boolean isVirtual() {
        return true;
    }

    @Override
    public void validate(AISValidationOutput output) {
        if (virtualScanFactory == null) {
            throw new StorageDescriptionInvalidException(object, "is missing factory");
        }
    }

}
