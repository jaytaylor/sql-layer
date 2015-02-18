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

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.qp.virtual.VirtualScanFactory;

import java.util.Map;

public class VirtualTableStorageFormat extends StorageFormat<VirtualTableStorageDescription>
{
    private final Map<TableName,VirtualScanFactory> virtualScanFactories;

    final static String identifier = "virtual";

    private VirtualTableStorageFormat(Map<TableName, VirtualScanFactory> virtualScanFactories) {
        this.virtualScanFactories = virtualScanFactories;
    }

    public static void register(StorageFormatRegistry registry, Map<TableName,VirtualScanFactory> virtualScanFactories) {
        registry.registerStorageFormat(CommonProtobuf.virtualTable, null, VirtualTableStorageDescription.class, new VirtualTableStorageFormat(virtualScanFactories));
    }

    public VirtualTableStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, VirtualTableStorageDescription storageDescription) {
        switch (pbStorage.getExtension(CommonProtobuf.virtualTable)) {
        case VIRTUAL_SCAN_FACTORY:
            if (storageDescription == null) {
                storageDescription = new VirtualTableStorageDescription(forObject, identifier);
            }
            storageDescription.setVirtualScanFactory(virtualScanFactories.get(((Group)forObject).getName()));
            break;
        }
        return storageDescription;
    }
}
