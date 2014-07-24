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
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;

import java.util.Map;

public class MemoryTableStorageFormat extends StorageFormat<MemoryTableStorageDescription>
{
    private final Map<TableName,MemoryTableFactory> memoryTableFactories;

    private final static String identifier = "rowdata";

    private MemoryTableStorageFormat(Map<TableName,MemoryTableFactory> memoryTableFactories) {
        this.memoryTableFactories = memoryTableFactories;
    }

    public static void register(StorageFormatRegistry registry, Map<TableName,MemoryTableFactory> memoryTableFactories) {
        registry.registerStorageFormat(CommonProtobuf.memoryTable, null, MemoryTableStorageDescription.class, new MemoryTableStorageFormat(memoryTableFactories));
    }

    public MemoryTableStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, MemoryTableStorageDescription storageDescription) {
        switch (pbStorage.getExtension(CommonProtobuf.memoryTable)) {
        case MEMORY_TABLE_FACTORY:
            if (storageDescription == null) {
                storageDescription = new MemoryTableStorageDescription(forObject, identifier);
            }
            storageDescription.setMemoryTableFactory(memoryTableFactories.get(((Group)forObject).getName()));
            break;
        }
        return storageDescription;
    }
}
