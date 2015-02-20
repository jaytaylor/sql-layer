/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.MemoryProtobuf;

import java.util.UUID;

public class MemoryStorageFormat extends StorageFormat<MemoryStorageDescription>
{
    public final static String FORMAT_NAME = "memory";

    private MemoryStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        registry.registerStorageFormat(MemoryProtobuf.uuid,
                                       FORMAT_NAME,
                                       MemoryStorageDescription.class,
                                       new MemoryStorageFormat());
    }

    public MemoryStorageDescription readProtobuf(Storage pbStorage,
                                                 HasStorage forObject,
                                                 MemoryStorageDescription storageDescription) {
        if(storageDescription == null) {
            storageDescription = new MemoryStorageDescription(forObject, FORMAT_NAME);
        }
        String uuidStr = pbStorage.getExtension(MemoryProtobuf.uuid);
        if(uuidStr != null) {
            storageDescription.setUUID(UUID.fromString(uuidStr));
        }
        return storageDescription;
    }
}
