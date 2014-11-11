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
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.PersistitProtobuf;

import java.io.File;

public class PersistitStorageFormat extends StorageFormat<PersistitStorageDescription>
{
    public final static String identifier = "rowdata";

    private PersistitStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        registry.registerStorageFormat(PersistitProtobuf.treeName, identifier, PersistitStorageDescription.class, new PersistitStorageFormat());
    }

    public PersistitStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, PersistitStorageDescription storageDescription) {
        if (storageDescription == null) {
            storageDescription = new PersistitStorageDescription(forObject, identifier);
        }
        storageDescription.setTreeName(pbStorage.getExtension(PersistitProtobuf.treeName));
        return storageDescription;
    }
}
