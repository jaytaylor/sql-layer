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

package com.foundationdb.server.store.format.columnkeys;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.server.store.format.StorageFormat;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.sql.parser.StorageFormatNode;

public class ColumnKeysStorageFormat extends StorageFormat<ColumnKeysStorageDescription>
{

    private final static String storageFormat = "column_keys";


    private ColumnKeysStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        registry.registerStorageFormat(FDBProtobuf.columnKeys, storageFormat, ColumnKeysStorageDescription.class, new ColumnKeysStorageFormat());
    }

    public ColumnKeysStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, ColumnKeysStorageDescription storageDescription) {
        if (storageDescription == null) {
            storageDescription = new ColumnKeysStorageDescription(forObject, storageFormat);
        }
        // no options yet
        return storageDescription;
    }

    public ColumnKeysStorageDescription parseSQL(StorageFormatNode node, HasStorage forObject) {
        ColumnKeysStorageDescription storageDescription = new ColumnKeysStorageDescription(forObject, storageFormat);
        // no options yet
        return storageDescription;
    }
}
