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

package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.server.store.format.StorageFormat;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.sql.parser.StorageFormatNode;

public class PersistitProtobufStorageFormat extends StorageFormat<PersistitProtobufStorageDescription>
{
    private final static String identifier = "protobuf";

    private PersistitProtobufStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        CustomOptions.registerAllExtensions(registry.getExtensionRegistry());
        registry.registerStorageFormat(CommonProtobuf.protobufRow, identifier, PersistitProtobufStorageDescription.class, new PersistitProtobufStorageFormat());
    }

    public PersistitProtobufStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, PersistitProtobufStorageDescription storageDescription) {
        if (storageDescription == null) {
            storageDescription = new PersistitProtobufStorageDescription(forObject, identifier);
        }
        storageDescription.readProtobuf(pbStorage.getExtension(CommonProtobuf.protobufRow));
        return storageDescription;
    }


    public PersistitProtobufStorageDescription parseSQL(StorageFormatNode node, HasStorage forObject) {
        PersistitProtobufStorageDescription storageDescription = new PersistitProtobufStorageDescription(forObject, identifier);
        String singleTableOption = node.getOptions().get("no_group");
        boolean singleTable = (singleTableOption != null) && Boolean.valueOf(singleTableOption);
        storageDescription.setFormatType(singleTable ?
                                         CommonProtobuf.ProtobufRowFormat.Type.SINGLE_TABLE :
                                         CommonProtobuf.ProtobufRowFormat.Type.GROUP_MESSAGE);
        return storageDescription;
    }

}
