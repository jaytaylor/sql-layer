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
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.server.store.format.StorageFormat;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.sql.parser.StorageFormatNode;

public class FDBProtobufStorageFormat extends StorageFormat<FDBProtobufStorageDescription>
{
    private static final String identifier = "protobuf";
    private FDBProtobufStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        CustomOptions.registerAllExtensions(registry.getExtensionRegistry());
        registry.registerStorageFormat(CommonProtobuf.protobufRow, identifier, FDBProtobufStorageDescription.class, new FDBProtobufStorageFormat());
    }

    public FDBProtobufStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, FDBProtobufStorageDescription storageDescription) {
        if (storageDescription == null) {
            storageDescription = new FDBProtobufStorageDescription(forObject, identifier);
        }
        storageDescription.readProtobuf(pbStorage.getExtension(CommonProtobuf.protobufRow));
        return storageDescription;
    }

    public FDBProtobufStorageDescription parseSQL(StorageFormatNode node, HasStorage forObject) {
        FDBProtobufStorageDescription storageDescription = new FDBProtobufStorageDescription(forObject,identifier);
        String singleTableOption = node.getOptions().get("no_group");
        boolean singleTable = (singleTableOption != null) && Boolean.valueOf(singleTableOption);
        String noTupleOption = node.getOptions().get("no_tuple");
        boolean noTuple = (noTupleOption != null) && Boolean.valueOf(noTupleOption);
        storageDescription.setFormatType(singleTable ?
                                         CommonProtobuf.ProtobufRowFormat.Type.SINGLE_TABLE :
                                         CommonProtobuf.ProtobufRowFormat.Type.GROUP_MESSAGE);
        storageDescription.setUsage(noTuple ?
                                    null :
                                    FDBProtobuf.TupleUsage.KEY_ONLY);
        return storageDescription;
    }

}
