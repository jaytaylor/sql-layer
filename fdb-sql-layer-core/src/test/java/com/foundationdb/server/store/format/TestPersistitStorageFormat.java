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
import com.foundationdb.ais.protobuf.TestProtobuf.TestMessage;
import com.foundationdb.sql.parser.StorageFormatNode;

public class TestPersistitStorageFormat extends StorageFormat<TestPersistitStorageDescription>
{
    private final static String identifier = "rowdata";

    private TestPersistitStorageFormat() {
    }

    public static void register(StorageFormatRegistry registry) {
        registry.registerStorageFormat(TestMessage.msg, "test", TestPersistitStorageDescription.class, new TestPersistitStorageFormat());
    }

    public TestPersistitStorageDescription readProtobuf(Storage pbStorage, HasStorage forObject, TestPersistitStorageDescription storageDescription) {
        if (storageDescription == null) {
            storageDescription = new TestPersistitStorageDescription(forObject, identifier);
        }
        TestMessage testMessage = pbStorage.getExtension(TestMessage.msg);
        storageDescription.setName(testMessage.getName());
        if (testMessage.hasOption()) {
            storageDescription.setOption(testMessage.getOption());

        }
        return storageDescription;
    }


    public TestPersistitStorageDescription parseSQL(StorageFormatNode node, HasStorage forObject) {
        TestPersistitStorageDescription storageDescription = new TestPersistitStorageDescription(forObject, identifier);
        storageDescription.setName(node.getOptions().get("name"));
        storageDescription.setOption(node.getOptions().get("option"));
        return storageDescription;
    }

}
