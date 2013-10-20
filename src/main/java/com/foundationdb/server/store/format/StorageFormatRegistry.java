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
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;

import com.google.protobuf.ExtensionRegistry;
import java.util.HashMap;
import java.util.Map;
import java.io.File;

public abstract class StorageFormatRegistry
{
    private final Map<TableName,MemoryTableFactory> memoryTableFactories = new HashMap<>();
    protected final ExtensionRegistry extensionRegistry;

    protected StorageFormatRegistry() {
        extensionRegistry = ExtensionRegistry.newInstance();
        CommonProtobuf.registerAllExtensions(extensionRegistry);
    }

    public ExtensionRegistry getExtensionRegistry() {
        return extensionRegistry;
    }

    public StorageDescription readProtobuf(Storage pbStorage, HasStorage forObject) {
        if (pbStorage.hasExtension(CommonProtobuf.memoryTableFactory)) {
            return new MemoryTableStorageDescription(forObject, memoryTableFactories.get(((Group)forObject).getName()));
        }
        else if (pbStorage.hasExtension(CommonProtobuf.fullTextIndexPath)) {
            return new FullTextIndexFileStorageDescription(forObject, new File(pbStorage.getExtension(CommonProtobuf.fullTextIndexPath)));
        }
        else {
            return null;
        }
    }

    public StorageDescription registerMemoryFactory(MemoryTableFactory memoryFactory, Group group) {
        // TODO: Is there ever a case where the group has already been
        // loaded without knowing it and needs to be updated in place?
        memoryTableFactories.put(group.getName(), memoryFactory);
        return new MemoryTableStorageDescription(group, memoryFactory);
    }

    public void unregisterMemoryFactory(TableName name) {
        memoryTableFactories.remove(name);
    }

    public abstract StorageDescription convertTreeName(String treeName, HasStorage forObject);

    public abstract void finishStorageDescription(HasStorage object, NameGenerator nameGenerator);
}
