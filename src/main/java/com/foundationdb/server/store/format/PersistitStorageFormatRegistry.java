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

import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.PersistitProtobuf;
import com.foundationdb.server.store.PersistitNameGenerator;

public class PersistitStorageFormatRegistry extends StorageFormatRegistry
{
    public PersistitStorageFormatRegistry() {
        super();
        PersistitProtobuf.registerAllExtensions(extensionRegistry);
    }

    @Override
    public StorageDescription readProtobuf(Storage pbStorage, HasStorage forObject) {
        StorageDescription common = super.readProtobuf(pbStorage, forObject);
        if (common != null) {
            return common;
        }
        else if (pbStorage.hasExtension(PersistitProtobuf.treeName)) {
            return new PersistitStorageDescription(forObject, pbStorage.getExtension(PersistitProtobuf.treeName));
        }
        else {
            return null;        // TODO: Or error?
        }
    }
    
    public StorageDescription convertTreeName(String treeName, HasStorage forObject) {
        return new PersistitStorageDescription(forObject, treeName);
    }

    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        super.finishStorageDescription(object, nameGenerator);
        // TODO: Once there are multiple formats, this will need to handle the
        // case of a (subclass of) PersistitStorageDescription but without its
        // treeName filled in yet.
        if (object.getStorageDescription() == null) {
            object.setStorageDescription(new PersistitStorageDescription(object, generateTreeName(object, nameGenerator)));
        }
    }

    protected String generateTreeName(HasStorage object, NameGenerator nameGenerator) {
        if (object instanceof Index) {
            return nameGenerator.generateIndexTreeName((Index)object);
        }
        else if (object instanceof Group) {
            TableName name = ((Group)object).getName();
            return nameGenerator.generateGroupTreeName(name.getSchemaName(), name.getTableName());
        }
        else if (object instanceof Sequence) {
            return nameGenerator.generateSequenceTreeName((Sequence)object);
        }
        else {
            throw new IllegalArgumentException(object.toString());
        }
    }

}
