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
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.server.store.FDBNameGenerator;

import org.apache.commons.codec.binary.Base64;

public class FDBStorageFormatRegistry extends StorageFormatRegistry
{
    public FDBStorageFormatRegistry() {
        super();
        FDBProtobuf.registerAllExtensions(extensionRegistry);
    }

    @Override
    public StorageDescription readProtobuf(Storage pbStorage, HasStorage forObject) {
        StorageDescription common = super.readProtobuf(pbStorage, forObject);
        if (common != null) {
            return common;
        }
        else if (pbStorage.hasExtension(FDBProtobuf.prefixBytes)) {
            return new FDBStorageDescription(forObject, pbStorage.getExtension(FDBProtobuf.prefixBytes).toByteArray());
        }
        else {
            return null;        // TODO: Or error?
        }
    }
    
    // The old tree name field as the prefix bytes Base64 encoded.
    public StorageDescription convertTreeName(String treeName, HasStorage forObject) {
        return new FDBStorageDescription(forObject, Base64.decodeBase64(treeName));
    }

    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        if (object.getStorageDescription() == null) {
            object.setStorageDescription(new FDBStorageDescription(object, generatePrefixBytes(object, (FDBNameGenerator)nameGenerator)));
        }
    }

    protected byte[] generatePrefixBytes(HasStorage object, FDBNameGenerator nameGenerator) {
        if (object instanceof Index) {
            return nameGenerator.generateIndexPrefixBytes((Index)object);
        }
        else if (object instanceof Group) {
            TableName name = ((Group)object).getName();
            return nameGenerator.generateGroupPrefixBytes(name.getSchemaName(), name.getTableName());
        }
        else if (object instanceof Sequence) {
            return nameGenerator.generateSequencePrefixBytes((Sequence)object);
        }
        else {
            throw new IllegalArgumentException(object.toString());
        }
    }

}
