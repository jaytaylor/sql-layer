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

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AISBuilder.StandinStorageDescription;
import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;

public class DummyStorageFormatRegistry extends StorageFormatRegistry
{
    private DummyStorageFormatRegistry() {
        super();
    }

    public static StorageFormatRegistry create() {
        StorageFormatRegistry dummy = new DummyStorageFormatRegistry();
        return dummy;
    }

    /** Convenience to make an AISCloner using the dummy. */
    public static AISCloner aisCloner() {
        return new AISCloner(create());
    }

    @Override
    public StorageDescription readProtobuf(Storage pbStorage, HasStorage forObject) {
        StorageDescription common = super.readProtobuf(pbStorage, forObject);
        if (common != null) {
            return common;
        }
        else if (pbStorage.hasExtension(CommonProtobuf.test)) {
            return new TestStorageDescription(forObject, pbStorage.getExtension(CommonProtobuf.test));
        }
        else {
            // These don't serialize, but we still want to pass
            // validation until rendezvoused with real storage.
            return new StandinStorageDescription(forObject);
        }
    }
    
    public StorageDescription convertTreeName(String treeName, HasStorage forObject) {
        throw new UnsupportedOperationException();
    }

    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        super.finishStorageDescription(object, nameGenerator);
        if (object.getStorageDescription() == null) {
            object.setStorageDescription(new TestStorageDescription(object, generateTreeName(object, nameGenerator)));
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
