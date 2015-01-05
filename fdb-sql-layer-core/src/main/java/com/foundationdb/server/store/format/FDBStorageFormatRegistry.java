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
import com.foundationdb.ais.protobuf.FDBProtobuf.TupleUsage;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.store.FDBNameGenerator;
import com.foundationdb.server.store.format.columnkeys.ColumnKeysStorageFormat;
import com.foundationdb.server.store.format.protobuf.FDBProtobufStorageFormat;
import com.foundationdb.server.store.format.tuple.TupleStorageDescription;
import com.foundationdb.server.store.format.tuple.TupleStorageFormat;

public class FDBStorageFormatRegistry extends StorageFormatRegistry
{
    public FDBStorageFormatRegistry(ConfigurationService configService) {
        super(configService);
    }

    @Override
    public void registerStandardFormats() {
        FDBStorageFormat.register(this);
        TupleStorageFormat.register(this);
        FDBProtobufStorageFormat.register(this);
        ColumnKeysStorageFormat.register(this);
        super.registerStandardFormats();
    }

    @Override
    public StorageDescription getDefaultStorageDescription(HasStorage object) {
        StorageDescription sd = super.getDefaultStorageDescription(object);
        if(sd instanceof TupleStorageDescription) {
            TupleStorageDescription tsd = (TupleStorageDescription)sd;
            if(object instanceof Group) {
                tsd.setUsage(TupleUsage.KEY_AND_ROW);
            } else {
                tsd.setUsage(TupleUsage.KEY_ONLY);
            }
        }
        return sd;
    }

    public boolean isDescriptionClassAllowed(Class<? extends StorageDescription> descriptionClass) {
        return (super.isDescriptionClassAllowed(descriptionClass) ||
                FDBStorageDescription.class.isAssignableFrom(descriptionClass));
    }

    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        super.finishStorageDescription(object, nameGenerator);
        assert object.getStorageDescription() != null;
        if (object.getStorageDescription() instanceof FDBStorageDescription) {
            FDBStorageDescription storageDescription = 
                    (FDBStorageDescription)object.getStorageDescription();
            if (storageDescription.getPrefixBytes() == null) {
                storageDescription.setPrefixBytes(generatePrefixBytes(object, (FDBNameGenerator)nameGenerator));
            }
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
