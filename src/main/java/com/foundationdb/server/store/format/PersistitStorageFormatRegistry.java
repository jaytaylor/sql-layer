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
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.store.PersistitNameGenerator;
import com.foundationdb.server.store.format.protobuf.PersistitProtobufStorageFormat;

public class PersistitStorageFormatRegistry extends StorageFormatRegistry
{

    public PersistitStorageFormatRegistry(ConfigurationService configService) {
        super(configService, PersistitStorageFormat.identifier);
    }

    @Override
    public void registerStandardFormats() {
        PersistitStorageFormat.register(this);
        PersistitProtobufStorageFormat.register(this);
        super.registerStandardFormats();
    }

    @Override
    void getDefaultDescriptionConstructor() {}

    @Override
    public StorageDescription getDefaultStorageDescription(HasStorage object) {
        return new PersistitStorageDescription(object, PersistitStorageFormat.identifier);
    }

    public boolean isDescriptionClassAllowed(Class<? extends StorageDescription> descriptionClass) {
        return (super.isDescriptionClassAllowed(descriptionClass) ||
                PersistitStorageDescription.class.isAssignableFrom(descriptionClass));
    }

    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        super.finishStorageDescription(object, nameGenerator);
        assert object.getStorageDescription() != null;
        if (object.getStorageDescription() instanceof PersistitStorageDescription) {
            PersistitStorageDescription storageDescription = 
                    (PersistitStorageDescription)object.getStorageDescription();
            if (storageDescription.getTreeName() == null) {
                storageDescription.setTreeName(generateTreeName(object, (PersistitNameGenerator)nameGenerator));
            }
        }
    }

    protected String generateTreeName(HasStorage object, PersistitNameGenerator nameGenerator) {
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
