/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.store.format.protobuf.MemoryProtobufStorageFormat;

import java.util.UUID;

public class MemoryStorageFormatRegistry extends StorageFormatRegistry
{
    public MemoryStorageFormatRegistry(ConfigurationService configService) {
        super(configService);
    }

    @Override
    public void registerStandardFormats() {
        MemoryStorageFormat.register(this);
        MemoryProtobufStorageFormat.register(this);
        super.registerStandardFormats();
    }

    @Override
    void getDefaultDescriptionConstructor()
    {}

    // Note: Overrides any configured
    @Override
    public StorageDescription getDefaultStorageDescription(HasStorage object) {
        return new MemoryStorageDescription(object, MemoryStorageFormat.FORMAT_NAME);
    }

    public boolean isDescriptionClassAllowed(Class<? extends StorageDescription> descriptionClass) {
        return (super.isDescriptionClassAllowed(descriptionClass) ||
               MemoryStorageDescription.class.isAssignableFrom(descriptionClass));
    }

    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        super.finishStorageDescription(object, nameGenerator);
        assert object.getStorageDescription() != null;
        if(object.getStorageDescription() instanceof MemoryStorageDescription) {
            MemoryStorageDescription storageDescription = (MemoryStorageDescription)object.getStorageDescription();
            if(storageDescription.getUUID() == null) {
                storageDescription.setUUID(UUID.randomUUID());
            }
        }
    }
}
