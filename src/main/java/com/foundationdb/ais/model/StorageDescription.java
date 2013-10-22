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

package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;

public abstract class StorageDescription
{
    protected final HasStorage object;

    protected StorageDescription(HasStorage object) {
        this.object = object;
    }
    
    public HasStorage getObject() {
        return object;
    }

    public String getSchemaName() {
        return object.getSchemaName();
    }

    public abstract StorageDescription cloneForObject(HasStorage forObject);

    public abstract void writeProtobuf(Storage.Builder builder);

    public abstract Object getUniqueKey();

    public abstract String getNameString();

    public abstract void validate(AISValidationOutput output);

    public boolean isMemoryTableFactory() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("%s for %s", getNameString(), object);
    }

}
