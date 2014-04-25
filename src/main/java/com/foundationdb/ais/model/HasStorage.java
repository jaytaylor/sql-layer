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

public abstract class HasStorage
{
    protected StorageDescription storageDescription;

    public StorageDescription getStorageDescription() {
        return storageDescription;
    }

    public void setStorageDescription(StorageDescription storageDescription) {
        this.storageDescription = storageDescription;
    }

    public void copyStorageDescription(HasStorage fromObject) {
        this.storageDescription = 
            (fromObject.storageDescription != null) ?
            fromObject.storageDescription.cloneForObject(this) :
            null;
    }
    
    public abstract AkibanInformationSchema getAIS();

    public abstract String getTypeString();

    public abstract String getNameString();

    public abstract String getSchemaName();

    public Object getStorageUniqueKey() {
        return (storageDescription != null) ? storageDescription.getUniqueKey() : null;
    }

    public String getStorageNameString() {
        return (storageDescription != null) ? storageDescription.getNameString() : null;
    }

    @Override
    public String toString()
    {
        return getTypeString() + "(" + getNameString() + ")";
    }
}
