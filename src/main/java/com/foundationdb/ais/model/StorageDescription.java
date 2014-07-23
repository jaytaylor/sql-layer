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
import com.google.protobuf.UnknownFieldSet;

public abstract class StorageDescription
{
    protected final HasStorage object;
    protected UnknownFieldSet unknownFields;
    protected final String storageFormat;

    protected StorageDescription(HasStorage object, String storageFormat) {
        //assert(storageFormat != null):
        this.storageFormat = storageFormat;
        this.object = object;
    }
    
    protected StorageDescription(HasStorage object, StorageDescription other, String storageFormat) {
        this.object = object;
        this.storageFormat = storageFormat;
    }
    
    /** Get the AIS object for which this describes the storage. */
    public HasStorage getObject() {
        return object;
    }

    public String getSchemaName() {
        return object.getSchemaName();
    }

    /** Make a copy of this format (same tree name, for instance), but
     * pointing to a new object.
     */
    public abstract StorageDescription cloneForObject(HasStorage forObject);

    /** Populate the extension fields of the <code>Storage</code>
     * field. */
    public abstract void writeProtobuf(Storage.Builder builder);

    public String getStorageFormat(){
        return storageFormat;
    }

    /** If there is a unique identifier for the storage "area"
     * described by this, return it, else <code>null</code>.
     */
    public abstract Object getUniqueKey();

    /** Get a string for printing the "location" of the storage
     * area. */
    public abstract String getNameString();

    /** Check that the <code>StorageDescription</code> has been filled
     * in completely and consistently before the AIS is frozen and
     * committed. */
    public abstract void validate(AISValidationOutput output);

    /** Does this describe something that lives in memory rather than
     * persistently? */
    public boolean isMemoryTableFactory() {
        return false;
    }

    /** Does this description include unknown fields?
     * Such a <code>HasStorage</code> will save in the AIS but cannot be used.
     */
    public boolean hasUnknownFields() {
        return (unknownFields != null);
    }

    public void setUnknownFields(UnknownFieldSet unknownFields) {
        this.unknownFields = unknownFields;
    }

    public void writeUnknownFields(Storage.Builder builder) {
        if (unknownFields != null) {
            builder.mergeUnknownFields(unknownFields);
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getNameString()).append(" for ").append(object);
        if (unknownFields != null) {
            str.append(" with unknown fields ").append(unknownFields.asMap().keySet());
        }
        return str.toString();
    }

}
