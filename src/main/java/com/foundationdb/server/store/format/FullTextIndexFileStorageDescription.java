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

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import java.io.File;

/** A full text index saved in the local file system. */
public class FullTextIndexFileStorageDescription extends StorageDescription
{
    File path;

    public FullTextIndexFileStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public FullTextIndexFileStorageDescription(HasStorage forObject, File path, String storageFormat) {
        super(forObject, storageFormat);
        this.path = path;
    }

    public FullTextIndexFileStorageDescription(HasStorage forObject, FullTextIndexFileStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.path = other.path;
    }

    public File getPath() {
        return path;
    }

    public File mergePath(File basepath) {
        if (path == null)
            return null;
        else if (path.isAbsolute())
            return path;
        else
            return new File(basepath, path.getPath());
    }

    public void setPath(File path) {
        this.path = path;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new FullTextIndexFileStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(CommonProtobuf.fullTextIndexPath, path.getPath());
        writeUnknownFields(builder);
    }

    @Override
    public Object getUniqueKey() {
        return path;
    }

    @Override
    public String getNameString() {
        return (path != null) ? path.getPath() : null;
    }

    @Override
    public void validate(AISValidationOutput output) {
        if (path == null) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is missing path")));
        }
    }

}
