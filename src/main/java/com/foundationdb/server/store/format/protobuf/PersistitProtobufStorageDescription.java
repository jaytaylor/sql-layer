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

package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf.ProtobufRowFormat;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.store.format.PersistitStorageDescription;

import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.persistit.Exchange;

public class PersistitProtobufStorageDescription extends PersistitStorageDescription
{
    private ProtobufRowFormat format;
    private transient ProtobufRowDataConverter converter;

    public PersistitProtobufStorageDescription(HasStorage forObject) {
        super(forObject);
    }

    public PersistitProtobufStorageDescription(HasStorage forObject, PersistitProtobufStorageDescription other) {
        super(forObject, other);
        this.format = other.format;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new PersistitProtobufStorageDescription(forObject, this);
    }

    public ProtobufRowFormat getFormat() {
        return format;
    }
    public void setFormat(ProtobufRowFormat format) {
        this.format = format;
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        super.writeProtobuf(builder);
        builder.setExtension(CommonProtobuf.protobufRow, format);
        writeUnknownFields(builder);
    }

    @Override
    public void validate(AISValidationOutput output) {
        super.validate(output);
        if (!(object instanceof Group)) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is not a Group and cannot use Protocol Buffers")));
        }
        if (format == ProtobufRowFormat.SINGLE_TABLE) {
            if (!((Group)object).getRoot().getChildJoins().isEmpty()) {
                output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "has more than one table")));
            }
        }
    }

    public synchronized ProtobufRowDataConverter ensureConverter(PersistitStore store) {
        if (converter == null) {
            Group group = (Group)object;
            FileDescriptor fileDescriptor = store.getSchemaManager()
                .getGeneratedFormat(group.getName(), new ProtobufFormatHandler(format));
            converter = ProtobufRowDataConverter.forGroup(group, fileDescriptor);
        }
        return converter;
    }

    @Override
    public void packRowData(PersistitStore store, Exchange exchange, RowData rowData) {
        ensureConverter(store);
        DynamicMessage msg = converter.encode(rowData);
        PersistitProtobufRow holder = new PersistitProtobufRow(converter, msg);
        exchange.getValue().directPut(store.getProtobufValueCoder(), holder, null);
    }

    @Override
    public void expandRowData(PersistitStore store, Exchange exchange, RowData rowData) {
        ensureConverter(store);
        PersistitProtobufRow holder = new PersistitProtobufRow(converter, null);
        exchange.getValue().directGet(store.getProtobufValueCoder(), holder, PersistitProtobufRow.class, null);
        converter.decode(holder.getMessage(), rowData);
    }

}
