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

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf.ProtobufRowFormat;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.store.format.PersistitStorageDescription;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DynamicMessage;
import com.persistit.Exchange;

import static com.foundationdb.server.store.format.protobuf.ProtobufStorageDescriptionHelper.*;

public class PersistitProtobufStorageDescription extends PersistitStorageDescription implements ProtobufStorageDescription
{
    private ProtobufRowFormat.Type formatType;
    private FileDescriptorProto fileProto;
    private transient ProtobufRowDataConverter rowDataConverter;
    private transient ProtobufRowConverter rowConverter;

    public PersistitProtobufStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public PersistitProtobufStorageDescription(HasStorage forObject, PersistitProtobufStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.formatType = other.formatType;
        this.fileProto = other.fileProto;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new PersistitProtobufStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public FileDescriptorProto getFileProto() {
        return fileProto;
    }
    
    @Override
    public ProtobufRowFormat.Type getFormatType() {
        return formatType;
    }
    public void setFormatType(ProtobufRowFormat.Type formatType) {
        this.formatType = formatType;
    }

    public void readProtobuf(ProtobufRowFormat pbFormat) {
        formatType = pbFormat.getType();
        fileProto = pbFormat.getFileDescriptor();
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        super.writeProtobuf(builder);
        ProtobufStorageDescriptionHelper.writeProtobuf(builder, formatType, fileProto);
        writeUnknownFields(builder);
    }

    @Override
    public void validate(AISValidationOutput output) {
        super.validate(output);
        fileProto = validateAndGenerate(object, formatType, fileProto, output);
    }

    public synchronized ProtobufRowDataConverter ensureRowDataConverter() {
        if (rowDataConverter == null) {
            rowDataConverter = buildRowDataConverter(object, fileProto);
        }
        return rowDataConverter;
    }
    
    public synchronized ProtobufRowConverter ensureRowConverter() {
        if (rowConverter == null) {
            rowConverter = buildRowConverter(object, fileProto);
        }
        return rowConverter;
    }
    
    @Override
    public void packRow (PersistitStore store, Session session, 
                        Exchange exchange, Row row) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void packRowData(PersistitStore store, Session session,
                            Exchange exchange, RowData rowData) {
        ensureRowDataConverter();
        DynamicMessage msg = rowDataConverter.encode(rowData);
        PersistitProtobufRow holder = new PersistitProtobufRow(rowDataConverter, msg);
        exchange.getValue().directPut(store.getProtobufValueCoder(), holder, null);
    }

    @Override 
    public void expandRow (PersistitStore store, Session session, 
                            Exchange exchange, Row row) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void expandRowData(PersistitStore store, Session session,
                              Exchange exchange, RowData rowData) {
        ensureRowDataConverter();
        PersistitProtobufRow holder = new PersistitProtobufRow(rowDataConverter, null);
        exchange.getValue().directGet(store.getProtobufValueCoder(), holder, PersistitProtobufRow.class, null);
        rowDataConverter.decode(holder.getMessage(), rowData);
    }

}
