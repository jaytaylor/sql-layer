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
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.ProtobufReadException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.format.tuple.TupleStorageDescription;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import static com.foundationdb.server.store.format.protobuf.ProtobufStorageDescriptionHelper.*;

public class FDBProtobufStorageDescription extends TupleStorageDescription implements ProtobufStorageDescription
{
    private ProtobufRowFormat.Type formatType;
    private FileDescriptorProto fileProto;
    private transient ProtobufRowDataConverter rowDataConverter;
    private transient ProtobufRowConverter rowConverter;

    public FDBProtobufStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public FDBProtobufStorageDescription(HasStorage forObject, FDBProtobufStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.formatType = other.formatType;
        this.fileProto = other.fileProto;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new FDBProtobufStorageDescription(forObject, this, storageFormat);
    }
    
    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        FDBProtobufStorageDescription sd = new FDBProtobufStorageDescription(forObject, storageFormat);
        sd.setUsage(this.getUsage());
        sd.setFormatType(this.formatType);
        return sd;
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
    public void packRow(FDBStore store, Session session,
                        FDBStoreData storeData, Row row) {
        ensureRowConverter();
        DynamicMessage msg = rowConverter.encode(row);
        storeData.rawValue = msg.toByteArray();
    }
    
    @Override
    public void packRowData(FDBStore store, Session session,
                            FDBStoreData storeData, RowData rowData) {
        ensureRowDataConverter();
        DynamicMessage msg = rowDataConverter.encode(rowData);
        storeData.rawValue = msg.toByteArray();        
    }

    @Override 
    public Row expandRow (FDBStore store, Session session,
                            FDBStoreData storeData, Schema schema) {
        ensureRowConverter();
        DynamicMessage msg;
        try {
            msg = DynamicMessage.parseFrom(rowConverter.getMessageType(), storeData.rawValue);
        } catch (InvalidProtocolBufferException ex) {
            ProtobufReadException nex = new ProtobufReadException(rowDataConverter.getMessageType().getName(), ex.getMessage());
            nex.initCause(ex);
            throw nex;
        }
        return rowConverter.decode(msg);
    }

    @Override
    public void expandRowData(FDBStore store, Session session,
                              FDBStoreData storeData, RowData rowData) {
        ensureRowDataConverter();
        DynamicMessage msg;
        try {
            msg = DynamicMessage.parseFrom(rowDataConverter.getMessageType(), storeData.rawValue);
        }
        catch (InvalidProtocolBufferException ex) {
            ProtobufReadException nex = new ProtobufReadException(rowDataConverter.getMessageType().getName(), ex.getMessage());
            nex.initCause(ex);
            throw nex;
        }
        rowDataConverter.decode(msg, rowData);
    }

}
