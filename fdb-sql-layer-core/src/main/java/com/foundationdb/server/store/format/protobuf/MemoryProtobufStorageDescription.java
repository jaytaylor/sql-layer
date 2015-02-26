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

package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf.ProtobufRowFormat;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.ProtobufReadException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.MemoryStore;
import com.foundationdb.server.store.MemoryStoreData;
import com.foundationdb.server.store.format.MemoryStorageDescription;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import static com.foundationdb.server.store.format.protobuf.ProtobufStorageDescriptionHelper.*;

public class MemoryProtobufStorageDescription extends MemoryStorageDescription implements ProtobufStorageDescription
{
    private ProtobufRowFormat.Type formatType;
    private FileDescriptorProto fileProto;
    private transient ProtobufRowConverter rowConverter;

    public MemoryProtobufStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public MemoryProtobufStorageDescription(HasStorage forObject,
                                            MemoryProtobufStorageDescription other,
                                            String storageFormat) {
        super(forObject, other, storageFormat);
        this.formatType = other.formatType;
        this.fileProto = other.fileProto;
    }

    public synchronized ProtobufRowConverter ensureRowConverter() {
        if(rowConverter == null) {
            rowConverter = buildRowConverter(object, fileProto);
        }
        return rowConverter;
    }

    public void setFormatType(ProtobufRowFormat.Type formatType) {
        this.formatType = formatType;
    }

    public void readProtobuf(ProtobufRowFormat pbFormat) {
        formatType = pbFormat.getType();
        fileProto = pbFormat.getFileDescriptor();
    }

    //
    // StorageDescription
    //

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new MemoryProtobufStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        MemoryProtobufStorageDescription sd = new MemoryProtobufStorageDescription(forObject, storageFormat);
        sd.setFormatType(this.formatType);
        return sd;
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

    //
    // StoreStorageDescription
    //

    @Override
    public Row expandRow(MemoryStore store, Session session, MemoryStoreData storeData, Schema schema) {
        ensureRowConverter();
        DynamicMessage msg;
        try {
            msg = DynamicMessage.parseFrom(rowConverter.getMessageType(), storeData.rawValue);
        } catch(InvalidProtocolBufferException ex) {
            ProtobufReadException nex = new ProtobufReadException(rowConverter.getMessageType().getName(), ex.getMessage());
            nex.initCause(ex);
            throw nex;
        }
        return rowConverter.decode(msg);
    }

    @Override
    public void packRow(MemoryStore store, Session session, MemoryStoreData storeData, Row row) {
        ensureRowConverter();
        DynamicMessage msg = rowConverter.encode(row);
        storeData.rawValue = msg.toByteArray();
    }

    //
    // ProtobufStorageDescription
    //

    @Override
    public FileDescriptorProto getFileProto() {
        return fileProto;
    }

    @Override
    public ProtobufRowFormat.Type getFormatType() {
        return formatType;
    }
}
