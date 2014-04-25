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
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.CommonProtobuf;
import com.foundationdb.ais.protobuf.CommonProtobuf.ProtobufRowFormat;
import com.foundationdb.server.error.ProtobufBuildException;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;

/** <code><i>Protobuf</i>StorageDescription</code> needs to depend the
 * store's base class, so this takes care of store independent
 * handling.
 */
class ProtobufStorageDescriptionHelper
{
    private ProtobufStorageDescriptionHelper() {
    }
    
    public static final FileDescriptor[] DEPENDENCIES = {
        CustomOptions.getDescriptor()
    };

    static void writeProtobuf(Storage.Builder storageBuilder,
                              ProtobufRowFormat.Type formatType,
                              FileDescriptorProto fileProto) {
        ProtobufRowFormat.Builder formatBuilder = ProtobufRowFormat.newBuilder();
        formatBuilder.setType(formatType);
        if (fileProto != null) {
            formatBuilder.setFileDescriptor(fileProto);
        }
        storageBuilder.setExtension(CommonProtobuf.protobufRow, formatBuilder.build());
    }
                              
    static FileDescriptorProto validateAndGenerate(HasStorage object,
                                                   ProtobufRowFormat.Type formatType,
                                                   FileDescriptorProto fileProto,
                                                   AISValidationOutput output) {
        if (!(object instanceof Group)) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is not a Group and cannot use Protocol Buffers")));
            return null;
        }
        Group group = (Group)object;
        if (formatType == ProtobufRowFormat.Type.SINGLE_TABLE) {
            if (!group.getRoot().getChildJoins().isEmpty()) {
                output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "has more than one table")));
                return null;
            }
        }
        int currentVersion = sumTableVersions(group.getRoot());
        if (fileProto != null) {
            int storedVersion = fileProto.getOptions()
                .getExtension(CustomOptions.GroupOptions.fdbsql).getVersion();
            if (storedVersion == currentVersion) {
                return fileProto;
            }
        }
        FileDescriptorSet set = null;
        if (fileProto != null) {
            FileDescriptorSet.Builder builder = FileDescriptorSet.newBuilder();
            builder.addFile(fileProto);
            set = builder.build();
        }
        AISToProtobuf ais2p = new AISToProtobuf(formatType, set);
        ais2p.addGroup(group);
        set = ais2p.build();
        fileProto = set.getFile(0); // Only added one group.
        // Make sure it will build before committing to this format.
        try {
            FileDescriptor.buildFrom(fileProto, DEPENDENCIES);
        }
        catch (DescriptorValidationException ex) {
            output.reportFailure(new AISValidationFailure(new ProtobufBuildException(ex)));
        }
        return fileProto;
    }
                         
    private static int sumTableVersions(Table table) {
        int sum = table.getVersion() + 1;
        for (Join join : table.getChildJoins()) {
            sum += sumTableVersions(join.getChild());
        }
        return sum;
    }

    static ProtobufRowDataConverter buildConverter(HasStorage object,
                                                   FileDescriptorProto fileProto) {
        Group group = (Group)object;
        FileDescriptor fileDescriptor;
        try {
            fileDescriptor = FileDescriptor.buildFrom(fileProto, DEPENDENCIES);
        }
        catch (DescriptorValidationException ex) {
            throw new ProtobufBuildException(ex);
        }
        return ProtobufRowDataConverter.forGroup(group, fileDescriptor);
    }

}
