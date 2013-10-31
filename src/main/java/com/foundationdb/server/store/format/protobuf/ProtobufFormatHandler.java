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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.CommonProtobuf.ProtobufRowFormat;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.store.GeneratedFormatHandler;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.nio.ByteBuffer;

public class ProtobufFormatHandler implements GeneratedFormatHandler<FileDescriptor>
{
    private ProtobufRowFormat format;

    public ProtobufFormatHandler(ProtobufRowFormat format) {
        this.format = format;
    }

    public static final ExtensionRegistry EXTENSIONS = ExtensionRegistry.newInstance();
    static {
        CustomOptions.registerAllExtensions(EXTENSIONS);
    }
    public static final FileDescriptor[] DEPENDENCIES = {
        CustomOptions.getDescriptor()
    };
    private FileDescriptorSet decoded;

    @Override
    public String getKey() {
        return "protobuf";
    }

    @Override
    public FileDescriptor get() {
        try {
            return FileDescriptor.buildFrom(decoded.getFile(0), DEPENDENCIES);
        }
        catch (DescriptorValidationException ex) {
            throw new AkibanInternalException("generated protobuf invalid", ex);
        }
    }
    
    @Override
    public boolean decode(AkibanInformationSchema ais, TableName name, byte[] value) {
        try {
            decoded = FileDescriptorSet.parseFrom(value, EXTENSIONS);
        }
        catch (InvalidProtocolBufferException ex) {
            throw new AkibanInternalException("error decoding saved protobuf", ex);
        }
        return checkValid(ais, name);
    }

    protected boolean checkValid(AkibanInformationSchema ais, TableName name) {
        int storedVersion = decoded.getFile(0).getOptions()
            .getExtension(CustomOptions.GroupOptions.fdbsql).getVersion();
        Group group = ais.getGroup(name);
        if (group == null)
            return true;        // To get error in a better place.
        int currentVersion = sumTableVersions(group.getRoot());
        return (currentVersion == storedVersion);
    }
    
    private int sumTableVersions(Table table) {
        int sum = table.getVersion();
        for (Join join : table.getChildJoins()) {
            sum += sumTableVersions(join.getChild());
        }
        return sum;
    }

    @Override
    public byte[] encode(AkibanInformationSchema ais, TableName name) {
        Group group = ais.getGroup(name);
        if (group == null)
            return null;
        AISToProtobuf ais2p = new AISToProtobuf(format, decoded);
        ais2p.addGroup(group);
        decoded = ais2p.build();
        return decoded.toByteArray();
    }
    
}
