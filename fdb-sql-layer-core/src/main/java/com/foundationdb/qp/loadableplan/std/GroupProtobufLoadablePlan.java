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

package com.foundationdb.qp.loadableplan.std;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.protobuf.ProtobufDecompiler;
import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.operator.BindingNotSetException;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.NoSuchGroupException;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.store.format.protobuf.ProtobufStorageDescription;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;

import java.sql.Types;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Output group protobuf definition.
 */
public class GroupProtobufLoadablePlan extends LoadableDirectObjectPlan
{
    @Override
    public DirectObjectPlan plan() {
        return new DirectObjectPlan() {

            @Override
            public DirectObjectCursor cursor(QueryContext context, QueryBindings bindings) {
                return new GroupProtobufCursor(context, bindings);
            }

            @Override
            public TransactionMode getTransactionMode() {
                return TransactionMode.READ_ONLY;
            }

            @Override
            public OutputMode getOutputMode() {
                return OutputMode.COPY_WITH_NEWLINE;
            }
        };
    }
    
    public static final int MESSAGES_PER_FLUSH = 100;

    public static class GroupProtobufCursor extends DirectObjectCursor {
        private final QueryContext context;
        private final QueryBindings bindings;
        private File tempFile;
        private BufferedReader reader;
        private int messagesSent;

        public GroupProtobufCursor(QueryContext context, QueryBindings bindings) {
            this.context = context;
            this.bindings = bindings;
        }

        @Override
        public void open() {
            String currentSchema = context.getCurrentSchema();
            String schemaName, tableName;
            ValueSource value = valueNotNull(0);
            if (value == null)
                schemaName = currentSchema;
            else
                schemaName = value.getString();
            tableName = bindings.getValue(1).getString();
            TableName groupName = new TableName(schemaName, tableName);
            Group group = context.getStore().schema().ais().getGroup(groupName);
            if (group == null)
                throw new NoSuchGroupException(groupName);
            StorageDescription storage = group.getStorageDescription();
            if (!(storage instanceof ProtobufStorageDescription))
                throw new InvalidParameterValueException("group does not use STORAGE_FORMAT protobuf");
            FileDescriptorProto fileProto = ((ProtobufStorageDescription)storage).getFileProto();
            try {
                tempFile = File.createTempFile("group", ".proto");
                try (FileWriter writer = new FileWriter(tempFile)) {
                    new ProtobufDecompiler(writer).decompile(fileProto);
                }
                reader = new BufferedReader(new FileReader(tempFile));
            }
            catch (IOException ex) {
                throw new AkibanInternalException("decompiling error", ex);
            }
            messagesSent = 0;
        }

        protected ValueSource valueNotNull(int index) {
            try {
                ValueSource value = bindings.getValue(index);
                if (value.isNull())
                    return null;
                else
                    return value;
            }
            catch (BindingNotSetException ex) {
                return null;
            }            
        }

        @Override
        public List<String> next() {
            if (messagesSent >= MESSAGES_PER_FLUSH) {
                messagesSent = 0;
                return Collections.emptyList();
            }
            String line;
            try {
                line = reader.readLine();
            }
            catch (IOException ex) {
                throw new AkibanInternalException("temp file error", ex);
            }
            if (line == null)
                return null;
            else
                return Collections.singletonList(line);
        }

        @Override
        public void close() {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ex) {
                }
                reader = null;
            }
            if (tempFile != null) {
                tempFile.delete();
                tempFile = null;
            }
        }
    }

    @Override
    public int[] jdbcTypes() {
        return TYPES;
    }

    private static final int[] TYPES = new int[] { Types.VARCHAR };
}
