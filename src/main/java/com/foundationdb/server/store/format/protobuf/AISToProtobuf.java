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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.Types;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.MessageOptions;

import com.foundationdb.server.store.format.protobuf.CustomOptions.GroupOptions;
import com.foundationdb.server.store.format.protobuf.CustomOptions.TableOptions;
import com.foundationdb.server.store.format.protobuf.CustomOptions.ColumnOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AISToProtobuf
{
    private static final Map<com.foundationdb.ais.model.Type,
        com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type> TYPE_MAPPING = new HashMap<>();
    static {
        TYPE_MAPPING.put(Types.BIGINT, Type.TYPE_SINT64);
        TYPE_MAPPING.put(Types.U_BIGINT, Type.TYPE_SINT64);
        TYPE_MAPPING.put(Types.DOUBLE, Type.TYPE_DOUBLE);
        TYPE_MAPPING.put(Types.U_DOUBLE, Type.TYPE_DOUBLE);
        TYPE_MAPPING.put(Types.FLOAT, Type.TYPE_FLOAT);
        TYPE_MAPPING.put(Types.U_FLOAT, Type.TYPE_FLOAT);
        TYPE_MAPPING.put(Types.INT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.U_INT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.MEDIUMINT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.U_MEDIUMINT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.SMALLINT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.U_SMALLINT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.TINYINT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.U_TINYINT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.DATE, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.DATETIME, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.YEAR, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.TIME, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.TIMESTAMP, Type.TYPE_SINT32);
        TYPE_MAPPING.put(Types.VARBINARY, Type.TYPE_BYTES);
        TYPE_MAPPING.put(Types.BINARY, Type.TYPE_BYTES);
        TYPE_MAPPING.put(Types.VARCHAR, Type.TYPE_STRING);
        TYPE_MAPPING.put(Types.CHAR, Type.TYPE_STRING);
        TYPE_MAPPING.put(Types.TINYBLOB, Type.TYPE_BYTES);
        TYPE_MAPPING.put(Types.TINYTEXT, Type.TYPE_STRING);
        TYPE_MAPPING.put(Types.BLOB, Type.TYPE_BYTES);
        TYPE_MAPPING.put(Types.TEXT, Type.TYPE_STRING);
        TYPE_MAPPING.put(Types.MEDIUMBLOB, Type.TYPE_BYTES);
        TYPE_MAPPING.put(Types.MEDIUMTEXT, Type.TYPE_STRING);
        TYPE_MAPPING.put(Types.LONGBLOB, Type.TYPE_BYTES);
        TYPE_MAPPING.put(Types.LONGTEXT, Type.TYPE_STRING);
        TYPE_MAPPING.put(Types.BOOLEAN, Type.TYPE_BOOL);
        TYPE_MAPPING.put(Types.DECIMAL, Type.TYPE_SINT64);
        TYPE_MAPPING.put(Types.U_DECIMAL, Type.TYPE_SINT64);
    }
    private List<Table> tables = new ArrayList<>();
    private Map<Table,String> tableMessageNames = new HashMap<>();
    private FileDescriptorSet.Builder setBuilder;
    private FileDescriptorSet priorSet;
    private FileDescriptorProto.Builder fileBuilder;
    private FileDescriptorProto priorFile;
    private DescriptorProto.Builder messageBuilder;
    private DescriptorProto priorMessage;
    private FieldDescriptorProto.Builder fieldBuilder;
    private FieldDescriptorProto priorField;
    private Set<String> messageNames = new HashSet<>();
    private Set<String> fieldNames = new HashSet<>();
    private int nextField;

    public AISToProtobuf() {
        this(null);
    }

    public AISToProtobuf(FileDescriptorSet priorSet) {
        setBuilder = FileDescriptorSet.newBuilder();
        this.priorSet = priorSet;
    }

    public FileDescriptorSet build() {
        return setBuilder.build();
    }

    public void addGroup(Group group) {
        tables.clear();
        findTables(group.getRoot());
        Collections.sort(tables, new Comparator<Table>() {
                             @Override
                             public int compare(Table t1, Table t2) {
                                 return Integer.compare(t1.getOrdinal(), 
                                                        t2.getOrdinal());
                             }
                         });
        fileBuilder = setBuilder.addFileBuilder();
        fileBuilder.setName(ident(group.getName().getTableName(), false) + ".proto");
        fileBuilder.setPackage(ident(group.getName().getSchemaName(), false));
        fileBuilder.addDependency("sql_custom_options.proto");
        FileOptions.Builder fileOptions = FileOptions.newBuilder();
        GroupOptions.Builder groupOptions = GroupOptions.newBuilder();
        groupOptions.setName(group.getName().getTableName());
        groupOptions.setSchema(group.getName().getSchemaName());
        priorFile = null;
        if (priorSet != null) {
            String rootUuid = group.getRoot().getUuid().toString();
            for (FileDescriptorProto file : priorSet.getFileList()) {
                DescriptorProto firstMessage = file.getMessageType(0);
                MessageOptions options = firstMessage.getOptions();
                if ((options != null) &&
                    (options.hasExtension(TableOptions.fdbsql))) {
                    TableOptions tableOptions = options.getExtension(TableOptions.fdbsql);
                    if (tableOptions.getUuid().equals(rootUuid)) {
                        priorFile = file;
                        break;
                    }
                }
            }
        }
        messageNames.clear();
        for (Table table : tables) {
            addTable(table);
        }
        addGroupMessage();
        fileOptions.setExtension(GroupOptions.fdbsql, groupOptions.build());
        fileBuilder.setOptions(fileOptions);
    }

    protected void findTables(Table table) {
        tables.add(table);
        for (Join join : table.getChildJoins()) {
            findTables(join.getChild());
        }
    }

    protected void addTable(Table table) {
        String tableName = uniqueIdent(ident(table.getName().getTableName(), true), messageNames);
        tableMessageNames.put(table, tableName);
        messageBuilder = fileBuilder.addMessageTypeBuilder();
        messageBuilder.setName(tableName);
        MessageOptions.Builder messageOptions = MessageOptions.newBuilder();
        TableOptions.Builder tableOptions = TableOptions.newBuilder();
        tableOptions.setName(table.getName().getTableName());
        tableOptions.setSchema(table.getName().getSchemaName());
        tableOptions.setUuid(table.getUuid().toString());
        priorMessage = null;
        if (priorFile != null) {
            for (DescriptorProto message : priorFile.getMessageTypeList()) {
                MessageOptions options = message.getOptions();
                if ((options != null) &&
                    (options.hasExtension(TableOptions.fdbsql))) {
                    TableOptions toptions = options.getExtension(TableOptions.fdbsql);
                    if (toptions.getUuid().equals(tableOptions.getUuid())) {
                        priorMessage = message;
                        break;
                    }
                }
            }
        }
        nextField = 1;
        if (priorMessage != null) {
            TableOptions options = priorMessage.getOptions().getExtension(TableOptions.fdbsql);
            if (options.hasNextField()) {
                nextField = options.getNextField();
            }
            else {
                nextField = priorMessage.getField(priorMessage.getFieldCount() - 1)
                    .getNumber() + 1;
            }
        }
        fieldNames.clear();
        for (Column column : table.getColumns()) {
            addColumn(column);
        }
        if (nextField != messageBuilder.getFieldOrBuilder(messageBuilder.getFieldCount() - 1).getNumber() + 1) {
            tableOptions.setNextField(nextField);
        }
        messageOptions.setExtension(TableOptions.fdbsql, tableOptions.build());
        messageBuilder.setOptions(messageOptions);
    }

    protected void addColumn(Column column) {
        String fieldName = uniqueIdent(ident(column.getName(), false), fieldNames);
        fieldBuilder = messageBuilder.addFieldBuilder();
        fieldBuilder.setName(fieldName);
        fieldBuilder.setLabel(Label.LABEL_OPTIONAL);
        FieldOptions.Builder fieldBuilderOptions = FieldOptions.newBuilder();
        ColumnOptions.Builder columnOptions = ColumnOptions.newBuilder();
        if (!fieldName.equals(column.getName())) {
            columnOptions.setName(column.getName());
        }
        columnOptions.setSqlType(column.getTypeDescription().toUpperCase());
        columnOptions.setUuid(column.getUuid().toString());
        priorField = null;
        if (priorMessage != null) {
            for (FieldDescriptorProto field : priorMessage.getFieldList()) {
                FieldOptions options = field.getOptions();
                if ((options != null) &&
                    (options.hasExtension(ColumnOptions.fdbsql))) {
                    ColumnOptions coptions = options.getExtension(ColumnOptions.fdbsql);
                    if (coptions.getUuid().equals(columnOptions.getUuid())) {
                        priorField = field;
                        break;
                    }
                }
            }
        }
        setColumnType(column, columnOptions);
        setFieldNumber();
        fieldBuilderOptions.setExtension(ColumnOptions.fdbsql, columnOptions.build());
        fieldBuilder.setOptions(fieldBuilderOptions);
        if (column.getNullable() && 
            ((column.getDefaultValue() != null) ||
             (column.getDefaultFunction() != null))) {
            addNullForField(column.getName(), fieldBuilder.getNumber());
        }
    }

    protected void setColumnType(Column column, ColumnOptions.Builder columnOptions) {
        Type type = TYPE_MAPPING.get(column.getType());
        int decimalScale = -1;
        if ((column.getType() == Types.DECIMAL) ||
            (column.getType() == Types.U_DECIMAL)) {
            decimalScale = column.getTypeParameter2().intValue();
            int precision = column.getTypeParameter1().intValue();
            if (precision >= 18) {
                type = Type.TYPE_BYTES;
            }
        }
        fieldBuilder.setType(type);
        if (decimalScale >= 0) {
            columnOptions.setDecimalScale(decimalScale);
        }
        if ((priorField != null) &&
            ((priorField.getType() != type) ||
             (priorDecimalScale() != decimalScale))) {
            priorField = null;
        }
    }

    protected int priorDecimalScale() {
        if (priorField != null) {
            ColumnOptions columnOptions = priorField.getOptions().getExtension(ColumnOptions.fdbsql);
            if (columnOptions.hasDecimalScale()) {
                return columnOptions.getDecimalScale();
            }
        }
        return -1;
    }

    protected void setFieldNumber() {
        if (priorField != null) {
            fieldBuilder.setNumber(priorField.getNumber());
            if (fieldBuilder.getNumber() >= nextField) {
                nextField = fieldBuilder.getNumber() + 1;
            }
        }
        else {
            fieldBuilder.setNumber(nextField++);
        }
    }

    protected void addNullForField(String columnName, int forField) {
        String fieldName = uniqueIdent("_" + ident(columnName, false) + "_is_null", fieldNames);
        fieldBuilder = messageBuilder.addFieldBuilder();
        fieldBuilder.setName(fieldName);
        fieldBuilder.setType(Type.TYPE_BOOL);
        fieldBuilder.setLabel(Label.LABEL_OPTIONAL);
        FieldOptions.Builder fieldBuilderOptions = FieldOptions.newBuilder();
        ColumnOptions.Builder columnOptions = ColumnOptions.newBuilder();
        columnOptions.setNullForField(forField);
        priorField = null;
        if (priorMessage != null) {
            for (FieldDescriptorProto field : priorMessage.getFieldList()) {
                FieldOptions options = field.getOptions();
                if ((options != null) &&
                    (options.hasExtension(ColumnOptions.fdbsql))) {
                    ColumnOptions coptions = options.getExtension(ColumnOptions.fdbsql);
                    if (coptions.hasNullForField() &&
                        (coptions.getNullForField() == forField)) {
                        priorField = field;
                        break;
                    }
                }
            }
        }
        setFieldNumber();
        fieldBuilderOptions.setExtension(ColumnOptions.fdbsql, columnOptions.build());
        fieldBuilder.setOptions(fieldBuilderOptions);
    }

    protected void addGroupMessage() {
        messageBuilder = fileBuilder.addMessageTypeBuilder();
        messageBuilder.setName(uniqueIdent("_Group", messageNames));
        MessageOptions.Builder messageOptions = MessageOptions.newBuilder();
        TableOptions.Builder tableOptions = TableOptions.newBuilder();
        tableOptions.setIsGroup(true);
        priorMessage = null;
        if (priorFile != null) {
            for (DescriptorProto message : priorFile.getMessageTypeList()) {
                MessageOptions options = message.getOptions();
                if ((options != null) &&
                    (options.hasExtension(TableOptions.fdbsql))) {
                    TableOptions toptions = options.getExtension(TableOptions.fdbsql);
                    if (toptions.getIsGroup()) {
                        priorMessage = message;
                        break;
                    }
                }
            }
        }
        nextField = 1;
        if (priorMessage != null) {
            TableOptions options = priorMessage.getOptions().getExtension(TableOptions.fdbsql);
            if (options.hasNextField()) {
                nextField = options.getNextField();
            }
            else {
                nextField = priorMessage.getField(priorMessage.getFieldCount() - 1)
                    .getNumber() + 1;
            }
        }
        fieldNames.clear();
        for (Table table : tables) {
            String fieldName = uniqueIdent(ident(table.getName().getTableName(), false), fieldNames);
            fieldBuilder = messageBuilder.addFieldBuilder();
            fieldBuilder.setName(fieldName);
            fieldBuilder.setLabel(Label.LABEL_OPTIONAL);
            fieldBuilder.setType(Type.TYPE_MESSAGE);
            fieldBuilder.setTypeName(tableMessageNames.get(table));
            FieldOptions.Builder fieldBuilderOptions = FieldOptions.newBuilder();
            ColumnOptions.Builder columnOptions = ColumnOptions.newBuilder();
            columnOptions.setUuid(table.getUuid().toString());
            priorField = null;
            if (priorMessage != null) {
                for (FieldDescriptorProto field : priorMessage.getFieldList()) {
                    FieldOptions options = field.getOptions();
                    if ((options != null) &&
                        (options.hasExtension(ColumnOptions.fdbsql))) {
                        ColumnOptions coptions = options.getExtension(ColumnOptions.fdbsql);
                        if (coptions.getUuid().equals(columnOptions.getUuid())) {
                            priorField = field;
                            break;
                        }
                    }
                }
            }
            setFieldNumber();
            fieldBuilderOptions.setExtension(ColumnOptions.fdbsql, columnOptions.build());
            fieldBuilder.setOptions(fieldBuilderOptions);
        }
        if (nextField != messageBuilder.getFieldOrBuilder(messageBuilder.getFieldCount() - 1).getNumber() + 1) {
            tableOptions.setNextField(nextField);
        }
        messageOptions.setExtension(TableOptions.fdbsql, tableOptions.build());
        messageBuilder.setOptions(messageOptions);
    }

    protected String ident(String base, boolean camelize) {
        String ident = base.toLowerCase();
        if (camelize) {
            StringBuilder str = new StringBuilder();
            boolean upper = true;
            for (int i = 0; i < ident.length(); i++) {
                char ch = ident.charAt(i);
                if (ch == '_') {
                    upper = true;
                }
                else if (upper) {
                    str.append(Character.toUpperCase(ch));
                    upper = false;
                }
                else {
                    str.append(ch);
                }
            }
            ident = str.toString();
        }
        return ident;
    }

    protected String uniqueIdent(String base, Set<String> existing) {
        for (int i = 0; ; i++) {
            String ident = (i == 0) ? base : base + "_" + i;
            if (existing.add(ident)) {
                return ident;
            }
        }
    }
}
