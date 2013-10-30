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
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;

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
    private static final Map<TClass,Type> TYPE_MAPPING = new HashMap<>();
    static {
        TYPE_MAPPING.put(MNumeric.BIGINT, Type.TYPE_SINT64);
        TYPE_MAPPING.put(MNumeric.BIGINT_UNSIGNED, Type.TYPE_SINT64);
        TYPE_MAPPING.put(MApproximateNumber.DOUBLE, Type.TYPE_DOUBLE);
        TYPE_MAPPING.put(MApproximateNumber.DOUBLE_UNSIGNED, Type.TYPE_DOUBLE);
        TYPE_MAPPING.put(MApproximateNumber.FLOAT, Type.TYPE_FLOAT);
        TYPE_MAPPING.put(MApproximateNumber.FLOAT_UNSIGNED, Type.TYPE_FLOAT);
        TYPE_MAPPING.put(MNumeric.INT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MNumeric.INT_UNSIGNED, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MNumeric.MEDIUMINT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MNumeric.MEDIUMINT_UNSIGNED, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MNumeric.SMALLINT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MNumeric.SMALLINT_UNSIGNED, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MNumeric.TINYINT, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MNumeric.TINYINT_UNSIGNED, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MDatetimes.DATE, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MDatetimes.DATETIME, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MDatetimes.YEAR, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MDatetimes.TIME, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MDatetimes.TIMESTAMP, Type.TYPE_SINT32);
        TYPE_MAPPING.put(MBinary.VARBINARY, Type.TYPE_BYTES);
        TYPE_MAPPING.put(MBinary.BINARY, Type.TYPE_BYTES);
        TYPE_MAPPING.put(MString.VARCHAR, Type.TYPE_STRING);
        TYPE_MAPPING.put(MString.CHAR, Type.TYPE_STRING);
        TYPE_MAPPING.put(MBinary.TINYBLOB, Type.TYPE_BYTES);
        TYPE_MAPPING.put(MString.TINYTEXT, Type.TYPE_STRING);
        TYPE_MAPPING.put(MBinary.BLOB, Type.TYPE_BYTES);
        TYPE_MAPPING.put(MString.TEXT, Type.TYPE_STRING);
        TYPE_MAPPING.put(MBinary.MEDIUMBLOB, Type.TYPE_BYTES);
        TYPE_MAPPING.put(MString.MEDIUMTEXT, Type.TYPE_STRING);
        TYPE_MAPPING.put(MBinary.LONGBLOB, Type.TYPE_BYTES);
        TYPE_MAPPING.put(MString.LONGTEXT, Type.TYPE_STRING);
        TYPE_MAPPING.put(AkBool.INSTANCE, Type.TYPE_BOOL);
        TYPE_MAPPING.put(MNumeric.DECIMAL, Type.TYPE_SINT64);
        TYPE_MAPPING.put(MNumeric.DECIMAL_UNSIGNED, Type.TYPE_SINT64);
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
            tableMessageNames.put(table, uniqueIdent(ident(table.getName().getTableName(), true), messageNames));
        }
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
        messageBuilder = fileBuilder.addMessageTypeBuilder();
        messageBuilder.setName(tableMessageNames.get(table));
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
        for (Table child : tables) { // Continue to follow ordinal order.
            if (child.parentTable() == table) {
                addChildTable(child);
            }
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
        Type type;
        int decimalScale = -1;
        TClass tclass = TInstance.tClass(column.tInstance());
        if (tclass instanceof MBigDecimal) {
            decimalScale = column.getTypeParameter2().intValue();
            int precision = column.getTypeParameter1().intValue();
            if (precision < 19) { // log10(Long.MAX_VALUE) = 18.965
                type = Type.TYPE_SINT64;
            }
            else {
                type = Type.TYPE_BYTES;
            }
        }
        else {
            type = TYPE_MAPPING.get(tclass);
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

    protected void addChildTable(Table table) {
        String fieldName = uniqueIdent(ident(table.getName().getTableName(), false), fieldNames);
        fieldBuilder = messageBuilder.addFieldBuilder();
        fieldBuilder.setName(fieldName);
        fieldBuilder.setLabel(Label.LABEL_REPEATED);
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
