/**
 * Copyright (C) 2012 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.protobuf;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;

import com.akiban.ais.model.UserTable;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ProtobufWriter {
    private static class Schema {
        public final String name;
        public final List<Group> groups = new ArrayList<Group>();
        public final List<UserTable> userTables = new ArrayList<UserTable>();

        public Schema(String name) {
            this.name = name;
        }
    }

    private final ByteBuffer buffer;
    private AISProtobuf.AkibanInformationSchema pbAIS;
    private Map<String,Schema> schemaMap = new TreeMap<String,Schema>();


    public ProtobufWriter(ByteBuffer buffer) {
        assert buffer.hasArray() : buffer;
        this.buffer = buffer;
    }

    public void save(AkibanInformationSchema ais) {
        // Collect into schemas until that it is a top level AIS object
        for(Group group : ais.getGroups().values()) {
            String schemaName = group.getGroupTable().getRoot().getName().getSchemaName();
            getSchemaFromName(schemaName).groups.add(group);
        }

        for(UserTable userTable : ais.getUserTables().values()) {
            String schemaName = userTable.getName().getSchemaName();
            getSchemaFromName(schemaName).userTables.add(userTable);
        }

        AISProtobuf.AkibanInformationSchema.Builder aisBuilder = AISProtobuf.AkibanInformationSchema.newBuilder();

        // Write top level proto messages and recurse down as needed
        for(Type type : ais.getTypes()) {
            writeType(aisBuilder, type);
        }

        for(Schema schema : schemaMap.values()) {
            writeSchema(aisBuilder, schema);
        }

        pbAIS = aisBuilder.build();
        writeMessageLite(pbAIS);
    }

    AISProtobuf.AkibanInformationSchema getProtobufAIS() {
        return pbAIS;
    }

    private Schema getSchemaFromName(String schemaName) {
        Schema schema = schemaMap.get(schemaName);
        if(schema == null) {
            schema = new Schema(schemaName);
            schemaMap.put(schemaName, schema);
        }
        return schema;
    }

    private void writeMessageLite(MessageLite msg) {
        final int serializedSize = msg.getSerializedSize();
        buffer.putInt(serializedSize);
        final int initialPos = buffer.position();
        CodedOutputStream codedOutput = CodedOutputStream.newInstance(buffer.array(), initialPos, buffer.limit());
        try {
            msg.writeTo(codedOutput);
            // Successfully written, update backing buffer info
            buffer.position(initialPos + serializedSize);
        } catch(IOException e) {
            // CodedOutputStream really only throws OutOfSpace exception, but declares IOE
            throw new BufferOverflowException();
        }
    }

    private static void writeType(AISProtobuf.AkibanInformationSchema.Builder aisBuilder, Type type) {
        AISProtobuf.Type pbType = AISProtobuf.Type.newBuilder().
                setTypeName(type.name()).
                setParameters(type.nTypeParameters()).
                setFixedSize(type.fixedSize()).
                setMaxSizeBytes(type.maxSizeBytes()).
                build();
        aisBuilder.addTypes(pbType);
    }

    private static void writeSchema(AISProtobuf.AkibanInformationSchema.Builder aisBuilder, Schema schema) {
        AISProtobuf.Schema.Builder schemaBuilder = AISProtobuf.Schema.newBuilder();
        schemaBuilder.setSchemaName(schema.name);

        for(Group group : schema.groups) {
            writeGroup(schemaBuilder, group);
        }

        for(UserTable table : schema.userTables) {
            writeTable(schemaBuilder, table);
        }

        aisBuilder.addSchemas(schemaBuilder.build());
    }

    private static void writeGroup(AISProtobuf.Schema.Builder schemaBuilder, Group group) {
        final UserTable rootTable = group.getGroupTable().getRoot();
        AISProtobuf.Group.Builder groupBuilder = AISProtobuf.Group.newBuilder().
                setRootTableName(rootTable.getName().getTableName()).
                setTreeName(rootTable.getTreeName());

        for(Index index : group.getIndexes()) {
            writeGroupIndex(groupBuilder, index);
        }

        schemaBuilder.addGroups(groupBuilder.build());
    }

    private static void writeTable(AISProtobuf.Schema.Builder schemaBuilder, UserTable table) {
        AISProtobuf.Table.Builder tableBuilder = AISProtobuf.Table.newBuilder();
        tableBuilder.
                setTableName(table.getName().getTableName()).
                setTableId(table.getTableId()).
                setCharColl(makeCharCollation(table.getCharsetAndCollation()));
                // Not yet in AIS: ordinal, description, protected

        for(Column column : table.getColumns()) {
            writeColumn(tableBuilder, column);
        }

        for(Index index : table.getIndexes()) {
            writeTableIndex(tableBuilder, index);
        }

        Join join = table.getParentJoin();
        if(join != null) {
            final UserTable parent = join.getParent();
            AISProtobuf.Join.Builder joinBuilder = AISProtobuf.Join.newBuilder();
            joinBuilder.setParentTable(AISProtobuf.TableName.newBuilder().
                    setSchemaName(parent.getName().getSchemaName()).
                    setTableName(parent.getName().getTableName()).
                    build());

            int position = 0;
            for(JoinColumn joinColumn : join.getJoinColumns()) {
                joinBuilder.addColumns(AISProtobuf.JoinColumn.newBuilder().
                        setParentColumn(joinColumn.getParent().getName()).
                        setChildColumn(joinColumn.getChild().getName()).
                        setPosition(position++).
                        build());
            }

            tableBuilder.setParentTable(joinBuilder.build());
        }

        schemaBuilder.addTables(tableBuilder.build());
    }

    private static void writeColumn(AISProtobuf.Table.Builder tableBuilder, Column column) {
        AISProtobuf.Column.Builder columnBuilder = AISProtobuf.Column.newBuilder();
        columnBuilder.
                setColumnName(column.getName()).
                setTypeName(column.getType().name()).
                setIsNullable(column.getNullable()).
                setPosition(column.getPosition()).
                setCharColl(makeCharCollation(column.getCharsetAndCollation()));

        if(column.getTypeParameter1() != null) {
            columnBuilder.setTypeParam1(column.getTypeParameter1());
        }
        if(column.getTypeParameter2() != null) {
            columnBuilder.setTypeParam2(column.getTypeParameter2());
        }
        if(column.getInitialAutoIncrementValue() != null) {
            columnBuilder.setInitAutoInc(column.getInitialAutoIncrementValue());
        }
        
        tableBuilder.addColumns(columnBuilder.build());
    }

    private static AISProtobuf.Index writeIndexCommon(Index index, boolean withTableName) {
        final IndexName indexName = index.getIndexName();
        AISProtobuf.Index.Builder indexBuilder = AISProtobuf.Index.newBuilder();
        indexBuilder.
                setIndexName(indexName.getName()).
                setTreeName(index.getTreeName()).
                setIndexId(index.getIndexId()).
                setIsPK(index.isPrimaryKey()).
                setIsUnique(index.isUnique()).
                setIsAkFK(index.isAkibanForeignKey()).
                setJoinType(getProtoJoinType(index.getJoinType()));
                // Not yet in AIS: description

        for(IndexColumn indexColumn : index.getKeyColumns()) {
            writeIndexColumn(indexBuilder, indexColumn, withTableName);
        }

        return indexBuilder.build();
    }

    private static void writeTableIndex(AISProtobuf.Table.Builder tableBuilder, Index index) {
        tableBuilder.addIndexes(writeIndexCommon(index, false));
    }

    private static void writeGroupIndex(AISProtobuf.Group.Builder tableBuilder, Index index) {
        tableBuilder.addIndexes(writeIndexCommon(index, true));
    }

    private static void writeIndexColumn(AISProtobuf.Index.Builder indexBuilder, IndexColumn indexColumn, boolean withTableName) {
        AISProtobuf.IndexColumn.Builder indexColumnBuilder = AISProtobuf.IndexColumn.newBuilder().
                setColumnName(indexColumn.getColumn().getName()).
                setIsAscending(indexColumn.isAscending()).
                setPosition(indexColumn.getPosition());
        
        if(withTableName) {
            TableName tableName = indexColumn.getColumn().getTable().getName();
            indexColumnBuilder.setTableName(
                    AISProtobuf.TableName.newBuilder().
                            setSchemaName(tableName.getSchemaName()).
                            setTableName(tableName.getTableName()).
                            build()
            );
        }

        indexBuilder.addColumns(indexColumnBuilder.build());
    }

    private static AISProtobuf.JoinType getProtoJoinType(Index.JoinType joinType) {
        switch(joinType) {
            case LEFT: return AISProtobuf.JoinType.LEFT_OUTER_JOIN;
            case RIGHT: return AISProtobuf.JoinType.RIGHT_OUTER_JOIN;
        }
        throw new IllegalStateException("Unknown JoinType: " + joinType);
    }

    private static AISProtobuf.CharCollation makeCharCollation(CharsetAndCollation charAndColl) {
        if(charAndColl == null) {
            return null;
        }
        return AISProtobuf.CharCollation.newBuilder().
                setCharacterSetName(charAndColl.charset()).
                setCollationOrderName(charAndColl.collation()).
                build();
    }
}
