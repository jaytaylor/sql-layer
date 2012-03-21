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
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

public class ProtobufReader {
    private final ByteBuffer buffer;
    private AISProtobuf.AkibanInformationSchema pbAIS;
    
    public ProtobufReader(ByteBuffer buffer) {
        assert buffer.hasArray() : buffer;
        this.buffer = buffer;
    }
    
    public AkibanInformationSchema load()
    {
        return load(new AkibanInformationSchema());
    }

    public AkibanInformationSchema load(AkibanInformationSchema ais)
    {
        loadFromBuffer();
        // All fields (currently types, schemas) are optional
        loadTypes(ais, pbAIS.getTypesList());
        loadSchemas(ais, pbAIS.getSchemasList());
        return ais;
    }

    AISProtobuf.AkibanInformationSchema getProtobufAIS() {
        return pbAIS;
    }
    
    private void loadFromBuffer() {
        final int serializedSize = buffer.getInt();
        final int initialPos = buffer.position();
        CodedInputStream codedInput = CodedInputStream.newInstance(buffer.array(), buffer.position(), serializedSize);
        try {
            pbAIS = AISProtobuf.AkibanInformationSchema.parseFrom(codedInput);
            // Successfully consumed, update byte buffer
            buffer.position(initialPos + serializedSize);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static void loadTypes(AkibanInformationSchema ais, Collection<AISProtobuf.Type> pbTypes) {
        for(AISProtobuf.Type pbType : pbTypes) {
            hasRequiredFields(pbType);
            Type.create(
                    ais,
                    pbType.getTypeName(),
                    pbType.getParameters(),
                    pbType.getFixedSize(),
                    pbType.getMaxSizeBytes(),
                    null,
                    null
            );
        }
    }

    private static void loadSchemas(AkibanInformationSchema ais, Collection<AISProtobuf.Schema> pbSchemas) {
        for(AISProtobuf.Schema pbSchema : pbSchemas) {
            hasRequiredFields(pbSchema);
            String schemaName = pbSchema.getSchemaName();
            loadTables(ais, schemaName, pbSchema.getTablesList());
            loadGroups(ais, schemaName, pbSchema.getGroupsList());
        }
    }
    
    private static void loadGroups(AkibanInformationSchema ais, String schema, Collection<AISProtobuf.Group> pbGroups) {
        DefaultNameGenerator nameGenerator = new DefaultNameGenerator();
        for(AISProtobuf.Group pbGroup : pbGroups) {
            hasRequiredFields(pbGroup);
            String rootTableName = pbGroup.getRootTableName();
            Group group = Group.create(ais, nameGenerator.generateGroupName(rootTableName));
            UserTable userTable = ais.getUserTable(schema, rootTableName);
            userTable.setTreeName(pbGroup.getTreeName());
            userTable.setGroup(group);
        }
    }
    
    private static void loadTables(AkibanInformationSchema ais, String schema, Collection<AISProtobuf.Table> pbTables) {
        for(AISProtobuf.Table pbTable : pbTables) {
            hasRequiredFields(pbTable);
            UserTable userTable = UserTable.create(ais, schema, pbTable.getTableName(), pbTable.getTableId());
            userTable.setCharsetAndCollation(getCharColl(pbTable.hasCharColl(), pbTable.getCharColl()));
            loadColumns(userTable, pbTable.getColumnsList());
            loadIndexes(userTable, pbTable.getIndexesList());
        }
        // Assume no ordering of table list, load joins second
        for(AISProtobuf.Table pbTable : pbTables) {
            if(pbTable.hasParentTable()) {
                AISProtobuf.Join pbJoin = pbTable.getParentTable();
                hasRequiredFields(pbJoin);
                AISProtobuf.TableName pbParentName = pbJoin.getParentTable();
                hasRequiredFields(pbParentName);
                UserTable parentTable = ais.getUserTable(pbParentName.getSchemaName(), pbParentName.getTableName());
                UserTable childTable = ais.getUserTable(schema, pbTable.getTableName());

                String joinName = parentTable.getName() + "/" + childTable.getName();
                Join join = Join.create(ais, joinName, parentTable, childTable);
                for(AISProtobuf.JoinColumn pbJoinColumn : pbJoin.getColumnsList()) {
                    hasRequiredFields(pbJoinColumn);
                    JoinColumn.create(
                            join,
                            parentTable.getColumn(pbJoinColumn.getParentColumn()),
                            childTable.getColumn(pbJoinColumn.getChildColumn())
                    );
                }
            }
        }
    }
    
    private static void loadColumns(UserTable userTable, Collection<AISProtobuf.Column> pbColumns) {
        for(AISProtobuf.Column pbColumn : pbColumns) {
            hasRequiredFields(pbColumn);
            Type type = userTable.getAIS().getType(pbColumn.getTypeName());
            Column.create(
                    userTable,
                    pbColumn.getColumnName(),
                    pbColumn.getPosition(),
                    type,
                    pbColumn.getIsNullable(),
                    getOptionalField(Long.class, pbColumn, AISProtobuf.Column.TYPEPARAM1_FIELD_NUMBER),
                    getOptionalField(Long.class, pbColumn, AISProtobuf.Column.TYPEPARAM2_FIELD_NUMBER),
                    getOptionalField(Long.class, pbColumn, AISProtobuf.Column.INITAUTOINC_FIELD_NUMBER),
                    getCharColl(pbColumn.hasCharColl(), pbColumn.getCharColl())
            );
        }
    }
    
    private static void loadIndexes(UserTable userTable, Collection<AISProtobuf.Index> pbIndexes) {
        for(AISProtobuf.Index pbIndex : pbIndexes) {
            hasRequiredFields(pbIndex);
            TableIndex tableIndex = TableIndex.create(
                    userTable.getAIS(),
                    userTable,
                    pbIndex.getIndexName(),
                    pbIndex.getIndexId(),
                    pbIndex.getIsUnique(),
                    getIndexConstraint(pbIndex)
            );
            loadIndexColumns(tableIndex, pbIndex.getColumnsList());
        }
    }

    private static void loadIndexColumns(TableIndex index, Collection<AISProtobuf.IndexColumn> pbIndexColumns) {
        for(AISProtobuf.IndexColumn pbIndexColumn : pbIndexColumns) {
            hasRequiredFields(pbIndexColumn);
            IndexColumn.create(
                    index,
                    index.getTable().getColumn(pbIndexColumn.getColumnName()),
                    pbIndexColumn.getPosition(),
                    pbIndexColumn.getAscending(),
                    null /* indexedLength, not in proto */
            );
        }
    }

    private static <T> T getOptionalField(Class<T> clazz, AbstractMessage message, int fieldNumber) {
        Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByNumber(fieldNumber);
        if(message.hasField(field)) {
            Object obj = message.getField(field);
            if(!obj.getClass().equals(clazz)) {
                throw new IllegalArgumentException("Unexpected class: " + clazz + " vs " + obj.getClass());
            }
            return clazz.cast(obj);
        }
        return null;
    }

    private static String getIndexConstraint(AISProtobuf.Index pbIndex) {
        if(pbIndex.getIsPK()) {
            return Index.PRIMARY_KEY_CONSTRAINT;
        }
        if(pbIndex.getIsAkFK()) {
            return Index.FOREIGN_KEY_CONSTRAINT;
        }
        if(pbIndex.getIsUnique()) {
            return Index.UNIQUE_KEY_CONSTRAINT;
        }
        return Index.KEY_CONSTRAINT;
    }

    private static CharsetAndCollation getCharColl(boolean isValid, AISProtobuf.CharCollation pbCharAndCol) {
        if(isValid) {
            hasRequiredFields(pbCharAndCol);
            return CharsetAndCollation.intern(pbCharAndCol.getCharacterSetName(),
                                              pbCharAndCol.getCollationOrderName());
        }
        return null;
    }

    // Require all by default
    private static void hasRequiredFields(AbstractMessage message) {
        checkRequiredFields(message);
    }

    private static void hasRequiredFields(AISProtobuf.Group pbGroup) {
        checkRequiredFields(
                pbGroup,
                AISProtobuf.Group.TREENAME_FIELD_NUMBER,
                AISProtobuf.Group.INDEXES_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Schema pbSchema) {
        checkRequiredFields(
                pbSchema,
                AISProtobuf.Schema.TABLES_FIELD_NUMBER,
                AISProtobuf.Schema.GROUPS_FIELD_NUMBER,
                AISProtobuf.Schema.CHARCOLL_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Table pbTable) {
        checkRequiredFields(
                pbTable,
                AISProtobuf.Table.ORDINAL_FIELD_NUMBER,
                AISProtobuf.Table.CHARCOLL_FIELD_NUMBER,
                AISProtobuf.Table.INDEXES_FIELD_NUMBER,
                AISProtobuf.Table.PARENTTABLE_FIELD_NUMBER,
                AISProtobuf.Table.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Table.PROTECTED_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Column pbColumn) {
        checkRequiredFields(
                pbColumn,
                AISProtobuf.Column.TYPEPARAM1_FIELD_NUMBER,
                AISProtobuf.Column.TYPEPARAM2_FIELD_NUMBER,
                AISProtobuf.Column.INITAUTOINC_FIELD_NUMBER,
                AISProtobuf.Column.CHARCOLL_FIELD_NUMBER,
                AISProtobuf.Column.DESCRIPTION_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Index pbIndex) {
        checkRequiredFields(
                pbIndex,
                AISProtobuf.Index.TREENAME_FIELD_NUMBER,
                AISProtobuf.Index.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Index.JOINTYPE_FIELD_NUMBER
        );
    }

    private static void checkRequiredFields(AbstractMessage message, int... descriptorsNotRequired) {
        Collection<Descriptors.FieldDescriptor> required = new ArrayList<Descriptors.FieldDescriptor>(message.getDescriptorForType().getFields());
        Collection<Descriptors.FieldDescriptor> actual = message.getAllFields().keySet();
        required.removeAll(actual);
        if(descriptorsNotRequired != null) {
            for(int descId : descriptorsNotRequired) {
                required.remove(message.getDescriptorForType().findFieldByNumber(descId));
            }
        }
        if(!required.isEmpty()) {
            Collection<String> names = new ArrayList<String>(required.size());
            for(Descriptors.FieldDescriptor desc : required) {
                names.add(desc.getFullName());
            }
            throw new IllegalStateException("Missing required fields: " + names);
        }
    }
}
