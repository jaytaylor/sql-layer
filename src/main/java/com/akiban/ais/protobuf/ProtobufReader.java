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
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtobufReader {
    private final ByteBuffer buffer;
    private final AkibanInformationSchema destAIS;
    private AISProtobuf.AkibanInformationSchema pbAIS;
    private Map<TableName,Group> rootToGroupMap = new HashMap<TableName,Group>();
    
    public ProtobufReader(ByteBuffer buffer) {
        assert buffer.hasArray() : buffer;
        this.buffer = buffer;
        this.destAIS = new AkibanInformationSchema();
    }
    
    public AkibanInformationSchema load()
    {
        loadFromBuffer();
        // All fields (currently types, schemas) are optional
        loadTypes(pbAIS.getTypesList());
        loadSchemas(pbAIS.getSchemasList());
        return destAIS;
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
    
    private void loadTypes(Collection<AISProtobuf.Type> pbTypes) {
        for(AISProtobuf.Type pbType : pbTypes) {
            hasRequiredFields(pbType);
            Type.create(
                    destAIS,
                    pbType.getTypeName(),
                    pbType.getParameters(),
                    pbType.getFixedSize(),
                    pbType.getMaxSizeBytes(),
                    null,
                    null
            );
        }
    }

    private void loadSchemas(Collection<AISProtobuf.Schema> pbSchemas) {
        List<List<NewGroupInfo>> allNewGroups = new ArrayList<List<NewGroupInfo>>();

        // Assume no ordering, create groups after all tables exist
        for(AISProtobuf.Schema pbSchema : pbSchemas) {
            hasRequiredFields(pbSchema);
            List<NewGroupInfo> newGroups = loadGroups(pbSchema.getSchemaName(), pbSchema.getGroupsList());
            allNewGroups.add(newGroups);
        }

        for(AISProtobuf.Schema pbSchema : pbSchemas) {
            loadTables( pbSchema.getSchemaName(), pbSchema.getTablesList());
        }
        
        for(List<NewGroupInfo> newGroups : allNewGroups) {
            createGroupTablesAndIndexes(newGroups);
        }
    }
    
    private List<NewGroupInfo> loadGroups(String schema, Collection<AISProtobuf.Group> pbGroups) {
        List<NewGroupInfo> newGroups = new ArrayList<NewGroupInfo>();
        for(AISProtobuf.Group pbGroup : pbGroups) {
            hasRequiredFields(pbGroup);
            String rootTableName = pbGroup.getRootTableName();
            Group group = Group.create(destAIS, rootTableName);
            newGroups.add(new NewGroupInfo(schema, group, pbGroup));
            
            rootToGroupMap.put(TableName.create(schema, rootTableName), group);
        }
        return newGroups;
    }

    private void createGroupTablesAndIndexes(List<NewGroupInfo> newGroups) {
        int maxTableId = 1;
        for(Table table : destAIS.getUserTables().values()) {
            maxTableId = Math.max(maxTableId, table.getTableId());
        }
        for(Table table : destAIS.getGroupTables().values()) {
            maxTableId = Math.max(maxTableId, table.getTableId());
        }

        List<Join> joinsNeedingGroup = new ArrayList<Join>();
        
        DefaultNameGenerator nameGenerator = new DefaultNameGenerator();
        for(NewGroupInfo newGroupInfo : newGroups) {
            String rootTableName = newGroupInfo.pbGroup.getRootTableName();
            UserTable rootUserTable = destAIS.getUserTable(newGroupInfo.schema, rootTableName);
            rootUserTable.setTreeName(newGroupInfo.pbGroup.getTreeName());
            rootUserTable.setGroup(newGroupInfo.group);
            joinsNeedingGroup.addAll(rootUserTable.getCandidateChildJoins());

            GroupTable groupTable = GroupTable.create(
                    destAIS,
                    newGroupInfo.schema,
                    nameGenerator.generateGroupTableName(rootTableName),
                    ++maxTableId
            );
            newGroupInfo.group.setGroupTable(groupTable);
        }
        
        for(int i = 0; i < joinsNeedingGroup.size(); ++i) {
            Join join = joinsNeedingGroup.get(i);
            Group group = join.getParent().getGroup();
            join.setGroup(group);
            join.getChild().setGroup(group);
            joinsNeedingGroup.addAll(join.getChild().getCandidateChildJoins());
        }

        // Final pass (GI creation requires everything else be created)
        for(NewGroupInfo newGroupInfo : newGroups) {
            loadGroupIndexes(newGroupInfo.group, newGroupInfo.pbGroup.getIndexesList());
        }
    }

    private void loadTables(String schema, Collection<AISProtobuf.Table> pbTables) {
        for(AISProtobuf.Table pbTable : pbTables) {
            hasRequiredFields(pbTable);
            UserTable userTable = UserTable.create(destAIS, schema, pbTable.getTableName(), pbTable.getTableId());
            userTable.setCharsetAndCollation(getCharColl(pbTable.hasCharColl(), pbTable.getCharColl()));
            loadColumns(userTable, pbTable.getColumnsList());
            loadTableIndexes(userTable, pbTable.getIndexesList());
        }

        // Assume no ordering of table list, load joins second
        for(AISProtobuf.Table pbTable : pbTables) {
            if(pbTable.hasParentTable()) {
                AISProtobuf.Join pbJoin = pbTable.getParentTable();
                hasRequiredFields(pbJoin);
                AISProtobuf.TableName pbParentName = pbJoin.getParentTable();
                hasRequiredFields(pbParentName);
                UserTable parentTable = destAIS.getUserTable(pbParentName.getSchemaName(), pbParentName.getTableName());
                UserTable childTable = destAIS.getUserTable(schema, pbTable.getTableName());

                String joinName = parentTable.getName() + "/" + childTable.getName();
                Join join = Join.create(destAIS, joinName, parentTable, childTable);
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
    
    private void loadColumns(UserTable userTable, Collection<AISProtobuf.Column> pbColumns) {
        for(AISProtobuf.Column pbColumn : pbColumns) {
            hasRequiredFields(pbColumn);
            Type type = destAIS.getType(pbColumn.getTypeName());
            Column.create(
                    userTable,
                    pbColumn.getColumnName(),
                    pbColumn.getPosition(),
                    type,
                    pbColumn.getIsNullable(),
                    pbColumn.hasTypeParam1() ? pbColumn.getTypeParam1() : null,
                    pbColumn.hasTypeParam2() ? pbColumn.getTypeParam2() : null,
                    pbColumn.hasInitAutoInc() ? pbColumn.getInitAutoInc() : null,
                    getCharColl(pbColumn.hasCharColl(), pbColumn.getCharColl())
            );
        }
    }
    
    private void loadTableIndexes(UserTable userTable, Collection<AISProtobuf.Index> pbIndexes) {
        for(AISProtobuf.Index pbIndex : pbIndexes) {
            hasRequiredFields(pbIndex);
            TableIndex tableIndex = TableIndex.create(
                    destAIS,
                    userTable,
                    pbIndex.getIndexName(),
                    pbIndex.getIndexId(),
                    pbIndex.getIsUnique(),
                    getIndexConstraint(pbIndex)
            );
            loadIndexColumns(userTable, tableIndex, pbIndex.getColumnsList());
        }
    }

    private void loadGroupIndexes(Group group, Collection<AISProtobuf.Index> pbIndexes) {
        for(AISProtobuf.Index pbIndex : pbIndexes) {
            hasRequiredFieldsGI(pbIndex);
            GroupIndex groupIndex = GroupIndex.create(
                    destAIS,
                    group,
                    pbIndex.getIndexName(),
                    pbIndex.getIndexId(),
                    pbIndex.getIsUnique(),
                    getIndexConstraint(pbIndex),
                    getJoinType(pbIndex.hasJoinType(), pbIndex.getJoinType())
            );
            loadIndexColumns(null, groupIndex, pbIndex.getColumnsList());
        }
    }

    private void loadIndexColumns(UserTable table, Index index, Collection<AISProtobuf.IndexColumn> pbIndexColumns) {
        for(AISProtobuf.IndexColumn pbIndexColumn : pbIndexColumns) {
            hasRequiredFields(pbIndexColumn);
            if(pbIndexColumn.hasTableName()) {
                hasRequiredFields(pbIndexColumn.getTableName());
                table = destAIS.getUserTable(getTableName(true, pbIndexColumn.getTableName()));
            }
            IndexColumn.create(
                    index,
                    table != null ? table.getColumn(pbIndexColumn.getColumnName()) : null,
                    pbIndexColumn.getPosition(),
                    pbIndexColumn.getIsAscending(),
                    null /* indexedLength not in proto */
            );
        }
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

    private static Index.JoinType getJoinType(boolean isValid, AISProtobuf.JoinType joinType) {
        if(isValid) {
            switch(joinType) {
                case LEFT_OUTER_JOIN: return Index.JoinType.LEFT;
                case RIGHT_OUTER_JOIN: return Index.JoinType.RIGHT;
            }
        }
        return null;
    }
    
    private static TableName getTableName(boolean isValid, AISProtobuf.TableName tableName) {
        if(isValid) {
            hasRequiredFields(tableName);
            return new TableName(tableName.getSchemaName(), tableName.getTableName());
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

    private static void hasRequiredFieldsGI(AISProtobuf.Index pbIndex) {
        checkRequiredFields(
                pbIndex,
                AISProtobuf.Index.TREENAME_FIELD_NUMBER,
                AISProtobuf.Index.DESCRIPTION_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.IndexColumn pbIndexColumn) {
        checkRequiredFields(
                pbIndexColumn,
                AISProtobuf.IndexColumn.TABLENAME_FIELD_NUMBER
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
    
    private static class NewGroupInfo {
        final String schema;
        final Group group;
        final AISProtobuf.Group pbGroup;

        public NewGroupInfo(String schema, Group group, AISProtobuf.Group pbGroup) {
            this.schema = schema;
            this.group = group;
            this.pbGroup = pbGroup;
        }
    }
}
