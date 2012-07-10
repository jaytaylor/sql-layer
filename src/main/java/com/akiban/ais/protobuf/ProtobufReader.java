/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.ProtobufReadException;
import com.akiban.util.GrowableByteBuffer;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ProtobufReader {
    private final AkibanInformationSchema destAIS;
    private final AISProtobuf.AkibanInformationSchema.Builder pbAISBuilder;
    private final NameGenerator nameGenerator = new DefaultNameGenerator();

    public ProtobufReader() {
        this(new AkibanInformationSchema());
    }

    public ProtobufReader(AkibanInformationSchema destAIS) {
        this(destAIS, AISProtobuf.AkibanInformationSchema.newBuilder());
    }

    public ProtobufReader(AkibanInformationSchema destAIS, AISProtobuf.AkibanInformationSchema.Builder pbAISBuilder) {
        this.destAIS = destAIS;
        this.pbAISBuilder = pbAISBuilder;
    }

    public AkibanInformationSchema getAIS() {
        return destAIS;
    }

    public ProtobufReader loadAIS() {
        // AIS has two fields (types, schemas) and both are optional
        AISProtobuf.AkibanInformationSchema pbAIS = pbAISBuilder.clone().build();
        loadTypes(pbAIS.getTypesList());
        loadSchemas(pbAIS.getSchemasList());
        return this;
    }

    public ProtobufReader loadBuffer(GrowableByteBuffer buffer) {
        loadFromBuffer(buffer);
        return this;
    }

    public AkibanInformationSchema loadAndGetAIS(GrowableByteBuffer buffer) {
        loadBuffer(buffer);
        loadAIS();
        return getAIS();
    }

    private void loadFromBuffer(GrowableByteBuffer buffer) {
        final String MESSAGE_NAME = AISProtobuf.AkibanInformationSchema.getDescriptor().getFullName();
        checkBuffer(buffer);
        final int serializedSize = buffer.getInt();
        final int initialPos = buffer.position();
        final int bufferSize = buffer.limit() - initialPos;
        if(bufferSize < serializedSize) {
            throw new ProtobufReadException(
                    MESSAGE_NAME,
                    String.format("Required size exceeded actual size: %d vs %d", serializedSize, bufferSize)
            );
        }
        CodedInputStream codedInput = CodedInputStream.newInstance(buffer.array(), buffer.position(), Math.min(serializedSize, bufferSize));
        try {
            pbAISBuilder.mergeFrom(codedInput);
            // Successfully consumed, update byte buffer
            buffer.position(initialPos + serializedSize);
        } catch(IOException e) {
            // CodedInputStream really only throws InvalidProtocolBufferException, but declares IOE
            throw new ProtobufReadException(MESSAGE_NAME, e.getMessage());
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
                    null,
                    null
            );
        }
    }

    private void loadSchemas(Collection<AISProtobuf.Schema> pbSchemas) {
        List<List<NewGroupInfo>> allNewGroups = new ArrayList<List<NewGroupInfo>>();

        for(AISProtobuf.Schema pbSchema : pbSchemas) {
            hasRequiredFields(pbSchema);

            List<NewGroupInfo> newGroups = loadGroups(pbSchema.getSchemaName(), pbSchema.getGroupsList());
            allNewGroups.add(newGroups);

            // Requires no tables, does not load indexes
            loadTables(pbSchema.getSchemaName(), pbSchema.getTablesList());
        }

        // Assume no ordering of schemas or tables, load joins second
        for(AISProtobuf.Schema pbSchema : pbSchemas) {
            loadTableJoins(pbSchema.getSchemaName(), pbSchema.getTablesList());
        }

        // Hook up groups, create group tables and indexes after all in place
        for(List<NewGroupInfo> newGroups : allNewGroups) {
            createGroupTablesAndIndexes(newGroups);
        }
    }
    
    private List<NewGroupInfo> loadGroups(String schema, Collection<AISProtobuf.Group> pbGroups) {
        List<NewGroupInfo> newGroups = new ArrayList<NewGroupInfo>();
        for(AISProtobuf.Group pbGroup : pbGroups) {
            hasRequiredFields(pbGroup);
            String rootTableName = pbGroup.getRootTableName();
            Group group = Group.create(destAIS, nameGenerator.generateGroupName(rootTableName));
            String treeName = pbGroup.hasTreeName() ? pbGroup.getTreeName() : null;
            newGroups.add(new NewGroupInfo(schema, group, pbGroup, treeName));
        }
        return newGroups;
    }

    private void createGroupTablesAndIndexes(List<NewGroupInfo> newGroups) {
        Set<Integer> currentIDs = new HashSet<Integer>();
        // Cannot assert ID uniqueness here, no such restriction from proto (e.g. from adapter)
        for(Table table : destAIS.getUserTables().values()) {
            currentIDs.add(table.getTableId());
        }
        for(Table table : destAIS.getGroupTables().values()) {
            currentIDs.add((table.getTableId()));
        }

        List<Join> joinsNeedingGroup = new ArrayList<Join>();
        
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
                    computeNewTableID(currentIDs, rootUserTable.getTableId() + 1)
            );
            newGroupInfo.group.setGroupTable(groupTable);
            groupTable.setGroup(newGroupInfo.group);
            groupTable.setTreeName(newGroupInfo.treeName);
            rootUserTable.setTreeName(newGroupInfo.treeName);
        }
        
        for(int i = 0; i < joinsNeedingGroup.size(); ++i) {
            Join join = joinsNeedingGroup.get(i);
            Group group = join.getParent().getGroup();
            join.setGroup(group);
            join.getChild().setGroup(group);
            join.getChild().setTreeName(join.getParent().getTreeName());
            joinsNeedingGroup.addAll(join.getChild().getCandidateChildJoins());
        }

        // Final pass (GI creation requires everything else be completed)
        for(NewGroupInfo newGroupInfo : newGroups) {
            loadGroupIndexes(newGroupInfo.group, newGroupInfo.pbGroup.getIndexesList());
        }
    }

    private void loadTables(String schema, Collection<AISProtobuf.Table> pbTables) {
        int generatedId = 1;
        for(AISProtobuf.Table pbTable : pbTables) {
            hasRequiredFields(pbTable);
            UserTable userTable = UserTable.create(
                    destAIS,
                    schema,
                    pbTable.getTableName(),
                    pbTable.hasTableId() ? pbTable.getTableId() : generatedId++
            );
            userTable.setCharsetAndCollation(getCharColl(pbTable.hasCharColl(), pbTable.getCharColl()));
            if(pbTable.hasVersion()) {
                userTable.setVersion(pbTable.getVersion());
            }
            loadColumns(userTable, pbTable.getColumnsList());
            loadTableIndexes(userTable, pbTable.getIndexesList());
        }
    }

    private void loadTableJoins(String schema, Collection<AISProtobuf.Table> pbTables) {
        for(AISProtobuf.Table pbTable : pbTables) {
            if(pbTable.hasParentTable()) {
                AISProtobuf.Join pbJoin = pbTable.getParentTable();
                hasRequiredFields(pbJoin);
                AISProtobuf.TableName pbParentName = pbJoin.getParentTable();
                hasRequiredFields(pbParentName);
                UserTable childTable = destAIS.getUserTable(schema, pbTable.getTableName());
                UserTable parentTable = destAIS.getUserTable(pbParentName.getSchemaName(), pbParentName.getTableName());

                if(parentTable == null) {
                    throw new ProtobufReadException(
                            pbTable.getDescriptorForType().getFullName(),
                            String.format("%s has unknown parentTable %s.%s", childTable.getName(),
                                          pbParentName.getSchemaName(), pbParentName.getTableName())
                    );
                }

                List<String> parentColNames = new ArrayList<String>();
                List<String> childColNames = new ArrayList<String>();
                for(AISProtobuf.JoinColumn pbJoinColumn : pbJoin.getColumnsList()) {
                    hasRequiredFields(pbJoinColumn);
                    parentColNames.add(pbJoinColumn.getParentColumn());
                    childColNames.add(pbJoinColumn.getChildColumn());
                }

                String joinName = nameGenerator.generateJoinName(parentTable.getName(), childTable.getName(), parentColNames, childColNames);
                Join join = Join.create(destAIS, joinName, parentTable, childTable);
                for(AISProtobuf.JoinColumn pbJoinColumn : pbJoin.getColumnsList()) {
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
            Column.create(
                    userTable,
                    pbColumn.getColumnName(),
                    pbColumn.getPosition(),
                    destAIS.getType(pbColumn.getTypeName()),
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
            if(pbIndex.hasTreeName()) {
                tableIndex.setTreeName(pbIndex.getTreeName());
            }
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
                    convertJoinTypeOrNull(pbIndex.hasJoinType(), pbIndex.getJoinType())
            );
            if(pbIndex.hasTreeName()) {
                groupIndex.setTreeName(pbIndex.getTreeName());
            }
            loadIndexColumns(null, groupIndex, pbIndex.getColumnsList());
        }
    }

    private void loadIndexColumns(UserTable table, Index index, Collection<AISProtobuf.IndexColumn> pbIndexColumns) {
        for(AISProtobuf.IndexColumn pbIndexColumn : pbIndexColumns) {
            hasRequiredFields(pbIndexColumn);
            if(pbIndexColumn.hasTableName()) {
                hasRequiredFields(pbIndexColumn.getTableName());
                table = destAIS.getUserTable(convertTableNameOrNull(true, pbIndexColumn.getTableName()));
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

    private static Index.JoinType convertJoinTypeOrNull(boolean isValid, AISProtobuf.JoinType joinType) {
        if(isValid) {
            switch(joinType) {
                case LEFT_OUTER_JOIN: return Index.JoinType.LEFT;
                case RIGHT_OUTER_JOIN: return Index.JoinType.RIGHT;
            }
            throw new ProtobufReadException(AISProtobuf.JoinType.getDescriptor().getFullName(),
                                            "Unsupported join type: " + joinType.name());
        }
        return null;
    }
    
    private static TableName convertTableNameOrNull(boolean isValid, AISProtobuf.TableName tableName) {
        if(isValid) {
            hasRequiredFields(tableName);
            return new TableName(tableName.getSchemaName(), tableName.getTableName());
        }
        return null;
    }

    private static int computeNewTableID(Set<Integer> currentIDs, int starting) {
        while(!currentIDs.add(starting)) {
            ++starting;
        }
        return starting;
    }

    /**
     * Check that a given message instance has all (application) required fields.
     * By default, this is all declared fields. See overloads for specific types.
     * @param message Message to check
     */
    private static void hasRequiredFields(AbstractMessage message) {
        requireAllFieldsExcept(message);
    }

    private static void hasRequiredFields(AISProtobuf.Group pbGroup) {
        requireAllFieldsExcept(
                pbGroup,
                AISProtobuf.Group.TREENAME_FIELD_NUMBER,
                AISProtobuf.Group.INDEXES_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Schema pbSchema) {
        requireAllFieldsExcept(
                pbSchema,
                AISProtobuf.Schema.TABLES_FIELD_NUMBER,
                AISProtobuf.Schema.GROUPS_FIELD_NUMBER,
                AISProtobuf.Schema.CHARCOLL_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Table pbTable) {
        requireAllFieldsExcept(
                pbTable,
                AISProtobuf.Table.TABLEID_FIELD_NUMBER,
                AISProtobuf.Table.ORDINAL_FIELD_NUMBER,
                AISProtobuf.Table.CHARCOLL_FIELD_NUMBER,
                AISProtobuf.Table.INDEXES_FIELD_NUMBER,
                AISProtobuf.Table.PARENTTABLE_FIELD_NUMBER,
                AISProtobuf.Table.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Table.PROTECTED_FIELD_NUMBER,
                AISProtobuf.Table.VERSION_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Column pbColumn) {
        requireAllFieldsExcept(
                pbColumn,
                AISProtobuf.Column.TYPEPARAM1_FIELD_NUMBER,
                AISProtobuf.Column.TYPEPARAM2_FIELD_NUMBER,
                AISProtobuf.Column.INITAUTOINC_FIELD_NUMBER,
                AISProtobuf.Column.CHARCOLL_FIELD_NUMBER,
                AISProtobuf.Column.DESCRIPTION_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Index pbIndex) {
        requireAllFieldsExcept(
                pbIndex,
                AISProtobuf.Index.TREENAME_FIELD_NUMBER,
                AISProtobuf.Index.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Index.JOINTYPE_FIELD_NUMBER
        );
    }

    private static void hasRequiredFieldsGI(AISProtobuf.Index pbIndex) {
        requireAllFieldsExcept(
                pbIndex,
                AISProtobuf.Index.TREENAME_FIELD_NUMBER,
                AISProtobuf.Index.DESCRIPTION_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.IndexColumn pbIndexColumn) {
        requireAllFieldsExcept(
                pbIndexColumn,
                AISProtobuf.IndexColumn.TABLENAME_FIELD_NUMBER
        );
    }

    private static void requireAllFieldsExcept(AbstractMessage message, int... fieldNumbersNotRequired) {
        Collection<Descriptors.FieldDescriptor> required = new ArrayList<Descriptors.FieldDescriptor>(message.getDescriptorForType().getFields());
        Collection<Descriptors.FieldDescriptor> actual = message.getAllFields().keySet();
        required.removeAll(actual);
        if(fieldNumbersNotRequired != null) {
            for(int fieldNumber : fieldNumbersNotRequired) {
                required.remove(message.getDescriptorForType().findFieldByNumber(fieldNumber));
            }
        }
        if(!required.isEmpty()) {
            Collection<String> names = new ArrayList<String>(required.size());
            for(Descriptors.FieldDescriptor desc : required) {
                names.add(desc.getName());
            }
            throw new ProtobufReadException(message.getDescriptorForType().getFullName(),
                                            "Missing required fields: " + names.toString());
        }
    }

    private static void checkBuffer(GrowableByteBuffer buffer) {
        assert buffer != null;
        assert buffer.hasArray() : "Array backed buffer required: " + buffer;
    }
    
    private static class NewGroupInfo {
        final String schema;
        final Group group;
        final AISProtobuf.Group pbGroup;
        final String treeName;

        public NewGroupInfo(String schema, Group group, AISProtobuf.Group pbGroup, String treeName) {
            this.schema = schema;
            this.group = group;
            this.pbGroup = pbGroup;
            this.treeName = treeName;
        }
    }
}
