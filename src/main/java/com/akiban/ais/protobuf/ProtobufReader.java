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
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.pt.PendingOSC;
import com.akiban.ais.util.TableChange;
import com.akiban.server.error.ProtobufReadException;
import com.akiban.server.geophile.Space;
import com.akiban.util.GrowableByteBuffer;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

            loadSequences (pbSchema.getSchemaName(), pbSchema.getSequencesList());

            List<NewGroupInfo> newGroups = loadGroups(pbSchema.getSchemaName(), pbSchema.getGroupsList());
            allNewGroups.add(newGroups);

            // Requires no tables, does not load indexes
            loadTables(pbSchema.getSchemaName(), pbSchema.getTablesList());
            loadViews(pbSchema.getSchemaName(), pbSchema.getViewsList());
            loadRoutines(pbSchema.getSchemaName(), pbSchema.getRoutinesList());
            loadSQLJJars(pbSchema.getSchemaName(), pbSchema.getSqljJarsList());
        }

        // Assume no ordering of schemas or tables, load joins and view refs second
        for(AISProtobuf.Schema pbSchema : pbSchemas) {
            loadTableJoins(pbSchema.getSchemaName(), pbSchema.getTablesList());
            loadExternalRoutines(pbSchema.getSchemaName(), pbSchema.getRoutinesList());
        }

        // Hook up groups, create group tables and indexes after all in place
        for(List<NewGroupInfo> newGroups : allNewGroups) {
            hookUpGroupAndCreateGroupIndexes(newGroups);
        }
    }
    
    private List<NewGroupInfo> loadGroups(String schema, Collection<AISProtobuf.Group> pbGroups) {
        List<NewGroupInfo> newGroups = new ArrayList<NewGroupInfo>();
        for(AISProtobuf.Group pbGroup : pbGroups) {
            hasRequiredFields(pbGroup);
            String rootTableName = pbGroup.getRootTableName();
            Group group = Group.create(destAIS, schema, rootTableName);
            group.setTreeName(pbGroup.hasTreeName() ? pbGroup.getTreeName() : null);
            newGroups.add(new NewGroupInfo(schema, group, pbGroup));
        }
        return newGroups;
    }

    private void hookUpGroupAndCreateGroupIndexes(List<NewGroupInfo> newGroups) {
        List<Join> joinsNeedingGroup = new ArrayList<Join>();
        
        for(NewGroupInfo newGroupInfo : newGroups) {
            String rootTableName = newGroupInfo.pbGroup.getRootTableName();
            UserTable rootUserTable = destAIS.getUserTable(newGroupInfo.schema, rootTableName);
            rootUserTable.setGroup(newGroupInfo.group);
            joinsNeedingGroup.addAll(rootUserTable.getCandidateChildJoins());
            newGroupInfo.group.setRootTable(rootUserTable);
        }
        
        for(int i = 0; i < joinsNeedingGroup.size(); ++i) {
            Join join = joinsNeedingGroup.get(i);
            Group group = join.getParent().getGroup();
            join.setGroup(group);
            join.getChild().setGroup(group);
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
            if (pbTable.hasPendingOSC()) {
                userTable.setPendingOSC(loadPendingOSC(pbTable.getPendingOSC()));
            }
        }
    }

    private void loadSequences (String schema, Collection<AISProtobuf.Sequence> pbSequences) {
        for (AISProtobuf.Sequence pbSequence : pbSequences) {
            hasRequiredFields(pbSequence);
            Sequence sequence = Sequence.create(
                    destAIS, 
                    schema,
                    pbSequence.getSequenceName(),
                    pbSequence.getStart(),
                    pbSequence.getIncrement(),
                    pbSequence.getMinValue(),
                    pbSequence.getMaxValue(),
                    pbSequence.getIsCycle());
            
            if (pbSequence.hasTreeName() ) {
                sequence.setTreeName(pbSequence.getTreeName());
            }
            if (pbSequence.hasAccumulator()) {
                sequence.setAccumIndex(pbSequence.getAccumulator());
            }
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
    
    private void loadViews(String schema, Collection<AISProtobuf.View> pbViews) {
        for(AISProtobuf.View pbView : pbViews) {
            hasRequiredFields(pbView);
            Map<TableName,Collection<String>> refs = 
                new HashMap<TableName,Collection<String>>();
            for(AISProtobuf.ColumnReference pbReference : pbView.getReferencesList()) {
                hasRequiredFields(pbReference);
                AISProtobuf.TableName pbTableName = pbReference.getTable();
                hasRequiredFields(pbTableName);
                TableName tableName = TableName.create(pbTableName.getSchemaName(), pbTableName.getTableName());
                Collection<String> columns = new HashSet<String>();
                Collection<String> old = refs.put(tableName, columns);
                assert (old == null);
                for(String colname : pbReference.getColumnsList()) {
                    boolean added = columns.add(colname);
                    assert added;
                }
            }
            View view = View.create(
                    destAIS,
                    schema,
                    pbView.getViewName(),
                    pbView.getDefinition(),
                    loadProperties(pbView.getDefinitionPropertiesList()),
                    refs
            );
            loadColumns(view, pbView.getColumnsList());
        }
    }

    private Properties loadProperties(Collection<AISProtobuf.Property> pbProperties) {
        Properties properties = new Properties();
        for(AISProtobuf.Property pbProperty : pbProperties) {
            hasRequiredFields(pbProperty);
            properties.put(pbProperty.getKey(), pbProperty.getValue());
        }
        return properties;
    }

    private void loadColumns(Columnar columnar, Collection<AISProtobuf.Column> pbColumns) {
        for(AISProtobuf.Column pbColumn : pbColumns) {
            hasRequiredFields(pbColumn);
            Long maxStorageSize = pbColumn.hasMaxStorageSize() ? pbColumn.getMaxStorageSize() : null;
            Integer prefixSize = pbColumn.hasPrefixSize() ? pbColumn.getPrefixSize() : null;
            Column column = Column.create(
                    columnar,
                    pbColumn.getColumnName(),
                    pbColumn.getPosition(),
                    destAIS.getType(pbColumn.getTypeName()), // TODO: types3, need to decide based on bundle
                    pbColumn.getIsNullable(),
                    pbColumn.hasTypeParam1() ? pbColumn.getTypeParam1() : null,
                    pbColumn.hasTypeParam2() ? pbColumn.getTypeParam2() : null,
                    pbColumn.hasInitAutoInc() ? pbColumn.getInitAutoInc() : null,
                    getCharColl(pbColumn.hasCharColl(), pbColumn.getCharColl()),
                    maxStorageSize,
                    prefixSize
            );
            if (pbColumn.hasDefaultIdentity()) {
                column.setDefaultIdentity(pbColumn.getDefaultIdentity());
            }
            if (pbColumn.hasSequence()) {
                TableName sequenceName = new TableName (pbColumn.getSequence().getSchemaName(), pbColumn.getSequence().getTableName());
                Sequence identityGenerator = getAIS().getSequence(sequenceName);
                column.setIdentityGenerator(identityGenerator);
            }
            if (pbColumn.hasDefaultValue()) {
                column.setDefaultValue(pbColumn.getDefaultValue());
            }
            // TODO: types3, pbColumn.getTypeBundleUUID()
            // TODO: types3, pbColumn.getTypeVersion()
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
            handleTreeName(tableIndex, pbIndex);
            handleSpatial(tableIndex, pbIndex);
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
            handleTreeName(groupIndex, pbIndex);
            handleSpatial(groupIndex, pbIndex);
            loadIndexColumns(null, groupIndex, pbIndex.getColumnsList());
        }
    }

    private void handleTreeName(Index index, AISProtobuf.Index pbIndex) {
        if(pbIndex.hasTreeName()) {
            index.setTreeName(pbIndex.getTreeName());
        }
    }

    private void handleSpatial(Index index, AISProtobuf.Index pbIndex) {
        if (pbIndex.hasIndexMethod()) {
            switch (pbIndex.getIndexMethod()) {
                case Z_ORDER_LAT_LON:
                    assert pbIndex.hasFirstSpatialArg() == pbIndex.hasDimensions();
                    int firstSpatialArg = 0;
                    int dimensions = Space.LAT_LON_DIMENSIONS;
                    if (pbIndex.hasFirstSpatialArg()) {
                        firstSpatialArg = pbIndex.getFirstSpatialArg();
                        dimensions = pbIndex.getDimensions();
                    }
                    index.markSpatial(firstSpatialArg, dimensions);
                    break;
            }
        }
    }

    private void loadIndexColumns(UserTable table, Index index, Collection<AISProtobuf.IndexColumn> pbIndexColumns) {
        for(AISProtobuf.IndexColumn pbIndexColumn : pbIndexColumns) {
            hasRequiredFields(pbIndexColumn);
            if(pbIndexColumn.hasTableName()) {
                hasRequiredFields(pbIndexColumn.getTableName());
                table = destAIS.getUserTable(convertTableNameOrNull(true, pbIndexColumn.getTableName()));
            }
            Integer indexedLength = null;
            if(pbIndexColumn.hasIndexedLength()) {
                indexedLength = pbIndexColumn.getIndexedLength();
            }
            IndexColumn.create(
                    index,
                    table != null ? table.getColumn(pbIndexColumn.getColumnName()) : null,
                    pbIndexColumn.getPosition(),
                    pbIndexColumn.getIsAscending(),
                    indexedLength
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

    private void loadRoutines(String schema, Collection<AISProtobuf.Routine> pbRoutines) {
        for (AISProtobuf.Routine pbRoutine : pbRoutines) {
            hasRequiredFields(pbRoutine);
            Routine routine = Routine.create(
                    destAIS,
                    schema,
                    pbRoutine.getRoutineName(),
                    pbRoutine.getLanguage(),
                    convertRoutineCallingConvention(pbRoutine.getCallingConvention())
            );
            loadParameters(routine, pbRoutine.getParametersList());
            if (pbRoutine.hasDefinition())
                routine.setDefinition(pbRoutine.getDefinition());
            if (pbRoutine.hasSqlAllowed())
                routine.setSQLAllowed(convertRoutineSQLAllowed(pbRoutine.getSqlAllowed()));
            if (pbRoutine.hasDynamicResultSets())
                routine.setDynamicResultSets(pbRoutine.getDynamicResultSets());
        }
    }
    
    private void loadParameters(Routine routine, Collection<AISProtobuf.Parameter> pbParameters) {
        for (AISProtobuf.Parameter pbParameter : pbParameters) {
            hasRequiredFields(pbParameter);
            Parameter parameter = Parameter.create(
                routine,
                pbParameter.getParameterName(),
                convertParameterDirection(pbParameter.getDirection()),
                destAIS.getType(pbParameter.getTypeName()),
                pbParameter.hasTypeParam1() ? pbParameter.getTypeParam1() : null,
                pbParameter.hasTypeParam2() ? pbParameter.getTypeParam2() : null
            );
        }
    }

    private static Routine.CallingConvention convertRoutineCallingConvention(AISProtobuf.RoutineCallingConvention callingConvention) {
        switch (callingConvention) {
        case JAVA:
        default:
            return Routine.CallingConvention.JAVA;
        case LOADABLE_PLAN: 
            return Routine.CallingConvention.LOADABLE_PLAN;
        case SQL_ROW: 
            return Routine.CallingConvention.SQL_ROW;
        case SCRIPT_FUNCTION_JAVA: 
            return Routine.CallingConvention.SCRIPT_FUNCTION_JAVA;
        case SCRIPT_BINDINGS: 
            return Routine.CallingConvention.SCRIPT_BINDINGS;
        }
    }

    private static Routine.SQLAllowed convertRoutineSQLAllowed(AISProtobuf.RoutineSQLAllowed sqlAllowed) {
        switch (sqlAllowed) {
        case MODIFIES_SQL_DATA:
        default:
            return Routine.SQLAllowed.MODIFIES_SQL_DATA;
        case READS_SQL_DATA:
            return Routine.SQLAllowed.READS_SQL_DATA;
        case CONTAINS_SQL:
            return Routine.SQLAllowed.CONTAINS_SQL;
        case NO_SQL:
            return Routine.SQLAllowed.NO_SQL;
        }
    }

    private static Parameter.Direction convertParameterDirection(AISProtobuf.ParameterDirection parameterDirection) {
        switch (parameterDirection) {
        case IN:
        default:
            return Parameter.Direction.IN;
        case OUT:
            return Parameter.Direction.OUT;
        case INOUT:
            return Parameter.Direction.INOUT;
        case RETURN:
            return Parameter.Direction.RETURN;
        }
    }

    private void loadSQLJJars(String schema, Collection<AISProtobuf.SQLJJar> pbJars) {
        for (AISProtobuf.SQLJJar pbJar : pbJars) {
            hasRequiredFields(pbJar);
            try {
                SQLJJar sqljJar = SQLJJar.create(destAIS, 
                                                 schema,
                                                 pbJar.getJarName(),
                                                 new URL(pbJar.getUrl()));
            }
            catch (MalformedURLException ex) {
                throw new ProtobufReadException(
                       pbJar.getDescriptorForType().getFullName(),
                       ex.toString()
                );
            }
        }        
    }

    private void loadExternalRoutines(String schema, Collection<AISProtobuf.Routine> pbRoutines) {
        for (AISProtobuf.Routine pbRoutine : pbRoutines) {
            if (pbRoutine.hasClassName() || pbRoutine.hasMethodName()) {
                Routine routine = destAIS.getRoutine(schema, pbRoutine.getRoutineName());
                if (routine == null) {
                    throw new ProtobufReadException(
                            pbRoutine.getDescriptorForType().getFullName(),
                            String.format("%s not found", pbRoutine.getRoutineName())
                    );
                }
                SQLJJar sqljJar = null;
                String className = null;
                String methodName = null;
                if (pbRoutine.hasJarName()) {
                    sqljJar = destAIS.getSQLJJar(pbRoutine.getJarName().getSchemaName(),
                                                 pbRoutine.getJarName().getTableName());
                    if (sqljJar == null) {
                        throw new ProtobufReadException(
                               pbRoutine.getDescriptorForType().getFullName(),
                               String.format("%s references JAR %s", pbRoutine.getRoutineName(), pbRoutine.getJarName())
                        );
                    }
                }
                if (pbRoutine.hasClassName())
                    className = pbRoutine.getClassName();
                if (pbRoutine.hasMethodName())
                    methodName = pbRoutine.getMethodName();
                routine.setExternalName(sqljJar, className, methodName);
            }
        }
    }

    private PendingOSC loadPendingOSC(AISProtobuf.PendingOSC pbPendingOSC) {
        hasRequiredFields(pbPendingOSC);
        List<TableChange> columnChanges = new ArrayList<TableChange>();
        loadPendingOSChanges(columnChanges, pbPendingOSC.getColumnChangesList());
        List<TableChange> indexChanges = new ArrayList<TableChange>();
        loadPendingOSChanges(indexChanges, pbPendingOSC.getIndexChangesList());
        PendingOSC osc = new PendingOSC(pbPendingOSC.getOriginalName(), columnChanges, indexChanges);
        if (pbPendingOSC.hasCurrentName())
            osc.setCurrentName(pbPendingOSC.getCurrentName());
        return osc;
    }
    
    private void loadPendingOSChanges(Collection<TableChange> changes, Collection<AISProtobuf.PendingOSChange> pbChanges) {
        for (AISProtobuf.PendingOSChange pbChange : pbChanges) {
            hasRequiredFields(pbChange);
            switch (pbChange.getType()) {
            case ADD:
                changes.add(TableChange.createAdd(pbChange.getNewName()));
                break;
            case DROP:
                changes.add(TableChange.createDrop(pbChange.getOldName()));
                break;
            case MODIFY:
                changes.add(TableChange.createModify(pbChange.getOldName(), pbChange.getNewName()));
                break;
            default:
                throw new ProtobufReadException(AISProtobuf.PendingOSChange.getDescriptor().getFullName(),
                                                "Unknown change type " + pbChange);
            }
        }
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
                AISProtobuf.Schema.SEQUENCES_FIELD_NUMBER,
                AISProtobuf.Schema.VIEWS_FIELD_NUMBER,
                AISProtobuf.Schema.ROUTINES_FIELD_NUMBER,
                AISProtobuf.Schema.SQLJJARS_FIELD_NUMBER,
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
                AISProtobuf.Table.VERSION_FIELD_NUMBER,
                AISProtobuf.Table.PENDINGOSC_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.View pbView) {
        requireAllFieldsExcept(
                pbView,
                AISProtobuf.View.COLUMNS_FIELD_NUMBER,
                AISProtobuf.View.DEFINITIONPROPERTIES_FIELD_NUMBER,
                AISProtobuf.View.REFERENCES_FIELD_NUMBER,
                AISProtobuf.View.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.View.PROTECTED_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Column pbColumn) {
        requireAllFieldsExcept(
                pbColumn,
                AISProtobuf.Column.TYPEPARAM1_FIELD_NUMBER,
                AISProtobuf.Column.TYPEPARAM2_FIELD_NUMBER,
                AISProtobuf.Column.INITAUTOINC_FIELD_NUMBER,
                AISProtobuf.Column.CHARCOLL_FIELD_NUMBER,
                AISProtobuf.Column.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Column.DEFAULTIDENTITY_FIELD_NUMBER,
                AISProtobuf.Column.SEQUENCE_FIELD_NUMBER,
                AISProtobuf.Column.MAXSTORAGESIZE_FIELD_NUMBER,
                AISProtobuf.Column.PREFIXSIZE_FIELD_NUMBER,
                AISProtobuf.Column.TYPEBUNDLEUUID_FIELD_NUMBER,
                AISProtobuf.Column.TYPEVERSION_FIELD_NUMBER,
                AISProtobuf.Column.DEFAULTVALUE_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Index pbIndex) {
        requireAllFieldsExcept(
                pbIndex,
                AISProtobuf.Index.TREENAME_FIELD_NUMBER,
                AISProtobuf.Index.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Index.JOINTYPE_FIELD_NUMBER,
                AISProtobuf.Index.INDEXMETHOD_FIELD_NUMBER,
                AISProtobuf.Index.FIRSTSPATIALARG_FIELD_NUMBER,
                AISProtobuf.Index.DIMENSIONS_FIELD_NUMBER
        );
    }

    private static void hasRequiredFieldsGI(AISProtobuf.Index pbIndex) {
        requireAllFieldsExcept(
                pbIndex,
                AISProtobuf.Index.TREENAME_FIELD_NUMBER,
                AISProtobuf.Index.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Index.INDEXMETHOD_FIELD_NUMBER,
                AISProtobuf.Index.FIRSTSPATIALARG_FIELD_NUMBER,
                AISProtobuf.Index.DIMENSIONS_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.IndexColumn pbIndexColumn) {
        requireAllFieldsExcept(
                pbIndexColumn,
                AISProtobuf.IndexColumn.TABLENAME_FIELD_NUMBER,
                AISProtobuf.IndexColumn.INDEXEDLENGTH_FIELD_NUMBER
        );
    }
    
    private static void hasRequiredFields (AISProtobuf.Sequence pbSequence) {
        requireAllFieldsExcept(
                pbSequence,
                AISProtobuf.Sequence.TREENAME_FIELD_NUMBER,
                AISProtobuf.Sequence.ACCUMULATOR_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Property pbProperty) {
        requireAllFieldsExcept(
                pbProperty
        );
    }

    private static void hasRequiredFields(AISProtobuf.ColumnReference pbReference) {
        requireAllFieldsExcept(
                pbReference,
                AISProtobuf.ColumnReference.COLUMNS_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Routine pbRoutine) {
        requireAllFieldsExcept(
                pbRoutine,
                AISProtobuf.Routine.PARAMETERS_FIELD_NUMBER,
                AISProtobuf.Routine.JARNAME_FIELD_NUMBER,
                AISProtobuf.Routine.CLASSNAME_FIELD_NUMBER,
                AISProtobuf.Routine.METHODNAME_FIELD_NUMBER,
                AISProtobuf.Routine.DEFINITION_FIELD_NUMBER,
                AISProtobuf.Routine.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Routine.PROTECTED_FIELD_NUMBER,
                AISProtobuf.Routine.SQLALLOWED_FIELD_NUMBER,
                AISProtobuf.Routine.DYNAMICRESULTSETS_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Parameter pbParameter) {
        requireAllFieldsExcept(
                pbParameter,
                AISProtobuf.Parameter.TYPEPARAM1_FIELD_NUMBER,
                AISProtobuf.Parameter.TYPEPARAM2_FIELD_NUMBER,
                AISProtobuf.Parameter.PARAMETERNAME_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.SQLJJar pbJar) {
        requireAllFieldsExcept(
                pbJar
        );
    }

    private static void hasRequiredFields(AISProtobuf.PendingOSC pbPendingOSC) {
        requireAllFieldsExcept(
                pbPendingOSC,
                AISProtobuf.PendingOSC.COLUMNCHANGES_FIELD_NUMBER,
                AISProtobuf.PendingOSC.INDEXCHANGES_FIELD_NUMBER,
                AISProtobuf.PendingOSC.CURRENTNAME_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.PendingOSChange pbChange) {
        requireAllFieldsExcept(
                pbChange,
                AISProtobuf.PendingOSChange.OLDNAME_FIELD_NUMBER,
                AISProtobuf.PendingOSChange.NEWNAME_FIELD_NUMBER
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

        public NewGroupInfo(String schema, Group group, AISProtobuf.Group pbGroup) {
            this.schema = schema;
            this.group = group;
            this.pbGroup = pbGroup;
        }
    }
}
