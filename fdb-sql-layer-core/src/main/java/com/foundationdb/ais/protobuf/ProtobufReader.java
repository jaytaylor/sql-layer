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

package com.foundationdb.ais.protobuf;

import com.foundationdb.ais.model.*;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.server.error.ProtobufReadException;
import com.foundationdb.server.spatial.Spatial;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.service.TypesRegistry;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class ProtobufReader {
    private final TypesRegistry typesRegistry;
    private final StorageFormatRegistry storageFormatRegistry;
    private final AkibanInformationSchema destAIS;
    private final AISProtobuf.AkibanInformationSchema.Builder pbAISBuilder;
    private final NameGenerator nameGenerator = new DefaultNameGenerator();

    public ProtobufReader(TypesRegistry typesRegistry, StorageFormatRegistry storageFormatRegistry) {
        this(typesRegistry, storageFormatRegistry, new AkibanInformationSchema());
    }

    public ProtobufReader(TypesRegistry typesRegistry, StorageFormatRegistry storageFormatRegistry, AkibanInformationSchema destAIS) {
        this(typesRegistry, storageFormatRegistry, destAIS, AISProtobuf.AkibanInformationSchema.newBuilder());
    }

    public ProtobufReader(TypesRegistry typesRegistry, StorageFormatRegistry storageFormatRegistry, AkibanInformationSchema destAIS, AISProtobuf.AkibanInformationSchema.Builder pbAISBuilder) {
        this.typesRegistry = typesRegistry;
        this.storageFormatRegistry = storageFormatRegistry;
        this.destAIS = destAIS;
        this.pbAISBuilder = pbAISBuilder;
    }

    public AkibanInformationSchema getAIS() {
        return destAIS;
    }

    public ProtobufReader loadAIS() {
        // AIS has two fields (types, schemas) and both are optional
        AISProtobuf.AkibanInformationSchema pbAIS = pbAISBuilder.clone().build();
        loadSchemas(pbAIS.getSchemasList());
        return this;
    }

    public ProtobufReader loadBuffer(ByteBuffer buffer) {
        loadFromBuffer(buffer);
        return this;
    }

    public AkibanInformationSchema loadAndGetAIS(ByteBuffer buffer) {
        loadBuffer(buffer);
        loadAIS();
        return getAIS();
    }

    private void loadFromBuffer(ByteBuffer buffer) {
        final String MESSAGE_NAME = AISProtobuf.AkibanInformationSchema.getDescriptor().getFullName();
        checkBuffer(buffer);
        final int serializedSize = buffer.getInt();
        final int initialPos = buffer.position();
        final int bufferSize = buffer.limit() - initialPos;
        if(bufferSize < serializedSize) {
            throw new ProtobufReadException(MESSAGE_NAME, "Buffer corrupt, serialized size greater than remaining");
        }
        CodedInputStream codedInput = CodedInputStream.newInstance(buffer.array(), buffer.position(), Math.min(serializedSize, bufferSize));
        try {
            pbAISBuilder.mergeFrom(codedInput, storageFormatRegistry.getExtensionRegistry());
            // Successfully consumed, update byte buffer
            buffer.position(initialPos + serializedSize);
        } catch(IOException e) {
            // CodedInputStream really only throws InvalidProtocolBufferException, but declares IOE
            throw new ProtobufReadException(MESSAGE_NAME, e.getMessage());
        }
    }

    private void loadSchemas(Collection<AISProtobuf.Schema> pbSchemas) {
        List<List<NewGroupInfo>> allNewGroups = new ArrayList<>();

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
        // Likewise full text indexes and foreign keys.
        for(AISProtobuf.Schema pbSchema : pbSchemas) {
            loadFullTextIndexes(pbSchema.getSchemaName(), pbSchema.getTablesList());
            loadForeignKeys(pbSchema.getSchemaName(), pbSchema.getTablesList());
        }        
    }
    
    private List<NewGroupInfo> loadGroups(String schema, Collection<AISProtobuf.Group> pbGroups) {
        List<NewGroupInfo> newGroups = new ArrayList<>();
        for(AISProtobuf.Group pbGroup : pbGroups) {
            hasRequiredFields(pbGroup);
            String rootTableName = pbGroup.getRootTableName();
            Group group = Group.create(destAIS, schema, rootTableName);
            StorageDescription storage = null;
            if (pbGroup.hasStorage()) {
                storage = storageFormatRegistry.readProtobuf(pbGroup.getStorage(), group);
            }
            group.setStorageDescription(storage);
            newGroups.add(new NewGroupInfo(schema, group, pbGroup));
        }
        return newGroups;
    }

    private void hookUpGroupAndCreateGroupIndexes(List<NewGroupInfo> newGroups) {
        List<Join> joinsNeedingGroup = new ArrayList<>();
        
        for(NewGroupInfo newGroupInfo : newGroups) {
            String rootTableName = newGroupInfo.pbGroup.getRootTableName();
            Table rootTable = destAIS.getTable(newGroupInfo.schema, rootTableName);
            rootTable.setGroup(newGroupInfo.group);
            joinsNeedingGroup.addAll(rootTable.getCandidateChildJoins());
            newGroupInfo.group.setRootTable(rootTable);
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
            Table table = Table.create(
                    destAIS,
                    schema,
                    pbTable.getTableName(),
                    pbTable.hasTableId() ? pbTable.getTableId() : generatedId++
            );
            if(pbTable.hasOrdinal()) {
                table.setOrdinal(pbTable.getOrdinal());
            }
            if (pbTable.hasUuid()) {
                table.setUuid(convertUUID(pbTable.getUuid()));
            }
            if(pbTable.hasCharColl()) {
                AISProtobuf.CharCollation pbCharAndCol = pbTable.getCharColl();
                hasRequiredFields(pbCharAndCol);
                table.setCharsetAndCollation(pbCharAndCol.getCharacterSetName(),
                                             pbCharAndCol.getCollationOrderName());
            }
            if(pbTable.hasVersion()) {
                table.setVersion(pbTable.getVersion());
            }
            loadColumns(table, pbTable.getColumnsList());
            loadTableIndexes(table, pbTable.getIndexesList());
            if (pbTable.hasPendingOSC()) {
                table.setPendingOSC(loadPendingOSC(pbTable.getPendingOSC()));
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
            
            StorageDescription storage = null;
            if (pbSequence.hasStorage()) {
                storage = storageFormatRegistry.readProtobuf(pbSequence.getStorage(), sequence);
            }
            sequence.setStorageDescription(storage);
        }
    }

    private void loadTableJoins(String schema, Collection<AISProtobuf.Table> pbTables) {
        for(AISProtobuf.Table pbTable : pbTables) {
            if(pbTable.hasParentTable()) {
                AISProtobuf.Join pbJoin = pbTable.getParentTable();
                hasRequiredFields(pbJoin);
                AISProtobuf.TableName pbParentName = pbJoin.getParentTable();
                hasRequiredFields(pbParentName);
                Table childTable = destAIS.getTable(schema, pbTable.getTableName());
                Table parentTable = destAIS.getTable(pbParentName.getSchemaName(), pbParentName.getTableName());

                if(parentTable == null) {
                    throw new ProtobufReadException(
                            pbTable.getDescriptorForType().getFullName(),
                            String.format("%s has unknown parentTable %s.%s", childTable.getName(),
                                          pbParentName.getSchemaName(), pbParentName.getTableName())
                    );
                }

                List<String> parentColNames = new ArrayList<>();
                List<String> childColNames = new ArrayList<>();
                for(AISProtobuf.JoinColumn pbJoinColumn : pbJoin.getColumnsList()) {
                    hasRequiredFields(pbJoinColumn);
                    parentColNames.add(pbJoinColumn.getParentColumn());
                    childColNames.add(pbJoinColumn.getChildColumn());
                }

                Join join = Join.create(destAIS, pbJoin.getConstraintName().getTableName(), parentTable, childTable);
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
                new HashMap<>();
            for(AISProtobuf.ColumnReference pbReference : pbView.getReferencesList()) {
                hasRequiredFields(pbReference);
                AISProtobuf.TableName pbTableName = pbReference.getTable();
                hasRequiredFields(pbTableName);
                TableName tableName = TableName.create(pbTableName.getSchemaName(), pbTableName.getTableName());
                Collection<String> columns = new HashSet<>();
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
            String charset = null, collation = null;
            if (pbColumn.hasCharColl()) {
                AISProtobuf.CharCollation pbCharAndCol = pbColumn.getCharColl();
                hasRequiredFields(pbCharAndCol);
                charset = pbCharAndCol.getCharacterSetName();
                collation = pbCharAndCol.getCollationOrderName();
            }
            TInstance type = typesRegistry.getType(
                    convertUUID(pbColumn.getTypeBundleUUID()),
                    pbColumn.getTypeName(),
                    pbColumn.getTypeVersion(),
                    pbColumn.hasTypeParam1() ? pbColumn.getTypeParam1() : null,
                    pbColumn.hasTypeParam2() ? pbColumn.getTypeParam2() : null,
                    charset, collation,
                    pbColumn.getIsNullable(),
                    columnar.getName().getSchemaName(), columnar.getName().getTableName(),
                    pbColumn.getColumnName()
            );
            Column column = Column.create(
                    columnar,
                    pbColumn.getColumnName(),
                    pbColumn.getPosition(),
                    type,
                    maxStorageSize,
                    prefixSize
            );
            if (pbColumn.hasUuid()) {
                if (pbColumn.hasUuid()) {
                    column.setUuid(convertUUID(pbColumn.getUuid()));
                }
            }
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
            if (pbColumn.hasDefaultFunction()) {
                column.setDefaultFunction(pbColumn.getDefaultFunction());
            }
        }
    }
    
    private void loadTableIndexes(Table table, Collection<AISProtobuf.Index> pbIndexes) {
        for(AISProtobuf.Index pbIndex : pbIndexes) {
            hasRequiredFields(pbIndex);
            TableIndex tableIndex = TableIndex.create(
                    destAIS,
                    table,
                    pbIndex.getIndexName(),
                    pbIndex.getIndexId(),
                    pbIndex.getIsUnique(),
                    pbIndex.getIsPK(),
                    getConstraintName(pbIndex)
            );
            handleStorage(tableIndex, pbIndex);
            handleSpatial(tableIndex, pbIndex);
            loadIndexColumns(table, tableIndex, pbIndex.getColumnsList());
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
                    pbIndex.getIsPK(),
                    getConstraintName(pbIndex), 
                    convertJoinTypeOrNull(pbIndex.hasJoinType(), pbIndex.getJoinType())
            );
            handleStorage(groupIndex, pbIndex);
            handleSpatial(groupIndex, pbIndex);
            loadIndexColumns(null, groupIndex, pbIndex.getColumnsList());
        }
    }

    private void loadFullTextIndexes(String schema, Collection<AISProtobuf.Table> pbTables) {
        for(AISProtobuf.Table pbTable : pbTables) {
            for(AISProtobuf.Index pbIndex : pbTable.getFullTextIndexesList()) {
                hasRequiredFields(pbIndex);
                Table table = destAIS.getTable(schema, pbTable.getTableName());
                FullTextIndex textIndex = FullTextIndex.create(
                        destAIS,
                        table,
                        pbIndex.getIndexName(),
                        pbIndex.getIndexId(),
                        getConstraintName(pbIndex)
                        );
                handleStorage(textIndex, pbIndex);
                handleSpatial(textIndex, pbIndex);
                loadIndexColumns(table, textIndex, pbIndex.getColumnsList());
            }
        }        
    }
    
    private TableName getConstraintName(AISProtobuf.Index pbIndex){
        TableName constraintName = null; 
        if (pbIndex.hasConstraintName()) {
            constraintName = new TableName(pbIndex.getConstraintName().getSchemaName(), pbIndex.getConstraintName().getTableName());
        }
        return constraintName;
    }

    private void loadForeignKeys(String schema, Collection<AISProtobuf.Table> pbTables) {
        for(AISProtobuf.Table pbTable : pbTables) {
            for(AISProtobuf.ForeignKey pbFK : pbTable.getForeignKeysList()) {
                hasRequiredFields(pbFK);
                Table referencingTable = destAIS.getTable(schema, pbTable.getTableName());
                Table referencedTable = destAIS.getTable(convertTableNameOrNull(true, pbFK.getReferencedTable()));
                List<Column> referencingColumns = getForeignKeyColumns(referencingTable, pbFK.getReferencingColumnsList());
                List<Column> referencedColumns = getForeignKeyColumns(referencedTable, pbFK.getReferencedColumnsList());
                ForeignKey.create(destAIS,
                                  pbFK.getConstraintName(),
                                  referencingTable,
                                  referencingColumns,
                                  referencedTable,
                                  referencedColumns,
                                  convertForeignKeyAction(pbFK.getOnDelete()),
                                  convertForeignKeyAction(pbFK.getOnUpdate()),
                                  pbFK.getDeferrable(),
                                  pbFK.getInitiallyDeferred());
            }
        }
    }

    private List<Column> getForeignKeyColumns(Table table, List<String> names) {
        List<Column> result = new ArrayList<>();
        for(String name : names) {
            result.add(table.getColumn(name));
        }
        return result;
    }

    private static ForeignKey.Action convertForeignKeyAction(AISProtobuf.ForeignKeyAction action) {
        switch (action) {
        case NO_ACTION:
        default:
            return ForeignKey.Action.NO_ACTION;
        case RESTRICT:
            return ForeignKey.Action.RESTRICT;
        case CASCADE:
            return ForeignKey.Action.CASCADE;
        case SET_NULL:
            return ForeignKey.Action.SET_NULL;
        case SET_DEFAULT:
            return ForeignKey.Action.SET_DEFAULT;
        }
    }

    private void handleStorage(Index index, AISProtobuf.Index pbIndex) {
        StorageDescription storage = null;
        if (pbIndex.hasStorage()) {
            storage = storageFormatRegistry.readProtobuf(pbIndex.getStorage(), index);
        }
        index.setStorageDescription(storage);
    }

    private void handleSpatial(Index index, AISProtobuf.Index pbIndex) {
        if (pbIndex.hasIndexMethod()) {
            switch (pbIndex.getIndexMethod()) {
                case GEO_LAT_LON:
                    assert pbIndex.hasFirstSpatialArg() == pbIndex.hasDimensions();
                    int firstSpatialArg = 0;
                    int lastSpatialArg = 0;
                    if (pbIndex.hasFirstSpatialArg()) {
                        firstSpatialArg = pbIndex.getFirstSpatialArg();
                        if (pbIndex.hasLastSpatialArg()) {
                            lastSpatialArg = pbIndex.getLastSpatialArg();
                        } else {
                            // Schema created before spatial object support, when spatial meant just lat/lon.
                            lastSpatialArg = firstSpatialArg + pbIndex.getDimensions() - 1;
                        }
                    }
                    index.markSpatial(firstSpatialArg, lastSpatialArg - firstSpatialArg + 1, pbIndex.getFunctionName());
                    break;
            }
        }
    }

    private void loadIndexColumns(Table table, Index index, Collection<AISProtobuf.IndexColumn> pbIndexColumns) {
        for(AISProtobuf.IndexColumn pbIndexColumn : pbIndexColumns) {
            hasRequiredFields(pbIndexColumn);
            if(pbIndexColumn.hasTableName()) {
                hasRequiredFields(pbIndexColumn.getTableName());
                table = destAIS.getTable(convertTableNameOrNull(true, pbIndexColumn.getTableName()));
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
            if (pbRoutine.hasDeterministic())
                routine.setDeterministic(pbRoutine.getDeterministic());
            if (pbRoutine.hasCalledOnNullInput())
                routine.setCalledOnNullInput(pbRoutine.getCalledOnNullInput());
            if (pbRoutine.hasVersion()) {
                routine.setVersion(pbRoutine.getVersion());
            }
        }
    }
    
    private void loadParameters(Routine routine, Collection<AISProtobuf.Parameter> pbParameters) {
        for (AISProtobuf.Parameter pbParameter : pbParameters) {
            hasRequiredFields(pbParameter);
            TInstance type = typesRegistry.getType(
                    convertUUID(pbParameter.getTypeBundleUUID()),
                    pbParameter.getTypeName(),
                    pbParameter.getTypeVersion(),
                    pbParameter.hasTypeParam1() ? pbParameter.getTypeParam1() : null,
                    pbParameter.hasTypeParam2() ? pbParameter.getTypeParam2() : null,
                    true,
                    routine.getName().getSchemaName(), routine.getName().getTableName(),
                    pbParameter.getParameterName()
            );
            Parameter parameter = Parameter.create(
                routine,
                pbParameter.getParameterName(),
                convertParameterDirection(pbParameter.getDirection()),
                    type
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
        case SCRIPT_FUNCTION_JSON: 
            return Routine.CallingConvention.SCRIPT_FUNCTION_JSON;
        case SCRIPT_BINDINGS_JSON: 
            return Routine.CallingConvention.SCRIPT_BINDINGS_JSON;
        case SCRIPT_LIBRARY: 
            return Routine.CallingConvention.SCRIPT_LIBRARY;
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
                if (pbJar.hasVersion()) {
                    sqljJar.setVersion(pbJar.getVersion());
                }
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
        List<TableChange> columnChanges = new ArrayList<>();
        loadPendingOSChanges(columnChanges, pbPendingOSC.getColumnChangesList());
        List<TableChange> indexChanges = new ArrayList<>();
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
    
    private static UUID convertUUID(AISProtobuf.UUID pbUuid) {
        return new UUID(pbUuid.getMostSignificantBits(),
                        pbUuid.getLeastSignificantBits());
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
                AISProtobuf.Group.INDEXES_FIELD_NUMBER,
                AISProtobuf.Group.STORAGE_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Join pbJoin) {
        requireAllFieldsExcept(
                pbJoin,
                AISProtobuf.Join.CONSTRAINTNAME_FIELD_NUMBER
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
                AISProtobuf.Table.PENDINGOSC_FIELD_NUMBER,
                AISProtobuf.Table.UUID_FIELD_NUMBER,
                AISProtobuf.Table.FULLTEXTINDEXES_FIELD_NUMBER,
                AISProtobuf.Table.ORDINAL_FIELD_NUMBER,
                AISProtobuf.Table.FOREIGNKEYS_FIELD_NUMBER
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
                AISProtobuf.Column.DEFAULTVALUE_FIELD_NUMBER,
                AISProtobuf.Column.UUID_FIELD_NUMBER,
                AISProtobuf.Column.DEFAULTFUNCTION_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Index pbIndex) {
        requireAllFieldsExcept(
                pbIndex,
                AISProtobuf.Index.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Index.JOINTYPE_FIELD_NUMBER,
                AISProtobuf.Index.INDEXMETHOD_FIELD_NUMBER,
                AISProtobuf.Index.FIRSTSPATIALARG_FIELD_NUMBER,
                AISProtobuf.Index.LASTSPATIALARG_FIELD_NUMBER,
                AISProtobuf.Index.DIMENSIONS_FIELD_NUMBER,
                AISProtobuf.Index.STORAGE_FIELD_NUMBER,
                AISProtobuf.Index.CONSTRAINTNAME_FIELD_NUMBER,
                AISProtobuf.Index.FUNCTIONNAME_FIELD_NUMBER
        );
    }

    private static void hasRequiredFieldsGI(AISProtobuf.Index pbIndex) {
        requireAllFieldsExcept(
                pbIndex,
                AISProtobuf.Index.DESCRIPTION_FIELD_NUMBER,
                AISProtobuf.Index.INDEXMETHOD_FIELD_NUMBER,
                AISProtobuf.Index.FIRSTSPATIALARG_FIELD_NUMBER,
                AISProtobuf.Index.LASTSPATIALARG_FIELD_NUMBER,
                AISProtobuf.Index.DIMENSIONS_FIELD_NUMBER,
                AISProtobuf.Index.STORAGE_FIELD_NUMBER,
                AISProtobuf.Index.CONSTRAINTNAME_FIELD_NUMBER,
                AISProtobuf.Index.FUNCTIONNAME_FIELD_NUMBER
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
                AISProtobuf.Sequence.ACCUMULATOR_FIELD_NUMBER,
                AISProtobuf.Sequence.STORAGE_FIELD_NUMBER
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
                AISProtobuf.Routine.DYNAMICRESULTSETS_FIELD_NUMBER,
                AISProtobuf.Routine.DETERMINISTIC_FIELD_NUMBER,
                AISProtobuf.Routine.CALLEDONNULLINPUT_FIELD_NUMBER,
                AISProtobuf.Routine.VERSION_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.Parameter pbParameter) {
        requireAllFieldsExcept(
                pbParameter,
                AISProtobuf.Parameter.PARAMETERNAME_FIELD_NUMBER,
                AISProtobuf.Parameter.TYPEPARAM1_FIELD_NUMBER,
                AISProtobuf.Parameter.TYPEPARAM2_FIELD_NUMBER
        );
    }

    private static void hasRequiredFields(AISProtobuf.SQLJJar pbJar) {
        requireAllFieldsExcept(
                pbJar,
                AISProtobuf.SQLJJar.VERSION_FIELD_NUMBER
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

    private static void hasRequiredFields(AISProtobuf.ForeignKey pbFK) {
        requireAllFieldsExcept(
                pbFK,
                AISProtobuf.ForeignKey.CONSTRAINTNAME_FIELD_NUMBER,
                AISProtobuf.ForeignKey.ONDELETE_FIELD_NUMBER,
                AISProtobuf.ForeignKey.ONUPDATE_FIELD_NUMBER,
                AISProtobuf.ForeignKey.DEFERRABLE_FIELD_NUMBER,
                AISProtobuf.ForeignKey.INITIALLYDEFERRED_FIELD_NUMBER
        );
    }

    private static void requireAllFieldsExcept(AbstractMessage message, int... fieldNumbersNotRequired) {
        Collection<Descriptors.FieldDescriptor> required = new ArrayList<>(message.getDescriptorForType().getFields());
        Collection<Descriptors.FieldDescriptor> actual = message.getAllFields().keySet();
        required.removeAll(actual);
        if(fieldNumbersNotRequired != null) {
            for(int fieldNumber : fieldNumbersNotRequired) {
                required.remove(message.getDescriptorForType().findFieldByNumber(fieldNumber));
            }
        }
        if(!required.isEmpty()) {
            Collection<String> names = new ArrayList<>(required.size());
            for(Descriptors.FieldDescriptor desc : required) {
                names.add(desc.getName());
            }
            throw new ProtobufReadException(message.getDescriptorForType().getFullName(),
                                            "Missing required fields: " + names.toString());
        }
    }

    private static void checkBuffer(ByteBuffer buffer) {
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
