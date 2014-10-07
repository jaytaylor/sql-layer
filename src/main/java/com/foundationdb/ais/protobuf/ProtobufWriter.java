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
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.ProtobufWriteException;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public class ProtobufWriter {
    public static interface WriteSelector {
        Columnar getSelected(Columnar columnar);
        /** Called for any table.getGroup() when getSelected(table) is not null */
        boolean isSelected(Group group);
        /** Called for all parent joins where getSelected(Table) is not null **/
        boolean isSelected(Join parentJoin);
        /** Called for all GroupIndexes and all table indexes where getSelected(Table) is not null **/
        boolean isSelected(Index index);
        boolean isSelected(Sequence sequence);
        boolean isSelected(Routine routine);
        boolean isSelected(SQLJJar sqljJar);
        boolean isSelected(ForeignKey foreignKey);
    }

    public static final WriteSelector ALL_SELECTOR = new WriteSelector() {
        @Override
        public Columnar getSelected(Columnar columnar) {
            return columnar;
        }

        @Override
        public boolean isSelected(Group group) {
            return true;
        }

        @Override
        public boolean isSelected(Join join) {
            return true;
        }

        @Override
        public boolean isSelected(Index index) {
            return true;
        }

        @Override
        public boolean isSelected(Sequence sequence) {
            return true;
        }

        @Override
        public boolean isSelected(Routine routine) {
            return true;
        }

        @Override
        public boolean isSelected(SQLJJar sqljJar) {
            return true;
        }

        @Override
        public boolean isSelected(ForeignKey foreignKey) {
            return true;
        }
    };

    public static abstract class TableFilterSelector implements WriteSelector {
        @Override
        public boolean isSelected(Index index) {
            return true;
        }

        @Override
        public boolean isSelected(Group group) {
            return true;
        }

        @Override
        public boolean isSelected(Join join) {
            return true;
        }

        @Override
        public boolean isSelected(Sequence sequence) {
            return true;
        }

        @Override
        public boolean isSelected(Routine routine) {
            return true;
        }

        @Override
        public boolean isSelected(SQLJJar sqljJar) {
            return true;
        }

        @Override
        public boolean isSelected(ForeignKey foreignKey) {
            return true;
        }
    }

    public static abstract class TableSelector extends TableFilterSelector {
        public abstract boolean isSelected(Columnar columnar);

        @Override
        public Columnar getSelected(Columnar columnar) {
            return isSelected(columnar) ? columnar : null;
        }
    }

    public static class SingleSchemaSelector implements WriteSelector {
        private final String schemaName;

        public SingleSchemaSelector(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        @Override
        public Columnar getSelected(Columnar columnar) {
            return schemaName.equals(columnar.getName().getSchemaName()) ? columnar : null;
        }

        @Override
        public boolean isSelected(Group group) {
            return true;
        }

        @Override
        public boolean isSelected(Join join) {
            return true;
        }

        @Override
        public boolean isSelected(Index index) {
            return true;
        }
        
        @Override 
        public boolean isSelected(Sequence sequence) {
            return schemaName.equals(sequence.getSequenceName().getSchemaName());
        }
        
        @Override 
        public boolean isSelected(Routine routine) {
            return schemaName.equals(routine.getName().getSchemaName());
        }

        @Override
        public boolean isSelected(SQLJJar sqljJar) {
            return schemaName.equals(sqljJar.getName().getSchemaName());
        }

        @Override
        public boolean isSelected(ForeignKey foreignKey) {
            return true;
        }
    }


    private AISProtobuf.AkibanInformationSchema pbAIS;
    private final WriteSelector selector;


    public ProtobufWriter() {
        this(ALL_SELECTOR);
    }

    public ProtobufWriter(WriteSelector selector) {
        this.selector = selector;
    }

    /** Convert the given AIS into Protobuf classes. Returned instance also available via {@link #getPBAIS()}. */
    public AISProtobuf.AkibanInformationSchema save(AkibanInformationSchema ais) {
        AISProtobuf.AkibanInformationSchema.Builder aisBuilder = AISProtobuf.AkibanInformationSchema.newBuilder();

        if(selector instanceof SingleSchemaSelector) {
            Schema schema = ais.getSchema(((SingleSchemaSelector) selector).getSchemaName());
            if(schema != null) {
                writeSchema(aisBuilder, schema, selector);
            }
        } else {
            for(Schema schema : ais.getSchemas().values()) {
                writeSchema(aisBuilder, schema, selector);
            }
        }

        pbAIS = aisBuilder.build();
        return pbAIS;
    }

    public AISProtobuf.AkibanInformationSchema getPBAIS() {
        return pbAIS;
    }

    /** Size buffer needs for calling {@link #serialize} */
    public int getBufferSize() {
        assert pbAIS != null;
        return pbAIS.getSerializedSize() + 4; // + int
    }

    /** Serialize into <code>buffer</code>. Buffer must have array and enough space, see {@link #getBufferSize}. */
    public void serialize(ByteBuffer buffer) {
        assert pbAIS != null;
        assert buffer.hasArray() : "Array required";
        String MESSAGE_NAME = AISProtobuf.AkibanInformationSchema.getDescriptor().getFullName();
        int requiredSize = getBufferSize();
        int serializedSize = requiredSize - 4; // Added by requiredBufferSize
        int bufferSize = buffer.limit() - buffer.position();
        if(bufferSize < requiredSize) {
            throw new ProtobufWriteException(MESSAGE_NAME, "Required size exceeds buffer size");
        }
        buffer.putInt(serializedSize);
        int bufferPos = buffer.position();
        bufferSize = buffer.limit() - bufferPos;
        CodedOutputStream codedOutput = CodedOutputStream.newInstance(buffer.array(), bufferPos, bufferSize);
        try {
            pbAIS.writeTo(codedOutput);
            // Successfully written, update backing buffer info
            buffer.position(bufferPos + serializedSize);
        } catch(IOException e) {
            // CodedOutputStream really only throws OutOfSpace exception, but declares IOE
            throw new ProtobufWriteException(MESSAGE_NAME, e.getMessage());
        }
    }

    private static void writeSchema(AISProtobuf.AkibanInformationSchema.Builder aisBuilder, Schema schema, WriteSelector selector) {
        AISProtobuf.Schema.Builder schemaBuilder = AISProtobuf.Schema.newBuilder();
        schemaBuilder.setSchemaName(schema.getName());
        // .setCharColl(convertCharAndCol(schema.getCharsetName(), schema.getCollationName()));
        boolean isEmpty = true;

        // Write groups into same schema as root table
        for(Table table : schema.getTables().values()) {
            table = (Table)selector.getSelected(table);
            if(table != null) {
                Group group = table.getGroup();
                if((table.getParentJoin() == null) && (group != null) && selector.isSelected(group)) {
                    writeGroup(schemaBuilder, table.getGroup(), selector);
                }
                writeTable(schemaBuilder, table, selector);
                isEmpty = false;
            }
        }
        
        for(View view : schema.getViews().values()) {
            view = (View)selector.getSelected(view);
            if(view != null) {
                writeView(schemaBuilder, view);
                isEmpty = false;
            }
        }

        for (Sequence sequence : schema.getSequences().values()) {
            if (selector.isSelected (sequence)) { 
                writeSequence(schemaBuilder, sequence);
                isEmpty = false;
            }
        }

        for (Routine routine : schema.getRoutines().values()) {
            if (selector.isSelected(routine)) { 
                writeRoutine(schemaBuilder, routine);
                isEmpty = false;
            }
        }

        for (SQLJJar sqljJar : schema.getSQLJJars().values()) {
            if (selector.isSelected(sqljJar)) { 
                writeSQLJJar(schemaBuilder, sqljJar);
                isEmpty = false;
            }
        }

        if(!isEmpty) {
            aisBuilder.addSchemas(schemaBuilder.build());
        }
    }

    private static void writeGroup(AISProtobuf.Schema.Builder schemaBuilder, Group group, WriteSelector selector) {
        final Table rootTable = group.getRoot();
        AISProtobuf.Group.Builder groupBuilder = AISProtobuf.Group.newBuilder().
                setRootTableName(rootTable.getName().getTableName());
        if(group.getStorageDescription() != null) {
            AISProtobuf.Storage.Builder storageBuilder = AISProtobuf.Storage.newBuilder();
            group.getStorageDescription().writeProtobuf(storageBuilder);
            groupBuilder.setStorage(storageBuilder.build());
        }

        for(Index index : group.getIndexes()) {
            if(selector.isSelected(index)) {
                writeGroupIndex(groupBuilder, index);
            }
        }

        schemaBuilder.addGroups(groupBuilder.build());
    }

    private static void writeTable(AISProtobuf.Schema.Builder schemaBuilder, Table table, WriteSelector selector) {
        AISProtobuf.Table.Builder tableBuilder = AISProtobuf.Table.newBuilder();
        tableBuilder
            .setTableName(table.getName().getTableName())
            .setTableId(table.getTableId());
        
        AISProtobuf.CharCollation cac = convertCharAndCol(table.getCharsetName(),
                                                          table.getCollationName());
        if (cac != null) {
            tableBuilder.setCharColl(cac);
        }

        if(table.getOrdinal() != null) {
            tableBuilder.setOrdinal(table.getOrdinal());
        }

        AISProtobuf.UUID tableUuid = convertUUID(table.getUuid());
        if (tableUuid != null) {
            tableBuilder.setUuid(tableUuid);
        }

        if(table.hasVersion()) {
            tableBuilder.setVersion(table.getVersion());
        }

        for(Column column : table.getColumnsIncludingInternal()) {
            writeColumn(tableBuilder, column);
        }

        for(Index index : table.getIndexesIncludingInternal()) {
            if(selector.isSelected(index)) {
                writeTableIndex(tableBuilder, index);
            }
        }

        Join join = table.getParentJoin();
        if((join != null) && selector.isSelected(join)) {
            final Table parent = join.getParent();
            AISProtobuf.Join.Builder joinBuilder = AISProtobuf.Join.newBuilder();
            TableName joinConstraintName = join.getConstraintName();
            joinBuilder.setConstraintName(AISProtobuf.TableName.newBuilder().
                    setSchemaName(joinConstraintName.getSchemaName()).setTableName(joinConstraintName.getTableName())).
                    setParentTable(AISProtobuf.TableName.newBuilder().
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

        if (table.getPendingOSC() != null) {
            writePendingOSC(tableBuilder, table.getPendingOSC());
        }

        for(FullTextIndex index : table.getOwnFullTextIndexes()) {
            if(selector.isSelected(index)) {
                writeFullTextIndex(tableBuilder, index);
            }
        }

        for(ForeignKey fk : table.getReferencingForeignKeys()) {
            if(selector.isSelected(fk)) {
                writeForeignKey(tableBuilder, fk);
            }
        }

        schemaBuilder.addTables(tableBuilder.build());
    }

    private static void writeColumn(AISProtobuf.Table.Builder tableBuilder, Column column) {
        tableBuilder.addColumns(writeColumnCommon(column));
    }

    private static void writeView(AISProtobuf.Schema.Builder schemaBuilder, View view) {
        AISProtobuf.View.Builder viewBuilder = AISProtobuf.View.newBuilder();
        viewBuilder.
                setViewName(view.getName().getTableName()).
                setDefinition(view.getDefinition());
               // Not yet in AIS: description, protected

        for(Column column : view.getColumnsIncludingInternal()) {
            writeColumn(viewBuilder, column);
        }

        for(String key : view.getDefinitionProperties().stringPropertyNames()) {
            String value = view.getDefinitionProperties().getProperty(key);
            viewBuilder.addDefinitionProperties(AISProtobuf.Property.newBuilder().
                                                setKey(key).setValue(value).
                                                build());
        }
        
        for(Map.Entry<TableName,Collection<String>> entry : view.getTableColumnReferences().entrySet()) {
            AISProtobuf.ColumnReference.Builder referenceBuilder = 
                AISProtobuf.ColumnReference.newBuilder().
                setTable(AISProtobuf.TableName.newBuilder().
                         setSchemaName(entry.getKey().getSchemaName()).
                         setTableName(entry.getKey().getTableName()).
                         build());
            for (String column : entry.getValue()) {
                referenceBuilder.addColumns(column);
            }
            viewBuilder.addReferences(referenceBuilder.build());
        }

        schemaBuilder.addViews(viewBuilder.build());
    }

    private static void writeColumn(AISProtobuf.View.Builder viewBuilder, Column column) {
        viewBuilder.addColumns(writeColumnCommon(column));
    }

    private static AISProtobuf.Column writeColumnCommon(Column column) {
        AISProtobuf.Column.Builder columnBuilder = AISProtobuf.Column.newBuilder().
                setColumnName(column.getName()).
                setTypeName(column.getTypeName()).
                setTypeBundleUUID(convertUUID(column.getTypeBundleUUID())).
                setTypeVersion(column.getTypeVersion()).
                setIsNullable(column.getNullable()).
                setPosition(column.getPosition());

        if(column.hasCharsetAndCollation()) {
            columnBuilder.setCharColl(convertCharAndCol(column.getCharsetName(), column.getCollationName()));
        }

        AISProtobuf.UUID columnUuid = convertUUID(column.getUuid());
        if (columnUuid != null) {
            columnBuilder.setUuid(columnUuid);
        }

        if(column.getTypeParameter1() != null) {
            columnBuilder.setTypeParam1(column.getTypeParameter1());
        }
        if(column.getTypeParameter2() != null) {
            columnBuilder.setTypeParam2(column.getTypeParameter2());
        }
        if(column.getInitialAutoIncrementValue() != null) {
            columnBuilder.setInitAutoInc(column.getInitialAutoIncrementValue());
        }
        
        if (column.getDefaultIdentity() != null) {
            columnBuilder.setDefaultIdentity (column.getDefaultIdentity());
        }
        
        if (column.getIdentityGenerator() != null) {
            columnBuilder.setSequence(AISProtobuf.TableName.newBuilder()
                    .setSchemaName(column.getIdentityGenerator().getSequenceName().getSchemaName())
                    .setTableName(column.getIdentityGenerator().getSequenceName().getTableName())
                    .build());
        }
        Long maxStorage = column.getMaxStorageSizeWithoutComputing();
        if(maxStorage != null) {
            columnBuilder.setMaxStorageSize(maxStorage);
        }
        Integer prefix = column.getPrefixSizeWithoutComputing();
        if(prefix != null) {
            columnBuilder.setPrefixSize(prefix);
        }
        if(column.getDefaultValue() != null) {
            columnBuilder.setDefaultValue(column.getDefaultValue());
        }
        if(column.getDefaultFunction() != null) {
            columnBuilder.setDefaultFunction(column.getDefaultFunction());
        }
        return columnBuilder.build();
    }

    private static AISProtobuf.Index writeIndexCommon(Index index, boolean withTableName) {
        final IndexName indexName = index.getIndexName();
        AISProtobuf.Index.Builder indexBuilder = AISProtobuf.Index.newBuilder();
        AISProtobuf.TableName.Builder tableNameBuilder = AISProtobuf.TableName.newBuilder();
        AISProtobuf.TableName constraintName;
        indexBuilder.
                setIndexName(indexName.getName()).
                setIndexId(index.getIndexId()).
                setIsPK(index.isPrimaryKey()).
                setIsUnique(index.isUnique()).
                setIndexMethod(convertIndexMethod(index.getIndexMethod()));

        if (index.getConstraintName() != null) {
            tableNameBuilder.
                    setSchemaName(index.getConstraintName().getSchemaName()).
                    setTableName(index.getConstraintName().getTableName());
            constraintName = tableNameBuilder.build();
            indexBuilder.setConstraintName(constraintName);
        }
        // Not yet in AIS: description        
        if(index.isGroupIndex()) {
            indexBuilder.setJoinType(convertJoinType(index.getJoinType()));
        }
        if(index.getStorageDescription() != null) {
            AISProtobuf.Storage.Builder storageBuilder = AISProtobuf.Storage.newBuilder();
            index.getStorageDescription().writeProtobuf(storageBuilder);
            indexBuilder.setStorage(storageBuilder.build());
        }
        if (index.getIndexMethod() == Index.IndexMethod.Z_ORDER_LAT_LON) {
            indexBuilder.
                    setFirstSpatialArg(index.firstSpatialArgument()).
                    setLastSpatialArg(index.lastSpatialArgument()).
                    setDimensions(index.dimensions());

        }

        for(IndexColumn indexColumn : index.getKeyColumns()) {
            writeIndexColumn(indexBuilder, indexColumn, withTableName);
        }

        return indexBuilder.build();
    }

    private static void writeTableIndex(AISProtobuf.Table.Builder tableBuilder, Index index) {
        tableBuilder.addIndexes(writeIndexCommon(index, false));
    }

    private static void writeGroupIndex(AISProtobuf.Group.Builder groupBuilder, Index index) {
        groupBuilder.addIndexes(writeIndexCommon(index, true));
    }

    private static void writeFullTextIndex(AISProtobuf.Table.Builder tableBuilder, FullTextIndex index) {
        tableBuilder.addFullTextIndexes(writeIndexCommon(index, true));
    }

    private static void writeIndexColumn(AISProtobuf.Index.Builder indexBuilder, IndexColumn indexColumn, boolean withTableName) {
        AISProtobuf.IndexColumn.Builder indexColumnBuilder = AISProtobuf.IndexColumn.newBuilder().
                setColumnName(indexColumn.getColumn().getName()).
                setIsAscending(indexColumn.isAscending()).
                setPosition(indexColumn.getPosition());

        if(indexColumn.getIndexedLength() != null) {
            indexColumnBuilder.setIndexedLength(indexColumn.getIndexedLength());
        }
        
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

    private static AISProtobuf.UUID convertUUID(UUID uuid) {
        if (uuid == null)
            return null;
        return AISProtobuf.UUID.newBuilder().
            setMostSignificantBits(uuid.getMostSignificantBits()).
            setLeastSignificantBits(uuid.getLeastSignificantBits()).
            build();
    }

    private static AISProtobuf.JoinType convertJoinType(Index.JoinType joinType) {
        switch(joinType) {
            case LEFT: return AISProtobuf.JoinType.LEFT_OUTER_JOIN;
            case RIGHT: return AISProtobuf.JoinType.RIGHT_OUTER_JOIN;
        }
        throw new ProtobufWriteException(AISProtobuf.Join.getDescriptor().getFullName(),
                                         "No match for Index.JoinType "+joinType.name());
    }

    private static AISProtobuf.CharCollation convertCharAndCol(String charset, String collation) {
        if ((charset == null) && (collation == null))
            return null;
        return AISProtobuf.CharCollation.newBuilder().
                setCharacterSetName(charset).
                setCollationOrderName(collation).
                build();
    }
    
    private static AISProtobuf.IndexMethod convertIndexMethod(Index.IndexMethod indexMethod) {
        switch (indexMethod) {
        case NORMAL: 
        default:
            return AISProtobuf.IndexMethod.NORMAL;
        case Z_ORDER_LAT_LON: 
            return AISProtobuf.IndexMethod.Z_ORDER_LAT_LON;
        case FULL_TEXT: 
            return AISProtobuf.IndexMethod.FULL_TEXT;
        }
    }

    private static void writePendingOSC(AISProtobuf.Table.Builder tableBuilder, PendingOSC pendingOSC) {
        AISProtobuf.PendingOSC.Builder oscBuilder = AISProtobuf.PendingOSC.newBuilder();
        oscBuilder.setOriginalName(pendingOSC.getOriginalName());
        for (TableChange columnChange : pendingOSC.getColumnChanges()) {
            oscBuilder.addColumnChanges(writePendingOSChange(columnChange));
        }
        for (TableChange indexChange : pendingOSC.getIndexChanges()) {
            oscBuilder.addIndexChanges(writePendingOSChange(indexChange));
        }
        if (pendingOSC.getCurrentName() != null)
            oscBuilder.setCurrentName(pendingOSC.getCurrentName());
        tableBuilder.setPendingOSC(oscBuilder.build());
    }

    private static AISProtobuf.PendingOSChange writePendingOSChange(TableChange tableChange) {
        AISProtobuf.PendingOSChange.Builder oscBuilder = AISProtobuf.PendingOSChange.newBuilder();
        switch (tableChange.getChangeType()) {
        case ADD:
            oscBuilder.setType(AISProtobuf.PendingOSChangeType.ADD);
            oscBuilder.setNewName(tableChange.getNewName());
            break;
        case DROP:
            oscBuilder.setType(AISProtobuf.PendingOSChangeType.DROP);
            oscBuilder.setOldName(tableChange.getOldName());
            break;
        case MODIFY:
            oscBuilder.setType(AISProtobuf.PendingOSChangeType.MODIFY);
            oscBuilder.setOldName(tableChange.getOldName());
            oscBuilder.setNewName(tableChange.getNewName());
            break;
        }
        return oscBuilder.build();
    }

    private static void writeForeignKey(AISProtobuf.Table.Builder tableBuilder, ForeignKey fk) {
        AISProtobuf.ForeignKey.Builder fkBuilder = AISProtobuf.ForeignKey.newBuilder();
        fkBuilder
            .setConstraintName(fk.getConstraintName().getTableName())   
            .setReferencedTable(AISProtobuf.TableName.newBuilder().
                    setSchemaName(fk.getReferencedTable().getName().getSchemaName()).
                    setTableName(fk.getReferencedTable().getName().getTableName()).
                    build());

        for(Column fkColumn : fk.getReferencingColumns()) {
            fkBuilder.addReferencingColumns(fkColumn.getName());
        }
        for(Column fkColumn : fk.getReferencedColumns()) {
            fkBuilder.addReferencedColumns(fkColumn.getName());
        }

        fkBuilder.setOnDelete(convertForeignKeyAction(fk.getDeleteAction()));
        fkBuilder.setOnUpdate(convertForeignKeyAction(fk.getUpdateAction()));
        fkBuilder.setDeferrable(fk.isDeferrable());
        fkBuilder.setInitiallyDeferred(fk.isInitiallyDeferred());

        tableBuilder.addForeignKeys(fkBuilder.build());
    }

    private static AISProtobuf.ForeignKeyAction convertForeignKeyAction(ForeignKey.Action action) {
        switch (action) {
        case NO_ACTION:
        default:
            return AISProtobuf.ForeignKeyAction.NO_ACTION;
        case RESTRICT:
            return AISProtobuf.ForeignKeyAction.RESTRICT;
        case CASCADE:
            return AISProtobuf.ForeignKeyAction.CASCADE;
        case SET_NULL:
            return AISProtobuf.ForeignKeyAction.SET_NULL;
        case SET_DEFAULT:
            return AISProtobuf.ForeignKeyAction.SET_DEFAULT;
        }
    }

    private static void writeSequence (AISProtobuf.Schema.Builder schemaBuilder, Sequence sequence) {
        AISProtobuf.Sequence.Builder sequenceBuilder = AISProtobuf.Sequence.newBuilder()
                .setSequenceName(sequence.getSequenceName().getTableName())
                .setStart(sequence.getStartsWith())
                .setIncrement(sequence.getIncrement())
                .setMinValue(sequence.getMinValue())
                .setMaxValue(sequence.getMaxValue())
                .setIsCycle(sequence.isCycle());
        if(sequence.getStorageDescription() != null) {
            AISProtobuf.Storage.Builder storageBuilder = AISProtobuf.Storage.newBuilder();
            sequence.getStorageDescription().writeProtobuf(storageBuilder);
            sequenceBuilder.setStorage(storageBuilder.build());
        }
        schemaBuilder.addSequences (sequenceBuilder.build());
    }

    private static void writeRoutine(AISProtobuf.Schema.Builder schemaBuilder, Routine routine) {
        AISProtobuf.Routine.Builder routineBuilder = AISProtobuf.Routine.newBuilder()
            .setRoutineName(routine.getName().getTableName())
            .setLanguage(routine.getLanguage())
            .setCallingConvention(convertRoutineCallingConvention(routine.getCallingConvention()));
        for (Parameter parameter : routine.getParameters()) {
            writeParameter(routineBuilder, parameter);
        }
        if (routine.getReturnValue() != null) {
            writeParameter(routineBuilder, routine.getReturnValue());
        }
        SQLJJar sqljJar = routine.getSQLJJar();
        if (sqljJar != null) {
            routineBuilder.setJarName(AISProtobuf.TableName.newBuilder()
                                      .setSchemaName(sqljJar.getName().getSchemaName())
                                      .setTableName(sqljJar.getName().getTableName())
                                      .build());
        }
        if (routine.getClassName() != null)
            routineBuilder.setClassName(routine.getClassName());
        if (routine.getMethodName() != null)
            routineBuilder.setMethodName(routine.getMethodName());
        if (routine.getDefinition() != null)
            routineBuilder.setDefinition(routine.getDefinition());
        if (routine.getSQLAllowed() != null)
            routineBuilder.setSqlAllowed(convertRoutineSQLAllowed(routine.getSQLAllowed()));
        if (routine.getDynamicResultSets() > 0)
            routineBuilder.setDynamicResultSets(routine.getDynamicResultSets());
        if (routine.isDeterministic())
            routineBuilder.setDeterministic(routine.isDeterministic());
        if (routine.isCalledOnNullInput())
            routineBuilder.setCalledOnNullInput(routine.isCalledOnNullInput());
        if (routine.getVersion() > 0)
            routineBuilder.setVersion(routine.getVersion());
        schemaBuilder.addRoutines(routineBuilder.build());
    }

    private static void writeParameter(AISProtobuf.Routine.Builder routineBuilder, Parameter parameter) {
        AISProtobuf.Parameter.Builder parameterBuilder = AISProtobuf.Parameter.newBuilder()
            .setDirection(convertParameterDirection(parameter.getDirection()))
            .setTypeName(parameter.getTypeName())
            .setTypeBundleUUID(convertUUID(parameter.getTypeBundleUUID()))
            .setTypeVersion(parameter.getTypeVersion());
        if (parameter.getTypeParameter1() != null) {
            parameterBuilder.setTypeParam1(parameter.getTypeParameter1());
        }
        if (parameter.getTypeParameter2() != null) {
            parameterBuilder.setTypeParam2(parameter.getTypeParameter2());
        }
        if (parameter.getName() != null) {
            parameterBuilder.setParameterName(parameter.getName());
        }
        routineBuilder.addParameters(parameterBuilder.build());
    }

    private static AISProtobuf.RoutineCallingConvention convertRoutineCallingConvention(Routine.CallingConvention callingConvention) {
        switch (callingConvention) {
        case JAVA:
        default:
            return AISProtobuf.RoutineCallingConvention.JAVA;
        case LOADABLE_PLAN:
            return AISProtobuf.RoutineCallingConvention.LOADABLE_PLAN;
        case SQL_ROW: 
            return AISProtobuf.RoutineCallingConvention.SQL_ROW;
        case SCRIPT_FUNCTION_JAVA: 
            return AISProtobuf.RoutineCallingConvention.SCRIPT_FUNCTION_JAVA;
        case SCRIPT_BINDINGS: 
            return AISProtobuf.RoutineCallingConvention.SCRIPT_BINDINGS;
        case SCRIPT_FUNCTION_JSON: 
            return AISProtobuf.RoutineCallingConvention.SCRIPT_FUNCTION_JSON;
        case SCRIPT_BINDINGS_JSON: 
            return AISProtobuf.RoutineCallingConvention.SCRIPT_BINDINGS_JSON;
        case SCRIPT_LIBRARY: 
            return AISProtobuf.RoutineCallingConvention.SCRIPT_LIBRARY;
        }
    }

    private static AISProtobuf.RoutineSQLAllowed convertRoutineSQLAllowed(Routine.SQLAllowed sqlAllowed) {
        switch (sqlAllowed) {
        case MODIFIES_SQL_DATA:
        default:
            return AISProtobuf.RoutineSQLAllowed.MODIFIES_SQL_DATA;
        case READS_SQL_DATA:
            return AISProtobuf.RoutineSQLAllowed.READS_SQL_DATA;
        case CONTAINS_SQL:
            return AISProtobuf.RoutineSQLAllowed.CONTAINS_SQL;
        case NO_SQL:
            return AISProtobuf.RoutineSQLAllowed.NO_SQL;
        }
    }

    private static AISProtobuf.ParameterDirection convertParameterDirection(Parameter.Direction parameterDirection) {
        switch (parameterDirection) {
        case IN:
        default:
            return AISProtobuf.ParameterDirection.IN;
        case OUT:
            return AISProtobuf.ParameterDirection.OUT;
        case INOUT:
            return AISProtobuf.ParameterDirection.INOUT;
        case RETURN:
            return AISProtobuf.ParameterDirection.RETURN;
        }
    }

    private static void writeSQLJJar(AISProtobuf.Schema.Builder schemaBuilder, SQLJJar sqljJar) {
        AISProtobuf.SQLJJar.Builder jarBuilder = AISProtobuf.SQLJJar.newBuilder()
            .setJarName(sqljJar.getName().getTableName())
            .setUrl(sqljJar.getURL().toExternalForm());
        if (sqljJar.getVersion() > 0)
            jarBuilder.setVersion(sqljJar.getVersion());
        schemaBuilder.addSqljJars(jarBuilder.build());
    }

}
