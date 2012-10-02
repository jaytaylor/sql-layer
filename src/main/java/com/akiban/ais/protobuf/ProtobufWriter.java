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
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.Schema;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.pt.PendingOSC;
import com.akiban.ais.util.TableChange;
import com.akiban.server.error.ProtobufWriteException;
import com.akiban.util.GrowableByteBuffer;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ProtobufWriter {
    public static interface WriteSelector {
        Columnar getSelected(Columnar columnar);
        boolean isSelected(Group group);
        /** Called for all parent joins where getSelected(UserTable) is not null **/
        boolean isSelected(Join parentJoin);
        /** Called for all GroupIndexes and all table indexes where getSelected(UserTable) is not null **/
        boolean isSelected(Index index);
        boolean isSelected(Sequence sequence);
        boolean isSelected(Routine routine);
        boolean isSelected(SQLJJar sqljJar);
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
    }


    private static final GrowableByteBuffer NO_BUFFER = new GrowableByteBuffer(0);
    private final GrowableByteBuffer buffer;
    private AISProtobuf.AkibanInformationSchema pbAIS;
    private final WriteSelector selector;

    public ProtobufWriter() {
        this(NO_BUFFER);
    }

    public ProtobufWriter(GrowableByteBuffer buffer) {
        this(buffer, ALL_SELECTOR);
    }

    public ProtobufWriter(WriteSelector selector) {
        this(NO_BUFFER, selector);
    }

    public ProtobufWriter(GrowableByteBuffer buffer, WriteSelector selector) {
        assert buffer.hasArray() : buffer;
        this.buffer = buffer;
        this.selector = selector;
    }

    public AISProtobuf.AkibanInformationSchema save(AkibanInformationSchema ais) {
        AISProtobuf.AkibanInformationSchema.Builder aisBuilder = AISProtobuf.AkibanInformationSchema.newBuilder();

        // Write top level proto messages and recurse down as needed
        if(selector == ALL_SELECTOR) {
            for(Type type : ais.getTypes()) {
                writeType(aisBuilder, type);
            }
        }
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
        if (buffer != NO_BUFFER)
            writeMessageLite(pbAIS);
        return pbAIS;
    }

    private void writeMessageLite(MessageLite msg) {
        final String MESSAGE_NAME = AISProtobuf.AkibanInformationSchema.getDescriptor().getFullName();
        final int serializedSize = msg.getSerializedSize();
        buffer.prepareForSize(serializedSize + 4);
        buffer.limit(buffer.capacity());
        buffer.putInt(serializedSize);
        final int initialPos = buffer.position();
        final int bufferSize = buffer.limit() - initialPos;
        if(serializedSize > bufferSize) {
            throw new ProtobufWriteException(
                    MESSAGE_NAME,
                    String.format("Required size exceeded available size: %d vs %d", serializedSize, bufferSize)
            );
        }
        CodedOutputStream codedOutput = CodedOutputStream.newInstance(buffer.array(), initialPos, bufferSize);
        try {
            msg.writeTo(codedOutput);
            // Successfully written, update backing buffer info
            buffer.position(initialPos + serializedSize);
        } catch(IOException e) {
            // CodedOutputStream really only throws OutOfSpace exception, but declares IOE
            throw new ProtobufWriteException(MESSAGE_NAME, e.getMessage());
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

    private static void writeSchema(AISProtobuf.AkibanInformationSchema.Builder aisBuilder, Schema schema, WriteSelector selector) {
        AISProtobuf.Schema.Builder schemaBuilder = AISProtobuf.Schema.newBuilder();
        schemaBuilder.setSchemaName(schema.getName());
        boolean isEmpty = true;

        // Write groups into same schema as root table
        for(UserTable table : schema.getUserTables().values()) {
            table = (UserTable)selector.getSelected(table);
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
        final UserTable rootTable = group.getRoot();
        AISProtobuf.Group.Builder groupBuilder = AISProtobuf.Group.newBuilder().
                setRootTableName(rootTable.getName().getTableName());
        if(group.getTreeName() != null) {
                groupBuilder.setTreeName(group.getTreeName());
        }

        for(Index index : group.getIndexes()) {
            if(selector.isSelected(index)) {
                writeGroupIndex(groupBuilder, index);
            }
        }

        schemaBuilder.addGroups(groupBuilder.build());
    }

    private static void writeTable(AISProtobuf.Schema.Builder schemaBuilder, UserTable table, WriteSelector selector) {
        AISProtobuf.Table.Builder tableBuilder = AISProtobuf.Table.newBuilder();
        tableBuilder.
                setTableName(table.getName().getTableName()).
                setTableId(table.getTableId()).
                setCharColl(convertCharAndCol(table.getCharsetAndCollation()));
                // Not yet in AIS: ordinal, description, protected

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

        if (table.getPendingOSC() != null) {
            writePendingOSC(tableBuilder, table.getPendingOSC());
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
                setTypeName(column.getType().name()).
                setIsNullable(column.getNullable()).
                setPosition(column.getPosition()).
                setCharColl(convertCharAndCol(column.getCharsetAndCollation()));

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
        return columnBuilder.build();
    }

    private static AISProtobuf.Index writeIndexCommon(Index index, boolean withTableName) {
        final IndexName indexName = index.getIndexName();
        AISProtobuf.Index.Builder indexBuilder = AISProtobuf.Index.newBuilder();
        indexBuilder.
                setIndexName(indexName.getName()).
                setIndexId(index.getIndexId()).
                setIsPK(index.isPrimaryKey()).
                setIsUnique(index.isUnique()).
                setIsAkFK(index.isAkibanForeignKey()).
                setJoinType(convertJoinType(index.getJoinType())).
                setIndexMethod(convertIndexMethod(index.getIndexMethod()));
                // Not yet in AIS: description

        if(index.getTreeName() != null) {
            indexBuilder.setTreeName(index.getTreeName());
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

    private static AISProtobuf.JoinType convertJoinType(Index.JoinType joinType) {
        switch(joinType) {
            case LEFT: return AISProtobuf.JoinType.LEFT_OUTER_JOIN;
            case RIGHT: return AISProtobuf.JoinType.RIGHT_OUTER_JOIN;
        }
        throw new ProtobufWriteException(AISProtobuf.Join.getDescriptor().getFullName(),
                                         "No match for Index.JoinType "+joinType.name());
    }

    private static AISProtobuf.CharCollation convertCharAndCol(CharsetAndCollation charAndColl) {
        if(charAndColl == null) {
            return null;
        }
        return AISProtobuf.CharCollation.newBuilder().
                setCharacterSetName(charAndColl.charset()).
                setCollationOrderName(charAndColl.collation()).
                build();
    }
    
    private static AISProtobuf.IndexMethod convertIndexMethod(Index.IndexMethod indexMethod) {
        switch (indexMethod) {
        case NORMAL: 
        default:
            return AISProtobuf.IndexMethod.NORMAL;
        case Z_ORDER_LAT_LON: 
            return AISProtobuf.IndexMethod.Z_ORDER_LAT_LON;
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

    private static void writeSequence (AISProtobuf.Schema.Builder schemaBuilder, Sequence sequence) {
        AISProtobuf.Sequence.Builder sequenceBuilder = AISProtobuf.Sequence.newBuilder()
                .setSequenceName(sequence.getSequenceName().getTableName())
                .setStart(sequence.getStartsWith())
                .setIncrement(sequence.getIncrement())
                .setMinValue(sequence.getMinValue())
                .setMaxValue(sequence.getMaxValue())
                .setIsCycle(sequence.isCycle());
        if (sequence.getTreeName() != null) {
            sequenceBuilder.setTreeName(sequence.getTreeName());
        }
        if (sequence.getAccumIndex() != null) {
            sequenceBuilder.setAccumulator(sequence.getAccumIndex());
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
        schemaBuilder.addRoutines(routineBuilder.build());
    }

    private static void writeParameter(AISProtobuf.Routine.Builder routineBuilder, Parameter parameter) {
        AISProtobuf.Parameter.Builder parameterBuilder = AISProtobuf.Parameter.newBuilder()
            .setParameterName(parameter.getName())
            .setDirection(convertParameterDirection(parameter.getDirection()))
            .setTypeName(parameter.getType().name());
        if (parameter.getTypeParameter1() != null) {
            parameterBuilder.setTypeParam1(parameter.getTypeParameter1());
        }
        if (parameter.getTypeParameter2() != null) {
            parameterBuilder.setTypeParam2(parameter.getTypeParameter2());
        }
        routineBuilder.addParameters(parameterBuilder.build());
    }

    private static AISProtobuf.RoutineCallingConvention convertRoutineCallingConvention(Routine.CallingConvention routineCallingConvention) {
        switch (routineCallingConvention) {
        case JAVA:
        default:
            return AISProtobuf.RoutineCallingConvention.JAVA;
        case LOADABLE_PLAN:
            return AISProtobuf.RoutineCallingConvention.LOADABLE_PLAN;
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
        schemaBuilder.addSqljJars(jarBuilder.build());
    }

}
