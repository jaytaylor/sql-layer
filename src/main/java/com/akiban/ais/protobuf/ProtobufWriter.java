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
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Schema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;

import com.akiban.ais.model.UserTable;
import com.akiban.server.error.ProtobufWriteException;
import com.akiban.util.GrowableByteBuffer;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import java.io.IOException;

public class ProtobufWriter {
    public static interface WriteSelector {
        boolean isSelected(UserTable table);
        /** Called for all GroupIndexes and all table indexes where isSelected(UserTable) is true **/
        boolean isSelected(Index index);
    }

    public static WriteSelector ALL_SELECTOR = new WriteSelector() {
        @Override
        public boolean isSelected(UserTable table) {
            return true;
        }

        @Override
        public boolean isSelected(Index index) {
            return true;
        }
    };

    public static abstract class TableAllIndexSelector implements WriteSelector {
        @Override
        public boolean isSelected(Index index) {
            return true;
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
        public boolean isSelected(UserTable table) {
            return schemaName.equals(table.getName().getSchemaName());
        }

        @Override
        public boolean isSelected(Index index) {
            return true;
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
            if(selector.isSelected(table)) {
                if(table.getParentJoin() == null && table.getGroup() != null) {
                    writeGroup(schemaBuilder, table.getGroup(), selector);
                }
                writeTable(schemaBuilder, table, selector);
                isEmpty = false;
            }
        }

        if(!isEmpty) {
            aisBuilder.addSchemas(schemaBuilder.build());
        }
    }

    private static void writeGroup(AISProtobuf.Schema.Builder schemaBuilder, Group group, WriteSelector selector) {
        final UserTable rootTable = group.getGroupTable().getRoot();
        AISProtobuf.Group.Builder groupBuilder = AISProtobuf.Group.newBuilder().
                setRootTableName(rootTable.getName().getTableName()).
                setTreeName(rootTable.getTreeName());

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
        
        tableBuilder.addColumns(columnBuilder.build());
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
                setJoinType(convertJoinType(index.getJoinType()));
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
}
