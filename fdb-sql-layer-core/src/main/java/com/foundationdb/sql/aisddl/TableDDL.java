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

package com.foundationdb.sql.aisddl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.protobuf.ProtobufWriter.TableSelector;
import com.foundationdb.sql.parser.IndexDefinitionNode;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.sql.parser.TableElementList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.DefaultIndexNameGenerator;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.IndexNameGenerator;
import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.FDBProtobuf.TupleUsage;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.*;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.format.tuple.TupleStorageDescription;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.optimizer.FunctionsTypeComputer;
import com.foundationdb.sql.parser.ColumnDefinitionNode;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.ConstraintDefinitionNode;
import com.foundationdb.sql.parser.CreateTableNode;
import com.foundationdb.sql.parser.CurrentDatetimeOperatorNode;
import com.foundationdb.sql.parser.DropGroupNode;
import com.foundationdb.sql.parser.DropTableNode;
import com.foundationdb.sql.parser.FKConstraintDefinitionNode;
import com.foundationdb.sql.parser.IndexColumnList;
import com.foundationdb.sql.parser.IndexDefinition;
import com.foundationdb.sql.parser.RenameNode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.parser.SpecialFunctionNode;
import com.foundationdb.sql.parser.StatementType;
import com.foundationdb.sql.parser.StorageFormatNode;
import com.foundationdb.sql.parser.TableElementNode;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.parser.JavaToSQLValueNode;
import com.foundationdb.sql.parser.MethodCallNode;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;


import static com.foundationdb.sql.aisddl.DDLHelper.convertName;
import static com.foundationdb.sql.aisddl.DDLHelper.skipOrThrow;

/** DDL operations on Tables */
public class TableDDL
{
    private static final Logger logger = LoggerFactory.getLogger(TableDDL.class);

    private TableDDL() {
    }

    public static void dropTable (DDLFunctions ddlFunctions,
                                  Session session, 
                                  String defaultSchemaName,
                                  DropTableNode dropTable,
                                  QueryContext context) {
        TableName tableName = convertName(defaultSchemaName, dropTable.getObjectName());
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);
        
        Table table = ais.getTable(tableName);
        if(table == null) {
            if(skipOrThrow(context, dropTable.getExistenceCheck(), table, new NoSuchTableException(tableName))) {
                return;
            }
        }

        ViewDDL.checkDropTable(ddlFunctions, session, tableName);
        checkForeignKeyDropTable(table);
        ddlFunctions.dropTable(session, tableName);
    }

    public static void dropGroup (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName,
                                    DropGroupNode dropGroup,
                                    QueryContext context)
    {
        TableName tableName = convertName(defaultSchemaName, dropGroup.getObjectName());
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);

        Table curTable = ais.getTable(tableName);
        if((curTable == null) &&
           skipOrThrow(context, dropGroup.getExistenceCheck(), curTable, new NoSuchTableException(tableName))) {
            return;
        }

        if (!curTable.isRoot()) {
            throw new DropGroupNotRootException (tableName);
        }
        
        final Group root = curTable.getGroup();
        for (Table table : ais.getTables().values()) {
            if (table.getGroup() == root) {
                ViewDDL.checkDropTable(ddlFunctions, session, table.getName());
                checkForeignKeyDropTable(table);
            }
        }
        ddlFunctions.dropGroup(session, root.getName());
    }

    private static void checkForeignKeyDropTable(Table table) {
        for (ForeignKey foreignKey : table.getReferencedForeignKeys()) {
            if (table != foreignKey.getReferencingTable()) {
                throw new ForeignKeyPreventsDropTableException(table.getName(), foreignKey.getConstraintName().getTableName(), foreignKey.getReferencingTable().getName());
            }
        }
    }

    public static void renameTable (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName,
                                    RenameNode renameTable) {
        TableName oldName = convertName(defaultSchemaName, renameTable.getObjectName());
        TableName newName = convertName(defaultSchemaName, renameTable.getNewTableName());
        ddlFunctions.renameTable(session, oldName, newName);
    }

    public static void createTable(DDLFunctions ddlFunctions,
                                   Session session,
                                   String defaultSchemaName,
                                   CreateTableNode createTable,
                                   QueryContext context) {
        if (createTable.getQueryExpression() != null)
            throw new UnsupportedCreateSelectException();

        TableName fullName = convertName(defaultSchemaName, createTable.getObjectName());
        String schemaName = fullName.getSchemaName();
        String tableName = fullName.getTableName();
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);

        Table curTable = ais.getTable(fullName);
        if((curTable != null) &&
           skipOrThrow(context, createTable.getExistenceCheck(), curTable, new DuplicateTableNameException(fullName))) {
            return;
        }

        TypesTranslator typesTranslator = ddlFunctions.getTypesTranslator();
        AISBuilder builder = new AISBuilder();
        builder.getNameGenerator().mergeAIS(ais);
        builder.table(schemaName, tableName);
        Table table = builder.akibanInformationSchema().getTable(schemaName, tableName);
        IndexNameGenerator namer = DefaultIndexNameGenerator.forTable(table);

        cloneReferencedTables(defaultSchemaName,
                              ddlFunctions.getAISCloner(),
                              ais,
                              builder.akibanInformationSchema(),
                              createTable.getTableElementList());

        // First pass: Columns.
        int colpos = 0;
        for (TableElementNode tableElement : createTable.getTableElementList()) {
            if (tableElement instanceof ColumnDefinitionNode) {
                addColumn (builder, typesTranslator,
                           (ColumnDefinitionNode)tableElement, schemaName, tableName, colpos++);
            }
        }

        // Second pass: GROUPING, PRIMARY, UNIQUE and INDEX.
        // Requires the columns to have already been created.
        for (TableElementNode tableElement : createTable.getTableElementList()) {
            if (tableElement instanceof FKConstraintDefinitionNode) {
                FKConstraintDefinitionNode fkdn = (FKConstraintDefinitionNode)tableElement;
                if (fkdn.isGrouping()) {
                    addJoin (builder, fkdn, defaultSchemaName, schemaName, tableName);
                }
                // else: regular FK, done in third pass below
            }
            else if (tableElement instanceof ConstraintDefinitionNode) {
                addIndex (namer, builder, (ConstraintDefinitionNode)tableElement, schemaName, tableName, context);
            } else if (tableElement instanceof IndexDefinitionNode) {
                addIndex (namer, builder, (IndexDefinitionNode)tableElement, schemaName, tableName, context, ddlFunctions);
            } else if (!(tableElement instanceof ColumnDefinitionNode)) {
                throw new UnsupportedSQLException("Unexpected TableElement", tableElement);
            }
        }

        // Third pass: FOREIGN KEY.
        // Separate pass as to not create extraneous indexes, if possible.
        for (TableElementNode tableElement : createTable.getTableElementList()) {
            if (tableElement instanceof FKConstraintDefinitionNode) {
                FKConstraintDefinitionNode fkdn = (FKConstraintDefinitionNode)tableElement;
                if (!fkdn.isGrouping()) {
                    addForeignKey(builder, ddlFunctions.getAIS(session), fkdn, defaultSchemaName, schemaName, tableName);
                }
            }
        }

        setTableStorage(ddlFunctions, createTable, builder, tableName, table, schemaName);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        ddlFunctions.createTable(session, table);
    }

    public static void createTable(DDLFunctions ddlFunctions,
                                   Session session,
                                   String defaultSchemaName,
                                   CreateTableNode createTable,
                                   QueryContext context,
                                   List<DataTypeDescriptor>  descriptors,
                                   List<String> columnNames,
                                   ServerSession server) {

        if (createTable.getQueryExpression() == null)
            throw new IllegalArgumentException("Expected queryExpression");

        TableName fullName = convertName(defaultSchemaName, createTable.getObjectName());
        String schemaName = fullName.getSchemaName();
        String tableName = fullName.getTableName();

        AkibanInformationSchema ais = ddlFunctions.getAIS(session);
        Table curTable = ais.getTable(fullName);
        if((curTable != null) &&
           skipOrThrow(context, createTable.getExistenceCheck(), curTable, new DuplicateTableNameException(fullName))) {
            return;
        }

        TypesTranslator typesTranslator = ddlFunctions.getTypesTranslator();
        AISBuilder builder = new AISBuilder();
        builder.table(schemaName, tableName);
        Table table = builder.akibanInformationSchema().getTable(schemaName, tableName);
        ResultColumnList resultColumns = null;
        if(createTable != null)
            resultColumns = createTable.getResultColumns();
        String newColumnName;
        ResultColumn resultColumn;
        if(resultColumns != null && resultColumns.size() > descriptors.size())
            throw new InvalidCreateAsException("More columns names in create than in select query");
        int colpos = 0;
        for (DataTypeDescriptor descriptor : descriptors) {
            if ((resultColumns != null) && (resultColumns.size() > colpos)){
                    resultColumn = resultColumns.getResultColumn(colpos+ 1);
                    if(resultColumn != null) {
                        newColumnName = resultColumn.getName();
                    }else {
                        newColumnName = columnNames.get(colpos);
                     }

            } else {
                newColumnName = columnNames.get(colpos);
            }
            addColumn(builder, schemaName, tableName, colpos++, newColumnName,typesTranslator,  descriptor);
        }
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        setTableStorage(ddlFunctions, createTable, builder, tableName, table, schemaName);
        if(createTable.isWithData()) {
            ddlFunctions.createTable(session, table, createTable.getCreateAsQuery().toLowerCase(), context, server);
            return;
        }
        ddlFunctions.createTable(session, table);
    }

    /** Copy groups of tables referenced from {@code nodes}. Ones already present in {@code targetAIS} are skipped. */
    static void cloneReferencedTables(String defaultSchema,
                                      AISCloner cloner,
                                      AkibanInformationSchema curAIS,
                                      final AkibanInformationSchema targetAIS,
                                      TableElementList nodes) {
        final Set<Group> groups = new HashSet<>();
        for(TableElementNode elem : nodes) { 
            if(elem instanceof FKConstraintDefinitionNode) {
                FKConstraintDefinitionNode fkdn = (FKConstraintDefinitionNode)elem;
                if(fkdn.getRefTableName() != null) {
                    TableName name = getReferencedName(defaultSchema, (FKConstraintDefinitionNode)elem);
                    Table t = curAIS.getTable(name);
                    if(t != null) {
                        groups.add(t.getGroup());
                    }
                }
            } // else if(elem instanceof IndexDefinitionNode)  { // when inline group indexes are supported
        }

        cloner.clone(targetAIS, curAIS, new TableSelector() {
            @Override
            public boolean isSelected(Columnar columnar) {
                return (columnar instanceof Table) &&
                       (targetAIS.getTable(columnar.getName()) == null) &&
                       groups.contains(((Table)columnar).getGroup());
            }

            @Override
            public boolean isSelected(Sequence sequence) {
                return (targetAIS.getSequence(sequence.getSequenceName()) == null);
            }

            @Override
            public boolean isSelected(Routine routine) {
                return false;
            }

            @Override
            public boolean isSelected(SQLJJar sqljJar) {
                return false;
            }

            @Override
            public boolean isSelected(ForeignKey foreignKey) {
                return false;
            }
        });
    }

    private static void setTableStorage(DDLFunctions ddlFunctions, CreateTableNode createTable,
                                        AISBuilder builder, String tableName, Table table, String schemaName){
        if (createTable.getStorageFormat() != null) {
            if (!table.isRoot()) {
                throw new SetStorageNotRootException(tableName, schemaName);
            }
            setGroup(table, builder, tableName, schemaName);
            setStorage(ddlFunctions, table.getGroup(), createTable.getStorageFormat());
        }
        else if (table.isRoot()) {
            setGroup(table, builder, tableName, schemaName);
            setStorage(ddlFunctions, table.getGroup(), null);
        }
    }

    static void setGroup(Table table, AISBuilder builder, String tableName, String schemaName) {
        if (table.getGroup() == null) {
            builder.createGroup(tableName, schemaName);
            builder.addTableToGroup(tableName, schemaName, tableName);
        }
    }

    public static void setStorage(DDLFunctions ddlFunctions,
                                  HasStorage object, 
                                  StorageFormatNode storage) {
        if (storage != null) {
            object.setStorageDescription(ddlFunctions.getStorageFormatRegistry().parseSQL(storage, object));
            return;
        }
        object.setStorageDescription(ddlFunctions.getStorageFormatRegistry().getDefaultStorageDescription(object));
    }

    static void addColumn (final AISBuilder builder, final TypesTranslator typesTranslator, final ColumnDefinitionNode cdn,
                           final String schemaName, final String tableName, int colpos) {

        String typeName = cdn.getType().getTypeName();
        // Special handling for the "[BIG]SERIAL" column type -> which is transformed to
        // [BIG]INT NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1)
        boolean isSerial = "serial".equalsIgnoreCase(typeName);
        boolean isBigSerial = "bigserial".equalsIgnoreCase(typeName);
        if (isSerial || isBigSerial) {
            // [BIG]INT NOT NULL
            DataTypeDescriptor typeDesc = new DataTypeDescriptor(isBigSerial ? TypeId.BIGINT_ID : TypeId.INTEGER_ID, false);
            addColumn (builder, typesTranslator,
                       schemaName, tableName, cdn.getColumnName(), colpos,
                       typeDesc, null, null);
            // GENERATED BY DEFAULT AS IDENTITY
            setAutoIncrement (builder, schemaName, tableName, cdn.getColumnName(), true, 1, 1);
        } else {
            String[] defaultValueFunction = getColumnDefault(cdn, schemaName, tableName);
            addColumn(builder, typesTranslator,
                      schemaName, tableName, cdn.getColumnName(), colpos,
                      cdn.getType(), defaultValueFunction[0], defaultValueFunction[1]);
            if (cdn.isAutoincrementColumn()) {
                setAutoIncrement(builder, schemaName, tableName, cdn);
            }
        }
    }

    static void addColumn (final AISBuilder builder, final String schemaName,
                           final String tableName, int colpos, final String columnName,
                           final TypesTranslator typesTranslator, final DataTypeDescriptor d) {
        TInstance type = typesTranslator.typeForSQLType(d,
                schemaName, tableName, columnName);
        builder.column(schemaName, tableName, columnName,
                colpos, type, false, null, null);
    }

    public static void setAutoIncrement(AISBuilder builder, String schema, String table, ColumnDefinitionNode cdn) {
        // if the cdn has a default node-> GENERATE BY DEFAULT
        // if no default node -> GENERATE ALWAYS
        Boolean defaultIdentity = cdn.getDefaultNode() != null;
        setAutoIncrement(builder, schema, table, cdn.getColumnName(),
                         defaultIdentity, cdn.getAutoincrementStart(), cdn.getAutoincrementIncrement());
    }

    public static void setAutoIncrement(AISBuilder builder, String schemaName, String tableName, String columnName,
                                        boolean defaultIdentity, long start, long increment) {
        // make the column an identity column 
        builder.columnAsIdentity(schemaName, tableName, columnName, start, increment, defaultIdentity);
    }
    
    static String[] getColumnDefault(ColumnDefinitionNode cdn, 
                                     String schemaName, String tableName) {
        String defaultValue = null, defaultFunction = null;
        if (cdn.getDefaultNode() != null) {
            ValueNode valueNode = cdn.getDefaultNode().getDefaultTree();
            if (valueNode == null) {
            }
            else if (valueNode instanceof ConstantNode) {
                defaultValue = ((ConstantNode)valueNode).getValue().toString();
            }
            else if (valueNode instanceof SpecialFunctionNode) {
                defaultFunction = FunctionsTypeComputer.specialFunctionName((SpecialFunctionNode)valueNode);
            }
            else if (valueNode instanceof CurrentDatetimeOperatorNode) {
                defaultFunction = FunctionsTypeComputer.currentDatetimeFunctionName((CurrentDatetimeOperatorNode)valueNode);
            }
            else if ((valueNode instanceof JavaToSQLValueNode) && 
                    (((JavaToSQLValueNode) valueNode).getJavaValueNode() instanceof MethodCallNode) &&
                    (((MethodCallNode) ((JavaToSQLValueNode) valueNode).getJavaValueNode()).getMethodParameters().length == 0)) {
                // if default is a method with no arguments:
                defaultFunction = ((MethodCallNode) ((JavaToSQLValueNode) valueNode).getJavaValueNode()).getMethodName();
            }
            else {
                throw new BadColumnDefaultException(schemaName, tableName, 
                                                    cdn.getColumnName(), 
                                                    cdn.getDefaultNode().getDefaultText());
            }
        }
        return new String[] { defaultValue, defaultFunction };
    }

    static void addColumn(final AISBuilder builder, final TypesTranslator typesTranslator,
                          final String schemaName, final String tableName, final String columnName,
                          int colpos, DataTypeDescriptor sqlType,
                          final String defaultValue, final String defaultFunction) {
        TInstance type = typesTranslator.typeForSQLType(sqlType,
                schemaName, tableName, columnName);
        builder.column(schemaName, tableName, columnName, 
                       colpos, type, false, defaultValue, defaultFunction);
    }

    public static String addIndex(IndexNameGenerator namer, AISBuilder builder, ConstraintDefinitionNode cdn,
                                  String schemaName, String tableName, QueryContext context)  {
        // We don't (yet) have a constraint representation so override any provided
        Table table = builder.akibanInformationSchema().getTable(schemaName, tableName);
        boolean isUnique;
        boolean isPrimary;
        String indexName = cdn.getName();
        int colPos = 0;

        if (cdn.getConstraintType() == ConstraintDefinitionNode.ConstraintType.CHECK) {
            throw new UnsupportedCheckConstraintException ();
        }
        else if (cdn.getConstraintType() == ConstraintDefinitionNode.ConstraintType.PRIMARY_KEY) {
            indexName = Index.PRIMARY;
            isPrimary = isUnique = true;
        }
        else if (cdn.getConstraintType() == ConstraintDefinitionNode.ConstraintType.UNIQUE) {
            isPrimary = false;
            isUnique = true;
        } else {
            throw new UnsupportedCheckConstraintException();
        }

        if(indexName == null) {
            indexName = namer.generateIndexName(null, cdn.getColumnList().get(0).getName());
        }

        // index is unique or primary
        TableName constraintName = null;
        if (cdn.getConstraintName() != null) {
            constraintName = DDLHelper.convertName(schemaName, cdn.getConstraintName());
        }
        if (isPrimary) {
            if (constraintName == null) {
                builder.pk(schemaName, tableName);
            } else {
                builder.pkConstraint(schemaName, tableName, constraintName);
            }
        }
        else if (isUnique) {
            if (constraintName == null) {
                builder.unique(schemaName, tableName, indexName);
            } else {
                builder.uniqueConstraint(schemaName, tableName, indexName, constraintName);
            }
        }
        
        for (ResultColumn col : cdn.getColumnList()) {
            if(table.getColumn(col.getName()) == null) {
                throw new NoSuchColumnException(col.getName());
            }
            // Per SQL Specification: Feature ID: E141-08  - Not Null implied on Primary Key
            if (isPrimary) {
                Column tableColumn = table.getColumn(col.getName());
                tableColumn.setType(tableColumn.getType().withNullable(false));
            }
            builder.indexColumn(schemaName, tableName, indexName, col.getName(), colPos++, true, null);
        }
        return indexName;
    }

    public static String addIndex(IndexNameGenerator namer,
                                  AISBuilder builder,
                                  IndexDefinitionNode idn,
                                  String schemaName,
                                  String tableName,
                                  QueryContext context,
                                  DDLFunctions ddl) {
        if(idn.getJoinType() != null) {
            throw new UnsupportedSQLException("CREATE TABLE containing group index");
        }
        String indexName = idn.getName();
        Table table = builder.akibanInformationSchema().getTable(schemaName, tableName);
        return generateTableIndex(namer, builder, idn, indexName, table, context, ddl);
    }

    public static TableName getReferencedName(String schemaName, FKConstraintDefinitionNode fkdn) {
        return convertName(schemaName, fkdn.getRefTableName());
    }

    public static void addJoin(final AISBuilder builder, final FKConstraintDefinitionNode fkdn,
                               final String defaultSchemaName, final String schemaName, final String tableName)  {
        TableName parentName = getReferencedName(defaultSchemaName, fkdn);

        AkibanInformationSchema ais = builder.akibanInformationSchema();
        // Check parent table exists
        Table parentTable = ais.getTable(parentName);
        if (parentTable == null) {
            throw new JoinToUnknownTableException(new TableName(schemaName, tableName), parentName);
        }
        // Check child table exists
        Table childTable = ais.getTable(schemaName, tableName);
        if (childTable == null) {
            throw new NoSuchTableException(schemaName, tableName);
        }
        // Check that we aren't joining to ourselves
        if (parentTable == childTable) {
            throw new JoinToSelfException(schemaName, tableName);
        }
        // Check that fk list and pk list are the same size
        String[] fkColumns = columnNamesFromListOrPK(fkdn.getColumnList(), null); // No defaults for child table
        String[] pkColumns = columnNamesFromListOrPK(fkdn.getRefResultColumnList(), parentTable.getPrimaryKey());

        int actualPkColCount = parentTable.getPrimaryKeyIncludingInternal().getColumns().size();
        if ((fkColumns.length != actualPkColCount) || (pkColumns.length != actualPkColCount)) {
            throw new JoinColumnMismatchException(fkdn.getColumnList().size(),
                                                  new TableName(schemaName, tableName),
                                                  parentName,
                                                  parentTable.getPrimaryKeyIncludingInternal().getColumns().size());
        }

        int colPos = 0;
        while((colPos < fkColumns.length) && (colPos < pkColumns.length)) {
            String fkColumn = fkColumns[colPos];
            String pkColumn = pkColumns[colPos];
            if (childTable.getColumn(fkColumn) == null) {
                throw new NoSuchColumnException(String.format("%s.%s.%s", schemaName, tableName, fkColumn));
            }
            if (parentTable.getColumn(pkColumn) == null) {
                throw new JoinToWrongColumnsException(new TableName(schemaName, tableName),
                                                      fkColumn,
                                                      parentName,
                                                      pkColumn);
            }
            ++colPos;
        }

        String joinName = builder.getNameGenerator().generateJoinName(parentName, childTable.getName(), pkColumns, fkColumns);

        if (fkdn.getConstraintName() != null) {
            joinName = fkdn.getConstraintName().getTableName();
        }
        
        builder.joinTables(joinName, parentName.getSchemaName(), parentName.getTableName(), schemaName, tableName);

        colPos = 0;
        while(colPos < fkColumns.length) {
            builder.joinColumns(joinName,
                                parentName.getSchemaName(), parentName.getTableName(), pkColumns[colPos],
                                schemaName, tableName, fkColumns[colPos]);
            ++colPos;
        }
        builder.addJoinToGroup(parentTable.getGroup().getName(), joinName, 0);
    }

    private static String[] columnNamesFromListOrPK(ResultColumnList list, PrimaryKey pk) {
        String[] names = (list == null) ? null: list.getColumnNames();
        if(((names == null) || (names.length == 0)) && (pk != null)) {
            Index index = pk.getIndex();
            names = new String[index.getKeyColumns().size()];
            int i = 0;
            for(IndexColumn iCol : index.getKeyColumns()) {
                names[i++] = iCol.getColumn().getName();
            }
        }
        if(names == null) {
            names = new String[0];
        }
        return names;
    }
    
    private static String generateTableIndex(IndexNameGenerator namer,
            AISBuilder builder,
            IndexDefinition id,
            String indexName,
            Table table,
            QueryContext context,
            DDLFunctions ddl
            ) {
        IndexColumnList columnList = id.getIndexColumnList();
        Index tableIndex;
        TableName constraintName = null;
        if(indexName == null) {
            indexName = namer.generateIndexName(null, columnList.get(0).getColumnName());
        }
        if(id.isUnique()) {
            constraintName = builder.getNameGenerator().generateUniqueConstraintName(table.getName().getSchemaName(), indexName);
        }
        if (columnList.functionType() == IndexColumnList.FunctionType.FULL_TEXT) {
            logger.debug ("Building Full text index on table {}", table.getName()) ;
            tableIndex = IndexDDL.buildFullTextIndex(builder, table.getName(), indexName, id, null, null);
        } else if (IndexDDL.checkIndexType (id, table.getName()) == Index.IndexType.TABLE) {
            logger.debug ("Building Table index on table {}", table.getName()) ;
            tableIndex = IndexDDL.buildTableIndex (builder, table.getName(), indexName, id, constraintName, null, null);
        } else {
            logger.debug ("Building Group index on table {}", table.getName());
            tableIndex = IndexDDL.buildGroupIndex(builder, table.getName(), indexName, id, null, null);
        }

        boolean indexIsSpatial = columnList.functionType() == IndexColumnList.FunctionType.Z_ORDER_LAT_LON;
        // Can't check isSpatialCompatible before the index columns have been added.
        if (indexIsSpatial && !Index.isSpatialCompatible(tableIndex)) {
            throw new BadSpatialIndexException(tableIndex.getIndexName().getTableName(), null);
        }
        StorageFormatNode sfn = id.getStorageFormat();
        if (sfn != null) {
            tableIndex.setStorageDescription(ddl.getStorageFormatRegistry().parseSQL(sfn, tableIndex));
        }
        return tableIndex.getIndexName().getName();
    }

    protected static void addForeignKey(AISBuilder builder,
                                        AkibanInformationSchema sourceAIS,
                                        FKConstraintDefinitionNode fkdn,
                                        String defaultSchemaName,
                                        String referencingSchemaName,
                                        String referencingTableName) {
        AkibanInformationSchema targetAIS = builder.akibanInformationSchema();
        Table referencingTable = targetAIS.getTable(referencingSchemaName, referencingTableName);
        TableName referencedName = getReferencedName(defaultSchemaName, fkdn);
        Table referencedTable = sourceAIS.getTable(referencedName);
        if (referencedTable == null) {
            if (referencedName.equals(referencingTable.getName())) {
                referencedTable = referencingTable; // Circular reference to self.
            }
            else {
                throw new JoinToUnknownTableException(new TableName(referencingSchemaName, referencingTableName), referencedName);
            }
        }
        if (fkdn.getMatchType() != FKConstraintDefinitionNode.MatchType.SIMPLE) {
            throw new UnsupportedFKMatchException(fkdn);
        }
        String constraintName = fkdn.getName();
        if (constraintName == null) {
            constraintName = builder.getNameGenerator().generateFKConstraintName(referencingSchemaName, referencingTableName).getTableName();
        }
        String[] referencingColumnNames = columnNamesFromListOrPK(fkdn.getColumnList(), 
                                                                  null);
        String[] referencedColumnNames = columnNamesFromListOrPK(fkdn.getRefResultColumnList(), 
                                                                 referencedTable.getPrimaryKey());
        if (referencingColumnNames.length != referencedColumnNames.length) {
            throw new JoinColumnMismatchException(referencingColumnNames.length,
                                                  new TableName(referencingSchemaName, referencingTableName),
                                                  referencedName,
                                                  referencedColumnNames.length);
        }
        List<Column> referencedColumns = new ArrayList<>(referencedColumnNames.length);
        List<Column> referencingColumns = new ArrayList<>(referencingColumnNames.length);
        for (int i = 0; i < referencingColumnNames.length; i++) {
            Column referencingColumn = referencingTable.getColumn(referencingColumnNames[i]);
            if (referencingColumn == null) {
                throw new NoSuchColumnException(referencingColumnNames[i]); 
            }
            referencingColumns.add(referencingColumn);
            Column referencedColumn = referencedTable.getColumn(referencedColumnNames[i]);
            if (referencedColumn == null) {
                throw new NoSuchColumnException(referencedColumnNames[i]);
            }
            referencedColumns.add(referencedColumn);
        }

        // Note: Referenced side index checked in validation

        // Pick (or create) a referencing side index
        TableIndex referencingIndex = ForeignKey.findReferencingIndex(referencingTable,
                                                                      referencingColumns);
        if (referencingIndex == null) {
            List<String> allIndexNames = new ArrayList<>();
            for(Index index : referencingTable.getIndexesIncludingInternal()) {
                allIndexNames.add(index.getIndexName().getName());
            }
            for(Index index : referencingTable.getFullTextIndexes()) {
                allIndexNames.add(index.getIndexName().getName());
            }
            String name = DefaultNameGenerator.findUnique(allIndexNames, constraintName, DefaultNameGenerator.MAX_IDENT);
            builder.index(referencingSchemaName, referencingTableName, name);
            for(int i = 0; i < referencingColumnNames.length; ++i) {
                builder.indexColumn(referencingSchemaName,
                                    referencingTableName,
                                    name,
                                    referencingColumnNames[i],
                                    i,
                                    true,
                                    null /*indexedLength*/);
            }
        }

        builder.foreignKey(referencingSchemaName, referencingTableName,
                           Arrays.asList(referencingColumnNames),
                           referencedName.getSchemaName(), referencedName.getTableName(),
                           Arrays.asList(referencedColumnNames),
                           convertReferentialAction(fkdn.getRefActionDeleteRule()),
                           convertReferentialAction(fkdn.getRefActionUpdateRule()),
                           fkdn.isDeferrable(), fkdn.isInitiallyDeferred(),
                           constraintName);
    }

    private static ForeignKey.Action convertReferentialAction(int action) {
        switch (action) {
        case StatementType.RA_NOACTION:
        default:
            return ForeignKey.Action.NO_ACTION;
        case StatementType.RA_RESTRICT:
            return ForeignKey.Action.RESTRICT;
        case StatementType.RA_CASCADE:
            return ForeignKey.Action.CASCADE;
        case StatementType.RA_SETNULL:
            return ForeignKey.Action.SET_NULL;
        case StatementType.RA_SETDEFAULT:
            return ForeignKey.Action.SET_DEFAULT;
        }
    }
        
}
