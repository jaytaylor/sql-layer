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

import com.foundationdb.ais.model.Sequence;
import com.foundationdb.server.error.ColumnAlreadyGeneratedException;
import com.foundationdb.server.error.ColumnNotGeneratedException;
import com.foundationdb.server.error.ProtectedColumnDDLException;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.sql.parser.AlterDropIndexNode;
import com.foundationdb.sql.parser.AlterTableRenameColumnNode;
import com.foundationdb.sql.parser.AlterTableRenameNode;
import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.DefaultIndexNameGenerator;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.IndexNameGenerator;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.ais.util.TableChange.ChangeType;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.error.ForeignKeyPreventsAlterColumnException;
import com.foundationdb.server.error.JoinToMultipleParentsException;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchConstraintException;
import com.foundationdb.server.error.NoSuchForeignKeyException;
import com.foundationdb.server.error.NoSuchGroupingFKException;
import com.foundationdb.server.error.NoSuchIndexException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.NoSuchUniqueException;
import com.foundationdb.server.error.UnsupportedCheckConstraintException;
import com.foundationdb.server.error.UnsupportedFKIndexException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.ColumnDefinitionNode;
import com.foundationdb.sql.parser.ConstraintDefinitionNode;
import com.foundationdb.sql.parser.FKConstraintDefinitionNode;
import com.foundationdb.sql.parser.IndexDefinitionNode;
import com.foundationdb.sql.parser.ModifyColumnNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.parser.StatementType;
import com.foundationdb.sql.parser.TableElementList;
import com.foundationdb.sql.parser.TableElementNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.LoggerFactory;

import static com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import static com.foundationdb.sql.aisddl.DDLHelper.convertName;
import static com.foundationdb.sql.aisddl.DDLHelper.skipOrThrow;
import static com.foundationdb.sql.parser.ConstraintDefinitionNode.ConstraintType;

public class AlterTableDDL {
    private AlterTableDDL() {}

    public static ChangeLevel alterTable(DDLFunctions ddlFunctions,
                                         DMLFunctions dmlFunctions,
                                         Session session,
                                         String defaultSchemaName,
                                         AlterTableNode alterTable,
                                         QueryContext context) {
        final AkibanInformationSchema curAIS = ddlFunctions.getAIS(session);
        final TableName tableName = convertName(defaultSchemaName, alterTable.getObjectName());
        final Table table = curAIS.getTable(tableName);
        if((table == null) &&
           skipOrThrow(context, alterTable.getExistenceCheck(), null, new NoSuchTableException(tableName))) {
            return null;
        }

        if (alterTable.isUpdateStatistics()) {
            Collection<String> indexes = null;
            if (!alterTable.isUpdateStatisticsAll())
                indexes = Collections.singletonList(alterTable.getIndexNameForUpdateStatistics());
            ddlFunctions.updateTableStatistics(session, tableName, indexes);
            return null;
        }

        if (alterTable.isTruncateTable()) {
            dmlFunctions.truncateTable(session, table.getTableId(), alterTable.isCascade());
            return null;
        }

        ChangeLevel level = null;
        if((alterTable.tableElementList != null) && !alterTable.tableElementList.isEmpty()) {
            level = processAlter(ddlFunctions, session, defaultSchemaName, table, alterTable.tableElementList, context);
        }

        if(level == null) {
            throw new UnsupportedSQLException (alterTable.statementToString(), alterTable);
        }
        return level;
    }

    private static ChangeLevel processAlter(DDLFunctions ddl,
                                            Session session,
                                            String defaultSchema,
                                            Table origTable,
                                            TableElementList elements,
                                            QueryContext context) {
        List<TableChange> columnChanges = new ArrayList<>();
        List<TableChange> indexChanges = new ArrayList<>();
        List<ColumnDefinitionNode> columnDefNodes = new ArrayList<>();
        List<FKConstraintDefinitionNode> fkDefNodes= new ArrayList<>();
        List<ConstraintDefinitionNode> conDefNodes = new ArrayList<>();
        List<IndexDefinitionNode> indexDefNodes = new ArrayList<>();

        for(TableElementNode node : elements) {
            switch(node.getNodeType()) {
                case NodeTypes.COLUMN_DEFINITION_NODE: {
                    ColumnDefinitionNode cdn = (ColumnDefinitionNode) node;
                    String columnName = cdn.getColumnName();
                    columnChanges.add(TableChange.createAdd(columnName));
                    if (Column.isInternalName(columnName))
                    {
                        throw new ProtectedColumnDDLException(columnName);
                    }
                    columnDefNodes.add(cdn);
                } break;

                case NodeTypes.DROP_COLUMN_NODE: {
                    String columnName = ((ModifyColumnNode)node).getColumnName();
                    if(Column.isInternalName(columnName) || (origTable.getColumn(columnName) == null)) {
                        skipOrThrow(context, ((ModifyColumnNode)node).getExistenceCheck(), null, new NoSuchColumnException(columnName));
                    } else {
                        columnChanges.add(TableChange.createDrop(columnName));
                    }
                } break;

                case NodeTypes.MODIFY_COLUMN_DEFAULT_NODE:
                case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE:
                case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE:
                case NodeTypes.MODIFY_COLUMN_TYPE_NODE: {
                    ModifyColumnNode modNode = (ModifyColumnNode)node;
                    String columnName = modNode.getColumnName();
                    Column column = origTable.getColumn(columnName);
                    if(Column.isInternalName(columnName) || (column == null)) {
                        skipOrThrow(context, modNode.getExistenceCheck(), null, new NoSuchColumnException(columnName));
                    } else {
                        // Special case: The only requested change is RESTART WITH.
                        // Cannot go through the current ALTER flow as the new value may appear to be the same,
                        // triggering NONE change, but should still take affect as values may have been allocated.
                        if((elements.size() == 1) && isRestartWithNode(modNode)) {
                            Sequence curSeq = column.getIdentityGenerator();
                            if(curSeq == null) {
                                throw new ColumnNotGeneratedException(column);
                            }
                            AkibanInformationSchema aisCopy = new AkibanInformationSchema();
                            Sequence newSeq = Sequence.create(aisCopy,
                                                              curSeq.getSchemaName(),
                                                              curSeq.getSequenceName().getTableName(),
                                                              modNode.getAutoincrementStart(),
                                                              curSeq.getIncrement(),
                                                              curSeq.getMinValue(),
                                                              curSeq.getMaxValue(),
                                                              curSeq.isCycle());
                            ddl.alterSequence(session, curSeq.getSequenceName(), newSeq);
                            return ChangeLevel.METADATA;
                        } else {
                            columnChanges.add(TableChange.createModify(columnName, columnName));
                            columnDefNodes.add((ColumnDefinitionNode)node);
                        }
                    }
                } break;

                case NodeTypes.FK_CONSTRAINT_DEFINITION_NODE: {
                    FKConstraintDefinitionNode fkNode = (FKConstraintDefinitionNode) node;
                    if(fkNode.getConstraintType() == ConstraintType.DROP) {
                        if(fkNode.isGrouping()) {
                            if(origTable.getParentJoin() == null) {
                                skipOrThrow(context, fkNode.getExistenceCheck(), null, new NoSuchGroupingFKException(origTable.getName()));
                                fkNode = null;
                            }
                        } else {
                            if(fkNode.getConstraintName() == null) {
                                Collection<ForeignKey> fkeys = origTable.getReferencingForeignKeys();
                                if(fkeys.size() == 0) {
                                    skipOrThrow(context, fkNode.getExistenceCheck(), null, new UnsupportedFKIndexException());
                                    fkNode = null;
                                } else if(fkeys.size() != 1) {
                                    throw new UnsupportedFKIndexException();
                                } else {
                                    try {
                                        fkNode.setConstraintName(fkeys.iterator().next().getConstraintName().getTableName());
                                    } catch(StandardException ex) {
                                        throw new SQLParserInternalException(ex);
                                    }
                                }
                            } else if(origTable.getReferencingForeignKey(fkNode.getConstraintName().getTableName()) == null) {
                                skipOrThrow(context,
                                            fkNode.getExistenceCheck(),
                                            null,
                                            new NoSuchForeignKeyException(fkNode.getConstraintName().getTableName(), origTable.getName()));
                                fkNode = null;
                            }
                            if(fkNode != null) {
                                // Also drop the referencing index.
                                indexChanges.add(TableChange.createDrop(fkNode.getConstraintName().getTableName()));
                            }
                        }
                    }
                    if(fkNode != null) {
                        fkDefNodes.add(fkNode);
                    }
                } break;

                case NodeTypes.CONSTRAINT_DEFINITION_NODE: {
                    ConstraintDefinitionNode cdn = (ConstraintDefinitionNode) node;
                    if(cdn.getConstraintType() == ConstraintType.DROP) {
                        String name = cdn.getName();
                        switch(cdn.getVerifyType()) {
                            case PRIMARY_KEY:
                                if(origTable.getPrimaryKey() == null) {
                                    skipOrThrow(context, cdn.getExistenceCheck(),
                                                null,
                                                new NoSuchConstraintException(origTable.getName(), Index.PRIMARY));
                                    name = null;
                                } else {
                                    name = origTable.getPrimaryKey().getIndex().getIndexName().getName();
                                }
                            break;
                            case DROP:
                                boolean found = false;
                                String indexName = indexNameForConstrainName(origTable, name);
                                if (indexName != null) {
                                    found = true;
                                    name = indexName;
                                } else if (origTable.getReferencingForeignKey(name) != null) {
                                    fkDefNodes.add(newFKDropNode(node, name, Boolean.FALSE));
                                    found = true;
                                } else if (origTable.getParentJoin() != null && origTable.getParentJoin().getName().equals(name)) {
                                    fkDefNodes.add(newFKDropNode(node, name, Boolean.TRUE));
                                    found = true;
                                    name = null;
                                }
                                if(!found) {
                                   skipOrThrow(context,
                                               cdn.getExistenceCheck(),
                                               null,
                                               new NoSuchConstraintException(origTable.getName(), name));
                                    name = null;
                                }
                                break;
                            case UNIQUE:
                                Index index = origTable.getIndex(name);
                                if(index == null || !index.isUnique()) {
                                    skipOrThrow(context,
                                                cdn.getExistenceCheck(),
                                                null,
                                                new NoSuchUniqueException(origTable.getName(), cdn.getName()));
                                    name = null;
                                }
                            break;
                            case CHECK:
                                throw new UnsupportedCheckConstraintException();
                        }
                        if (name != null) {
                            indexChanges.add(TableChange.createDrop(name));
                        }
                    } else if (cdn.getConstraintType() == ConstraintType.PRIMARY_KEY) {
                        if (origTable.getPrimaryKeyIncludingInternal().isAkibanPK())
                        {
                            columnChanges.add(TableChange.createDrop(Column.ROW_ID_NAME));
                            String indexName = origTable.getPrimaryKeyIncludingInternal().getIndex().getIndexName().getName();
                            indexChanges.add(TableChange.createDrop(indexName));
                        }
                        conDefNodes.add(cdn);
                    } else {
                        conDefNodes.add(cdn);
                    }
                } break;

                case NodeTypes.INDEX_DEFINITION_NODE:
                    IndexDefinitionNode idn = (IndexDefinitionNode)node;
                    if(idn.getJoinType() != null) {
                        throw new UnsupportedSQLException("ALTER ADD INDEX containing group index");
                    }
                    indexDefNodes.add(idn);
                    break;
                    
                case NodeTypes.AT_DROP_INDEX_NODE: {
                    AlterDropIndexNode dropIndexNode = (AlterDropIndexNode)node;
                    String name = dropIndexNode.getIndexName();
                    if(origTable.getIndex(name) == null) {
                        skipOrThrow(context, dropIndexNode.getExistenceCheck(), null, new NoSuchIndexException(name));
                    } else {
                        indexChanges.add(TableChange.createDrop(name));
                    }
                }
                break;

                case NodeTypes.AT_RENAME_NODE:
                    TableName newName = DDLHelper.convertName(defaultSchema,
                                                              ((AlterTableRenameNode)node).newName());
                    TableName oldName = origTable.getName();
                    ddl.renameTable(session, oldName, newName);
                    return ChangeLevel.METADATA;
                    
                case NodeTypes.AT_RENAME_COLUMN_NODE:
                    AlterTableRenameColumnNode alterRenameCol = (AlterTableRenameColumnNode) node;
                    String oldColName = alterRenameCol.getName();
                    String newColName = alterRenameCol.newName();
                    final Column oldCol = origTable.getColumn(oldColName);

                    if (oldCol == null) {
                        throw new NoSuchColumnException(oldColName);
                    }
                    columnChanges.add(TableChange.createModify(oldColName, newColName));
                break;

                default:
                    return null; // Something unsupported
            }
        }
        
        for (ForeignKey foreignKey : origTable.getForeignKeys()) {
            if (foreignKey.getReferencingTable() == origTable) {
                checkForeignKeyAlterColumns(columnChanges, foreignKey.getReferencingColumns(),
                                            foreignKey, origTable);
            }
            if (foreignKey.getReferencedTable() == origTable) {
                checkForeignKeyAlterColumns(columnChanges, foreignKey.getReferencedColumns(),
                                            foreignKey, origTable);
            }
        }

        final AkibanInformationSchema origAIS = origTable.getAIS();
        final Table tableCopy = copyTable(ddl.getAISCloner(), origTable, columnChanges);
        final AkibanInformationSchema aisCopy = tableCopy.getAIS();
        TableDDL.cloneReferencedTables(defaultSchema, ddl.getAISCloner(), origAIS, aisCopy, elements);
        final TypesTranslator typesTranslator = ddl.getTypesTranslator();
        final AISBuilder builder = new AISBuilder(aisCopy);
        builder.getNameGenerator().mergeAIS(origAIS);

        int pos = tableCopy.getColumnsIncludingInternal().size();
        for(ColumnDefinitionNode cdn : columnDefNodes) {
            if(cdn instanceof ModifyColumnNode) {
                ModifyColumnNode modNode = (ModifyColumnNode) cdn;
                handleModifyColumnNode(modNode, builder, tableCopy, typesTranslator);
            } else {
                TableDDL.addColumn(builder, typesTranslator, cdn, origTable.getName().getSchemaName(), origTable.getName().getTableName(), pos++);
            }
        }
        // Ensure that internal columns stay at the end
        // because there's a bunch of places that assume that they are
        // (e.g. they assume getColumns() have indexes (1...getColumns().size()))
        // If the original table had a primary key, the hidden pk is added a bit farther down
        
        for (Column origColumn : origTable.getColumnsIncludingInternal()) {
            if (origColumn.isInternalColumn()) {
                String newName = findNewName(columnChanges, origColumn.getName());
                if (newName != null) {
                    Column.create(tableCopy, origColumn, newName, pos++);
                }
            }
        }
        copyTableIndexes(origTable, tableCopy, columnChanges, indexChanges);

        IndexNameGenerator indexNamer = DefaultIndexNameGenerator.forTable(tableCopy);
        TableName newName = tableCopy.getName();
        for(ConstraintDefinitionNode cdn : conDefNodes) {
            assert cdn.getConstraintType() != ConstraintType.DROP : cdn;
            String name = TableDDL.addIndex(indexNamer, builder, cdn, newName.getSchemaName(), newName.getTableName(), context);
            indexChanges.add(TableChange.createAdd(name));
            // This is required as the addIndex() for a primary key constraint 
            // *may* alter the NULL->NOT NULL status
            // of the columns in the primary key
            if (name.equals(Index.PRIMARY)) {
                for (IndexColumn col : tableCopy.getIndex(name).getKeyColumns())
                {
                    String columnName = col.getColumn().getName();
                    
                    // Check if the column was added in the same alter as creating the index: 
                    // ALTER TABLE c ADD COLUMN n SERIAL PRIMARY KEY
                    // You can't add and modify the column, so assume the add does the correct thing.
                    boolean columnAdded = false;
                    for (TableChange change : columnChanges) {
                        if (change.getChangeType() ==  ChangeType.ADD && columnName.equals(change.getNewName())) 
                            columnAdded = true;
                    }
                    if (!columnAdded)
                        columnChanges.add(TableChange.createModify(columnName, columnName));
                }
            }
        }

        for(IndexDefinitionNode idn : indexDefNodes) {
            String name = TableDDL.addIndex(indexNamer, builder, idn, newName.getSchemaName(), newName.getTableName(), context, ddl);
            indexChanges.add(TableChange.createAdd(name));
        }

        // Correctly adds the Hidden PK (including sequence).
        if (tableCopy.getPrimaryKeyIncludingInternal() == null) {
            if (origTable.getPrimaryKeyIncludingInternal().isAkibanPK()) {
                Column origColumn = origTable.getPrimaryKeyIncludingInternal().getColumns().get(0);
                Column.create(tableCopy, origColumn, Column.ROW_ID_NAME, tableCopy.getColumns().size());
            } else {
                tableCopy.addHiddenPrimaryKey(builder.getNameGenerator());
                columnChanges.add(TableChange.createAdd(Column.ROW_ID_NAME));
            }
        }
        
        for(FKConstraintDefinitionNode fk : fkDefNodes) {
            if(fk.isGrouping()) {
                if(fk.getConstraintType() == ConstraintType.DROP) {
                    Join parentJoin = tableCopy.getParentJoin();
                    tableCopy.setGroup(null);
                    tableCopy.removeCandidateParentJoin(parentJoin);
                    parentJoin.getParent().removeCandidateChildJoin(parentJoin);
                } else {
                    if(origTable.getParentJoin() != null) {
                        throw new JoinToMultipleParentsException(origTable.getName());
                    }
                    tableCopy.setGroup(null);
                    TableDDL.addJoin(builder, fk, defaultSchema, newName.getSchemaName(), newName.getTableName());
                }
            } else {
                if(fk.getConstraintType() == ConstraintType.DROP) {
                    String name = fk.getConstraintName().getTableName();
                    ForeignKey tableFK = null;
                    for (ForeignKey tfk : tableCopy.getReferencingForeignKeys()) {
                        if (name.equals(tfk.getConstraintName().getTableName())) {
                            tableFK = tfk;
                            break;
                        }
                    }
                    assert tableFK != null : name;
                    tableCopy.removeForeignKey(tableFK);
                } else {
                    TableDDL.addForeignKey(builder, origAIS, fk, defaultSchema, newName.getSchemaName(), newName.getTableName());
                }
            }
        }
        return ddl.alterTable(session, origTable.getName(), tableCopy, columnChanges, indexChanges, context);
    }

    private static void checkForeignKeyAlterColumns(List<TableChange> columnChanges, Collection<Column> columns, ForeignKey foreignKey, Table table) {
        for (Column column : columns) {
            for (TableChange change : columnChanges) {
                if (column.getName().equals(change.getOldName())) {
                    throw new ForeignKeyPreventsAlterColumnException(column.getName(), table.getName(), foreignKey.getConstraintName().getTableName());
                }
            }
        }
    }

    private static void handleModifyColumnNode(ModifyColumnNode modNode, AISBuilder builder, Table tableCopy, TypesTranslator typesTranslator) {
        AkibanInformationSchema aisCopy = tableCopy.getAIS();
        Column column = tableCopy.getColumn(modNode.getColumnName());
        assert column != null : modNode.getColumnName();
        switch(modNode.getNodeType()) {
            case NodeTypes.MODIFY_COLUMN_DEFAULT_NODE:
                if(modNode.isAutoincrementColumn()) {
                    int autoIncType = (int)modNode.getAutoinc_create_or_modify_Start_Increment();
                    switch(autoIncType) {
                        case ColumnDefinitionNode.CREATE_AUTOINCREMENT: {
                            if(column.getIdentityGenerator() != null) {
                                throw new ColumnAlreadyGeneratedException(column);
                            }
                            TableName name = tableCopy.getName();
                            TableDDL.setAutoIncrement(builder, name.getSchemaName(), name.getTableName(), modNode);
                        }
                        break;
                        case ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE:
                            throw new UnsupportedSQLException("SET INCREMENT BY", modNode);
                        case ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE:
                            // Note: Also handled above
                            throw new UnsupportedSQLException("RESTART WITH", modNode);
                        default:
                            throw new IllegalStateException("Unknown autoIncType: " + autoIncType);
                    }
                } else {
                    // DROP DEFAULT will come though as a NULL default, clears both GENERATED and DEFAULT
                    Sequence seq = column.getIdentityGenerator();
                    if(seq != null) {
                        column.setDefaultIdentity(null);
                        column.setIdentityGenerator(null);
                        aisCopy.removeSequence(seq.getSequenceName());
                    }
                    String[] defaultValueFunction = TableDDL.getColumnDefault(modNode, tableCopy.getName().getSchemaName(), tableCopy.getName().getTableName());
                    column.setDefaultValue(defaultValueFunction[0]);
                    column.setDefaultFunction(defaultValueFunction[1]);
                }
            break;
            case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE: // Type only comes from NULL
                column.setType(column.getType().withNullable(true));
            break;
            case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE: // Type only comes from NOT NULL
                column.setType(column.getType().withNullable(false));
            break;
            case NodeTypes.MODIFY_COLUMN_TYPE_NODE: // All but [NOT] NULL comes from type
                {
                    TInstance type = typesTranslator
                        .typeForSQLType(modNode.getType())
                        .withNullable(column.getNullable());
                    if (false) {
                        // TODO: Determine whether compatible, does affect sequence, etc.
                        column.setType(type);
                    }
                    else {
                        tableCopy.dropColumn(modNode.getColumnName());
                        builder.column(tableCopy.getName().getSchemaName(), tableCopy.getName().getTableName(), column.getName(),
                                       column.getPosition(), type, false, // column.getInitialAutoIncrementValue() != null
                                       column.getDefaultValue(), column.getDefaultFunction());
                    }
                }
            break;
            default:
                throw new IllegalStateException("Unexpected node type: " + modNode);
        }
    }

    private static ChangeType findOldName(List<TableChange> changes, String oldName) {
        for(TableChange change : changes) {
            if(oldName.equals(change.getOldName())) {
                return change.getChangeType();
            }
        }
        return null;
    }

    private static String findNewName(List<TableChange> changes, String oldName)
    {
        for(TableChange change : changes) {
            if(oldName.equals(change.getOldName())) {
                return change.getChangeType() == ChangeType.DROP ? null : change.getNewName();
            }
        }
        return oldName;
    }

    private static FKConstraintDefinitionNode newFKDropNode(TableElementNode node,
                                                            String name,
                                                            Boolean isGrouping) {
        try {
            QueryTreeNode fkName = node.getParserContext()
                                       .getNodeFactory()
                                       .getNode(NodeTypes.TABLE_NAME, null, name, node.getParserContext());

            return (FKConstraintDefinitionNode)node.getParserContext()
                                                   .getNodeFactory()
                                                   .getNode(NodeTypes.FK_CONSTRAINT_DEFINITION_NODE,
                                                            fkName,
                                                            ConstraintDefinitionNode.ConstraintType.DROP,
                                                            StatementType.DROP_DEFAULT,
                                                            isGrouping,
                                                            null /*existence*/,
                                                            node.getParserContext());
        } catch(StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
    }

    private static Table copyTable(AISCloner aisCloner, Table origTable, List<TableChange> columnChanges) {
        AkibanInformationSchema aisCopy = aisCloner.clone(origTable.getAIS(), new TableGroupWithoutIndexesSelector(origTable));
        Table tableCopy = aisCopy.getTable(origTable.getName());

        // Remove and recreate. NB: hidden PK/column handled downstream.
        tableCopy.dropColumns();

        int colPos = 0;
        // internal columns are copied after we add new columns
        for(Column origColumn : origTable.getColumns()) {
            String newName = findNewName(columnChanges, origColumn.getName());
            if(newName != null) {
                Column.create(tableCopy, origColumn, newName, colPos++);
            }
        }

        return tableCopy;
    }
    
    private static void copyTableIndexes(Table origTable,
                                         Table tableCopy,
                                         List<TableChange> columnChanges,
                                         List<TableChange> indexChanges) {
        for(TableIndex origIndex : origTable.getIndexesIncludingInternal()) {
            ChangeType indexChange = findOldName(indexChanges, origIndex.getIndexName().getName());
            if(indexChange == ChangeType.DROP) {
                continue;
            }
            TableIndex indexCopy = TableIndex.create(tableCopy, origIndex);
            boolean indexViable = true;
            int pos = 0;
            for(IndexColumn indexColumn : origIndex.getKeyColumns()) {
                String newName = findNewName(columnChanges, indexColumn.getColumn().getName());
                if(newName != null) {
                    IndexColumn.create(indexCopy, tableCopy.getColumn(newName), indexColumn, pos++);
                } else if (indexCopy.isSpatial() &&
                           indexColumn.getPosition() >= origIndex.firstSpatialArgument() &&
                           indexColumn.getPosition() <= origIndex.lastSpatialArgument()) {
                    indexViable = false;
                }
            }
            // DROP and MODIFY detection for indexes handled downstream
            if(indexCopy.getKeyColumns().isEmpty()) {
                indexViable = false;
            }
            if (!indexViable) {
                tableCopy.removeIndexes(Collections.singleton(indexCopy));
            }
        }
    }
    
    private static String indexNameForConstrainName(Table origTable, String name) {
        for(TableIndex ti : origTable.getIndexes()) {
            if ((ti.getConstraintName() != null) && (ti.getConstraintName().getTableName().equals(name))) {
                return ti.getIndexName().getName();
            }
        }
        return null;
    }

    private static boolean isRestartWithNode(ModifyColumnNode modNode) {
        return (modNode.getNodeType() == NodeTypes.MODIFY_COLUMN_DEFAULT_NODE) &&
                modNode.isAutoincrementColumn() &&
                (modNode.getAutoinc_create_or_modify_Start_Increment() == ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE);
    }

    private static class TableGroupWithoutIndexesSelector extends ProtobufWriter.TableSelector {
        private final Table table;
        private final Set<Table> fkTables;

        public TableGroupWithoutIndexesSelector(Table table) {
            this.table = table;
            Collection<ForeignKey> fkeys = table.getReferencingForeignKeys();
            if (fkeys.isEmpty()) {
                fkTables = Collections.emptySet();
            } else {
                fkTables = new HashSet<>(fkeys.size());
                for (ForeignKey fkey : fkeys) {
                    fkTables.add(fkey.getReferencedTable());
                }
            }
        }

        @Override
        public boolean isSelected(Columnar columnar) {
            return columnar.isTable() &&
                ((((Table)columnar).getGroup() == table.getGroup()) ||
                 fkTables.contains(columnar));
        }

        @Override
        public boolean isSelected(Index index) {
            return !(index.isTableIndex() && index.leafMostTable() == table);
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
            return (foreignKey.getReferencingTable() == table);
        }
    }
}
