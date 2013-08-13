/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.aisddl;

import com.akiban.ais.model.Sequence;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.ColumnAlreadyGeneratedException;
import com.akiban.server.error.ColumnNotGeneratedException;
import com.akiban.sql.parser.AlterTableRenameColumnNode;
import com.akiban.sql.parser.AlterTableRenameNode;
import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.DefaultIndexNameGenerator;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexNameGenerator;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.ais.util.TableChange;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.JoinToMultipleParentsException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchGroupingFKException;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchUniqueException;
import com.akiban.server.error.UnsupportedCheckConstraintException;
import com.akiban.server.error.UnsupportedFKIndexException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.ColumnDefinitionNode;
import com.akiban.sql.parser.ConstraintDefinitionNode;
import com.akiban.sql.parser.FKConstraintDefinitionNode;
import com.akiban.sql.parser.ModifyColumnNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.TableElementList;
import com.akiban.sql.parser.TableElementNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.akiban.ais.util.TableChangeValidator.ChangeLevel;
import static com.akiban.ais.util.TableChange.ChangeType;
import static com.akiban.sql.aisddl.DDLHelper.convertName;
import static com.akiban.sql.parser.ConstraintDefinitionNode.ConstraintType;

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
        final UserTable table = curAIS.getUserTable(tableName);
        checkExists(tableName, table);

        if (alterTable.isUpdateStatistics()) {
            Collection<String> indexes = null;
            if (!alterTable.isUpdateStatisticsAll())
                indexes = Collections.singletonList(alterTable.getIndexNameForUpdateStatistics());
            ddlFunctions.updateTableStatistics(session, tableName, indexes);
            return null;
        }

        if (alterTable.isTruncateTable()) {
            dmlFunctions.truncateTable(session, table.getTableId());
            return null;
        }

        ChangeLevel level = processAlter(session, ddlFunctions, defaultSchemaName, table, alterTable.tableElementList, context);
        if (level != null) {
            return level;
        }

        throw new UnsupportedSQLException (alterTable.statementToString(), alterTable);
    }

    private static void checkExists(TableName tableName, UserTable table) {
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
    }

    private static ChangeLevel processAlter(Session session, DDLFunctions ddl, String defaultSchema, UserTable table,
                                            TableElementList elements, QueryContext context) {
        // Should never come this way from the parser, but be defensive
        if((elements == null) || elements.isEmpty()) {
            return null;
        }

        List<TableChange> columnChanges = new ArrayList<>();
        List<TableChange> indexChanges = new ArrayList<>();
        List<ColumnDefinitionNode> columnDefNodes = new ArrayList<>();
        List<FKConstraintDefinitionNode> fkDefNodes= new ArrayList<>();
        List<ConstraintDefinitionNode> conDefNodes = new ArrayList<>();

        for(TableElementNode node : elements) {
            switch(node.getNodeType()) {
                case NodeTypes.COLUMN_DEFINITION_NODE: {
                    ColumnDefinitionNode cdn = (ColumnDefinitionNode) node;
                    columnChanges.add(TableChange.createAdd(cdn.getColumnName()));
                    columnDefNodes.add(cdn);
                } break;

                case NodeTypes.DROP_COLUMN_NODE: {
                    String columnName = ((ModifyColumnNode)node).getColumnName();
                    columnChanges.add(TableChange.createDrop(columnName));
                } break;

                case NodeTypes.MODIFY_COLUMN_DEFAULT_NODE:
                case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE:
                case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE:
                case NodeTypes.MODIFY_COLUMN_TYPE_NODE: {
                    String columnName = ((ModifyColumnNode)node).getColumnName();
                    columnChanges.add(TableChange.createModify(columnName, columnName));
                    columnDefNodes.add((ColumnDefinitionNode) node);
                } break;

                case NodeTypes.FK_CONSTRAINT_DEFINITION_NODE: {
                    FKConstraintDefinitionNode fkNode = (FKConstraintDefinitionNode) node;
                    if(fkNode.isGrouping()) {
                        fkDefNodes.add(fkNode);
                    } else {
                        throw new UnsupportedFKIndexException();
                    }
                } break;

                case NodeTypes.CONSTRAINT_DEFINITION_NODE: {
                    ConstraintDefinitionNode cdn = (ConstraintDefinitionNode) node;
                    if(cdn.getConstraintType() == ConstraintType.DROP) {
                        String name = cdn.getName();
                        switch(cdn.getVerifyType()) {
                            case PRIMARY_KEY:
                                name = Index.PRIMARY_KEY_CONSTRAINT;
                            break;
                            // TODO: Should add flags to AlterTableChange to avoid checks in multiple places
                            case DROP:
                            case UNIQUE:
                                Index index = table.getIndex(name);
                                if((index != null) && !index.isUnique()) {
                                    throw new NoSuchUniqueException(table.getName(), cdn.getName());
                                }
                            break;
                            case CHECK:
                                throw new UnsupportedCheckConstraintException();
                        }
                        indexChanges.add(TableChange.createDrop(name));
                    } else {
                        conDefNodes.add(cdn);
                    }
                } break;

                case NodeTypes.AT_RENAME_NODE:
                    TableName newName = DDLHelper.convertName(defaultSchema,
                                                              ((AlterTableRenameNode)node).newName());
                    TableName oldName = table.getName();
                    ddl.renameTable(session, oldName, newName);
                    return ChangeLevel.METADATA;
                    
                case NodeTypes.AT_RENAME_COLUMN_NODE:
                    AlterTableRenameColumnNode alterRenameCol = (AlterTableRenameColumnNode) node;
                    String oldColName = alterRenameCol.getName();
                    String newColName = alterRenameCol.newName();
                    final Column oldCol = table.getColumn(oldColName);
                    if (oldCol == null)
                        throw new NoSuchColumnException(oldColName);
                    columnChanges.add(TableChange.createModify(oldColName, newColName));
                break;

                default:
                    return null; // Something unsupported
            }
        }
        
        final AkibanInformationSchema origAIS = table.getAIS();
        final UserTable tableCopy = copyTable(table, columnChanges);
        final AkibanInformationSchema aisCopy = tableCopy.getAIS();
        final AISBuilder builder = new AISBuilder(aisCopy);

        int pos = table.getColumns().size();
        for(ColumnDefinitionNode cdn : columnDefNodes) {
            if(cdn instanceof ModifyColumnNode) {
                ModifyColumnNode modNode = (ModifyColumnNode) cdn;
                handleModifyColumnNode(modNode, builder, tableCopy);
            } else {
                TableDDL.addColumn(builder, cdn, table.getName().getSchemaName(), table.getName().getTableName(), pos++);
            }
        }
        copyTableIndexes(table, tableCopy, columnChanges, indexChanges);

        IndexNameGenerator indexNamer = DefaultIndexNameGenerator.forTable(tableCopy);
        TableName newName = tableCopy.getName();
        for(ConstraintDefinitionNode cdn : conDefNodes) {
            assert cdn.getConstraintType() != ConstraintType.DROP : cdn;
            String name = TableDDL.addIndex(indexNamer, builder, cdn, newName.getSchemaName(), newName.getTableName(), context);
            indexChanges.add(TableChange.createAdd(name));
        }

        for(FKConstraintDefinitionNode fk : fkDefNodes) {
            if(fk.getConstraintType() == ConstraintType.DROP) {
                Join parentJoin = tableCopy.getParentJoin();
                if(parentJoin == null) {
                    throw new NoSuchGroupingFKException(table.getName());
                }
                tableCopy.setGroup(null);
                tableCopy.removeCandidateParentJoin(parentJoin);
                parentJoin.getParent().removeCandidateChildJoin(parentJoin);
            } else {
                if(table.getParentJoin() != null) {
                    throw new JoinToMultipleParentsException(table.getName());
                }
                TableName parent = TableDDL.getReferencedName(defaultSchema, fk);
                if((aisCopy.getUserTable(parent) == null) && (origAIS.getUserTable(parent) != null)) {
                    TableDDL.addParentTable(builder, origAIS, fk, defaultSchema);
                }
                tableCopy.setGroup(null);
                TableDDL.addJoin(builder, fk, defaultSchema, newName.getSchemaName(), newName.getTableName());
            }
        }

        return ddl.alterTable(session, table.getName(), tableCopy, columnChanges, indexChanges, context);
    }

    private static void handleModifyColumnNode(ModifyColumnNode modNode, AISBuilder builder, UserTable tableCopy) {
        AkibanInformationSchema aisCopy = tableCopy.getAIS();
        Column column = tableCopy.getColumn(modNode.getColumnName());
        if(column == null) {
            throw new NoSuchColumnException(modNode.getColumnName());
        }
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
                        case ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE: {
                            Sequence curSeq = column.getIdentityGenerator();
                            if(curSeq == null) {
                                throw new ColumnNotGeneratedException(column);
                            }
                            aisCopy.removeSequence(curSeq.getSequenceName());
                            Sequence newSeq = Sequence.create(aisCopy,
                                                              curSeq.getSchemaName(),
                                                              curSeq.getSequenceName().getTableName(),
                                                              curSeq.getStartsWith(),
                                                              modNode.getAutoincrementIncrement(),
                                                              curSeq.getMinValue(),
                                                              curSeq.getMaxValue(),
                                                              curSeq.isCycle());
                            aisCopy.addSequence(newSeq);
                            column.setIdentityGenerator(newSeq);
                        }
                        break;
                        case ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE:
                            // Requires Accumulator reset
                            throw new UnsupportedSQLException("Not yet implemented", modNode);
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
                column.setNullable(true);
            break;
            case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE: // Type only comes from NOT NULL
                column.setNullable(false);
            break;
            case NodeTypes.MODIFY_COLUMN_TYPE_NODE:
                tableCopy.dropColumn(modNode.getColumnName());
                TableDDL.addColumn(builder, tableCopy.getName().getSchemaName(), tableCopy.getName().getTableName(),
                                   column.getName(), column.getPosition(), modNode.getType(), column.getNullable(),
                                   column.getDefaultValue(), column.getDefaultFunction());
            break;
            default:
                throw new IllegalStateException("Unexpected node type: " + modNode);
        }
    }

    private static void checkColumnChange(UserTable table, String columnName) {
        Column column = table.getColumn(columnName);
        if(column == null) {
            throw new NoSuchColumnException(columnName);
        }
    }

    private static void checkIndexChange(UserTable table, String indexName, boolean isNew) {
        Index index = table.getIndex(indexName);
        if(index == null && !isNew) {
            if(Index.PRIMARY_KEY_CONSTRAINT.equals(indexName)) {
                throw new NoSuchIndexException(indexName);
            } else {
                throw new NoSuchUniqueException(table.getName(), indexName);
            }
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

    private static String getNewName(List<TableChange> changes, String oldName)
    {
        for (TableChange change : changes)
            if (oldName.equals(change.getOldName()))
                return change.getChangeType() == ChangeType.DROP
                            ? null
                            : change.getNewName();
        return oldName;
    }

    private static UserTable copyTable(UserTable origTable, List<TableChange> columnChanges) {
        for(TableChange change : columnChanges) {
            if(change.getChangeType() != ChangeType.ADD) {
                checkColumnChange(origTable, change.getOldName());
            }
        }

        AkibanInformationSchema aisCopy = AISCloner.clone(origTable.getAIS(), new GroupSelector(origTable.getGroup()));
        UserTable tableCopy = aisCopy.getUserTable(origTable.getName());

        // Remove all and recreate (note: hidden PK and column are handled by DDL interface)
        tableCopy.dropColumns();
        tableCopy.removeIndexes(tableCopy.getIndexesIncludingInternal());
        tableCopy.getGroup().removeIndexes(tableCopy.getGroup().getIndexes());

        int colPos = 0;
        for(Column origColumn : origTable.getColumns()) {
            
            String newName = getNewName(columnChanges, origColumn.getName());
            if (newName != null)
                Column.create(tableCopy, origColumn, newName, colPos++);
        }

        return tableCopy;
    }
    
    private static void copyTableIndexes(UserTable origTable, UserTable tableCopy,
                                         List<TableChange> columnChanges, List<TableChange> indexChanges) {
        for(TableChange change : indexChanges) {
            checkIndexChange(origTable, change.getOldName(), change.getChangeType() == ChangeType.ADD);
        }

        Collection<TableIndex> indexesToDrop = new ArrayList<>();
        for(TableIndex origIndex : origTable.getIndexes()) {
            ChangeType indexChange = findOldName(indexChanges, origIndex.getIndexName().getName());
            if(indexChange == ChangeType.DROP) {
                continue;
            }
            TableIndex indexCopy = TableIndex.create(tableCopy, origIndex);
            boolean didModify = false;
            int pos = 0;
            
            for(IndexColumn indexColumn : origIndex.getKeyColumns()) {
                
                String newName = getNewName(columnChanges, indexColumn.getColumn().getName());
                if (newName != null)
                    IndexColumn.create(indexCopy, tableCopy.getColumn(newName), indexColumn, pos++);
                else
                    didModify = true;
            }

            // Automatically mark indexes for drop or modification
            if(indexCopy.getKeyColumns().isEmpty()) {
                indexesToDrop.add(indexCopy);
                indexChanges.add(TableChange.createDrop(indexCopy.getIndexName().getName()));
            } else if(didModify && (indexChange == null)) {
                String indexName = indexCopy.getIndexName().getName();
                indexChanges.add(TableChange.createModify(indexName, indexName));
            }
        }

        tableCopy.removeIndexes(indexesToDrop);
    }

    private static class GroupSelector extends ProtobufWriter.TableSelector {
        private final Group group;

        public GroupSelector(Group group) {
            this.group = group;
        }

        @Override
        public boolean isSelected(Columnar columnar) {
            return columnar.isTable() && ((Table)columnar).getGroup() == group;
        }
    }
}
