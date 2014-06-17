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
import com.foundationdb.ais.model.IndexNameGenerator;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.ais.util.TableChange;
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
import com.foundationdb.sql.parser.AlterAddIndexNode;
import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.ColumnDefinitionNode;
import com.foundationdb.sql.parser.ConstraintDefinitionNode;
import com.foundationdb.sql.parser.FKConstraintDefinitionNode;
import com.foundationdb.sql.parser.IndexConstraintDefinitionNode;
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

import static com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import static com.foundationdb.ais.util.TableChange.ChangeType;
import static com.foundationdb.sql.aisddl.DDLHelper.convertName;
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
        if(table == null) {
            throw new NoSuchTableException(tableName);
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
        List<IndexConstraintDefinitionNode> indexDefNodes = new ArrayList<>();

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
                    if (!fkNode.isGrouping() && 
                        (fkNode.getConstraintType() == ConstraintType.DROP)) {
                        if (fkNode.getConstraintName() == null) {
                            Collection<ForeignKey> fkeys = origTable.getReferencingForeignKeys();
                            if (fkeys.size() != 1) {
                                throw new UnsupportedFKIndexException();
                            }
                            try {
                                fkNode.setConstraintName(fkeys.iterator().next().getConstraintName());
                            }
                            catch (StandardException ex) {
                                throw new SQLParserInternalException(ex);
                            }
                        }
                        // Also drop the referencing index.
                        indexChanges.add(TableChange.createDrop(fkNode.getConstraintName().getTableName()));
                    }
                    fkDefNodes.add(fkNode);
                } break;

                case NodeTypes.CONSTRAINT_DEFINITION_NODE: {
                    ConstraintDefinitionNode cdn = (ConstraintDefinitionNode) node;
                    if(cdn.getConstraintType() == ConstraintType.DROP) {
                        String name = cdn.getName();
                        switch(cdn.getVerifyType()) {
                            case PRIMARY_KEY:
                                name = Index.PRIMARY_KEY_CONSTRAINT;
                            break;
                            case DROP: // TODO : Generic Drop
                                boolean found = false;
                                if (checkFKConstraint(origTable, name, node, fkDefNodes)) {
                                    found = true;
                                } else if (name.equalsIgnoreCase(Index.PRIMARY_KEY_CONSTRAINT)) {
                                    name = Index.PRIMARY_KEY_CONSTRAINT;
                                    found = true;
                                } else if (origTable.getIndex(name) != null) {
                                    if (origTable.getIndex(name).isUnique()) {
                                        found = true;
                                    }
                                } else if (origTable.getParentJoin() != null && origTable.getParentJoin().getName().equals(name)) {
                                    found = true;
                                    try {
                                        QueryTreeNode gfkName = node.getParserContext().getNodeFactory().getNode(NodeTypes.TABLE_NAME,
                                                null, 
                                                name,
                                                node.getParserContext());

                                        FKConstraintDefinitionNode fkNode = 
                                                (FKConstraintDefinitionNode)node.getParserContext().getNodeFactory().getNode(
                                                    NodeTypes.FK_CONSTRAINT_DEFINITION_NODE,
                                                    gfkName,
                                                    ConstraintDefinitionNode.ConstraintType.DROP,
                                                    StatementType.DROP_DEFAULT,
                                                    Boolean.TRUE,
                                                    node.getParserContext());
                                        fkDefNodes.add(fkNode);
                                    } catch (StandardException ex) {
                                        // TODO: Anything? 
                                    }
                                    name = null;
                                }
                                if (!found) {
                                    throw new NoSuchConstraintException(origTable.getName(), name);
                                }
                                break;
                            case UNIQUE:
                                Index index = origTable.getIndex(name);
                                if(index == null || !index.isUnique()) {
                                    throw new NoSuchUniqueException(origTable.getName(), cdn.getName());
                                }
                            break;
                            case CHECK:
                                throw new UnsupportedCheckConstraintException();
                        }
                        if (name != null)
                            indexChanges.add(TableChange.createDrop(name));
                    } else {
                        conDefNodes.add(cdn);
                    }
                } break;

                case NodeTypes.AT_ADD_INDEX_NODE:
                    AlterAddIndexNode aain = (AlterAddIndexNode)node;
                    IndexConstraintDefinitionNode icdnT = new IndexConstraintDefinitionNode();
                    com.foundationdb.sql.parser.TableName tableName = new com.foundationdb.sql.parser.TableName();
                    tableName.init(origTable.getName().getSchemaName() , origTable.getName().getTableName());
                    icdnT.init(tableName, aain.getIndexColunmList(), aain.getName(), aain.getJoinType(), aain.getStorageFormat());
                    indexDefNodes.add(icdnT);
                    indexChanges.add(TableChange.createAdd(icdnT.getName()));
                    break;
                    
                case NodeTypes.INDEX_CONSTRAINT_NODE:
                    IndexConstraintDefinitionNode icdn = (IndexConstraintDefinitionNode)node;
                    indexDefNodes.add(icdn);
                    indexChanges.add(TableChange.createAdd(icdn.getName()));
                    break;
                    
                case NodeTypes.AT_DROP_INDEX_NODE:
                    indexChanges.add(TableChange.createDrop(((AlterDropIndexNode)node).getIndexName()));
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
        final TypesTranslator typesTranslator = ddl.getTypesTranslator();
        final AISBuilder builder = new AISBuilder(aisCopy);

        int pos = origTable.getColumns().size();
        for(ColumnDefinitionNode cdn : columnDefNodes) {
            if(cdn instanceof ModifyColumnNode) {
                ModifyColumnNode modNode = (ModifyColumnNode) cdn;
                handleModifyColumnNode(modNode, builder, tableCopy, typesTranslator);
            } else {
                TableDDL.addColumn(builder, typesTranslator, cdn, origTable.getName().getSchemaName(), origTable.getName().getTableName(), pos++);
            }
        }
        copyTableIndexes(origTable, tableCopy, columnChanges, indexChanges);

        IndexNameGenerator indexNamer = DefaultIndexNameGenerator.forTable(tableCopy);
        TableName newName = tableCopy.getName();
        for(ConstraintDefinitionNode cdn : conDefNodes) {
            assert cdn.getConstraintType() != ConstraintType.DROP : cdn;
            String name = TableDDL.addIndex(indexNamer, builder, cdn, newName.getSchemaName(), newName.getTableName(), context);
            indexChanges.add(TableChange.createAdd(name));
        }

        for(IndexConstraintDefinitionNode icdn : indexDefNodes) {
            TableDDL.addIndex(indexNamer, builder, icdn, newName.getSchemaName(), newName.getTableName(), context);            
        }
        
        for(FKConstraintDefinitionNode fk : fkDefNodes) {
            if(fk.isGrouping()) {
                if(fk.getConstraintType() == ConstraintType.DROP) {
                    Join parentJoin = tableCopy.getParentJoin();
                    if(parentJoin == null) {
                        throw new NoSuchGroupingFKException(origTable.getName());
                    }
                    tableCopy.setGroup(null);
                    tableCopy.removeCandidateParentJoin(parentJoin);
                    parentJoin.getParent().removeCandidateChildJoin(parentJoin);
                } else {
                    if(origTable.getParentJoin() != null) {
                        throw new JoinToMultipleParentsException(origTable.getName());
                    }
                    TableName parent = TableDDL.getReferencedName(defaultSchema, fk);
                    if((aisCopy.getTable(parent) == null) && (origAIS.getTable(parent) != null)) {
                        TableDDL.addParentTable(builder, origAIS, fk, defaultSchema, newName.getSchemaName(), newName.getTableName());
                    }
                    tableCopy.setGroup(null);
                    TableDDL.addJoin(builder, fk, defaultSchema, newName.getSchemaName(), newName.getTableName());
                }
            } else {
                if(fk.getConstraintType() == ConstraintType.DROP) {
                    String name = fk.getConstraintName().getTableName();
                    ForeignKey tableFK = null;
                    for (ForeignKey tfk : tableCopy.getReferencingForeignKeys()) {
                        if (name.equals(tfk.getConstraintName())) {
                            tableFK = tfk;
                            break;
                        }
                    }
                    if (tableFK == null) {
                        throw new NoSuchForeignKeyException(name, origTable.getName());
                    }
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
                    throw new ForeignKeyPreventsAlterColumnException(column.getName(), table.getName(), foreignKey.getConstraintName());
                }
            }
        }
    }

    private static void handleModifyColumnNode(ModifyColumnNode modNode, AISBuilder builder, Table tableCopy, TypesTranslator typesTranslator) {
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

    private static void checkColumnsExist(Table table, List<TableChange> changes) {
        for(TableChange c : changes) {
            if(c.getChangeType() != ChangeType.ADD) {
                Column column = table.getColumn(c.getOldName());
                if(column == null) {
                    throw new NoSuchColumnException(c.getOldName());
                }
            }
        }
    }

    private static void checkIndexesExist(Table table, List<TableChange> changes) {
        for(TableChange c : changes) {
            if(c.getChangeType() != ChangeType.ADD) {
                Index index = table.getIndex(c.getOldName());
                if(index == null) {
                    throw new NoSuchIndexException(c.getOldName());
                }
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

    private static String findNewName(List<TableChange> changes, String oldName)
    {
        for(TableChange change : changes) {
            if(oldName.equals(change.getOldName())) {
                return change.getChangeType() == ChangeType.DROP ? null : change.getNewName();
            }
        }
        return oldName;
    }

    private static boolean checkFKConstraint(Table origTable, String name, TableElementNode node, List<FKConstraintDefinitionNode> fkDefNodes ) {
        boolean found = false;
        for (ForeignKey key : origTable.getReferencingForeignKeys()) {
            if (key.getConstraintName().equals(name)) {
                try {
                    QueryTreeNode fkName = node.getParserContext().getNodeFactory().getNode(NodeTypes.TABLE_NAME,
                            null, 
                            name,
                            node.getParserContext());
                    
                    FKConstraintDefinitionNode fkNode = 
                            (FKConstraintDefinitionNode)node.getParserContext().getNodeFactory().getNode(
                                NodeTypes.FK_CONSTRAINT_DEFINITION_NODE,
                                fkName,
                                ConstraintDefinitionNode.ConstraintType.DROP,
                                StatementType.DROP_DEFAULT,
                                Boolean.FALSE,
                                node.getParserContext());
                    fkDefNodes.add(fkNode);
                } catch (StandardException ex) {
                    //TODO: Anything?
                }
                found = true;
                break;
            }
        }
        return found;
    }
    private static Table copyTable(AISCloner aisCloner, Table origTable, List<TableChange> columnChanges) {
        checkColumnsExist(origTable, columnChanges);

        AkibanInformationSchema aisCopy = aisCloner.clone(origTable.getAIS(), new TableGroupWithoutIndexesSelector(origTable));
        Table tableCopy = aisCopy.getTable(origTable.getName());

        // Remove and recreate. NB: hidden PK/column handled downstream.
        tableCopy.dropColumns();

        int colPos = 0;
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
        checkIndexesExist(origTable, indexChanges);

        for(TableIndex origIndex : origTable.getIndexes()) {
            ChangeType indexChange = findOldName(indexChanges, origIndex.getIndexName().getName());
            if(indexChange == ChangeType.DROP) {
                continue;
            }
            TableIndex indexCopy = TableIndex.create(tableCopy, origIndex);
            int pos = 0;
            for(IndexColumn indexColumn : origIndex.getKeyColumns()) {
                String newName = findNewName(columnChanges, indexColumn.getColumn().getName());
                if(newName != null) {
                    IndexColumn.create(indexCopy, tableCopy.getColumn(newName), indexColumn, pos++);
                }
            }
            // DROP and MODIFY detection for indexes handled downstream
            if(indexCopy.getKeyColumns().isEmpty()) {
                tableCopy.removeIndexes(Collections.singleton(indexCopy));
            }
        }
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
            return false;
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
