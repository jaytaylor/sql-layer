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

package com.akiban.sql.aisddl;

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AISTableNameChanger;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.ais.util.TableChange;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.JoinColumnMismatchException;
import com.akiban.server.error.JoinToProtectedTableException;
import com.akiban.server.error.JoinToUnknownTableException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchUniqueException;
import com.akiban.server.error.ProtectedIndexException;
import com.akiban.server.error.UnsupportedCheckConstraintException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.dxl.DXLFunctionsHook;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.ColumnDefinitionNode;
import com.akiban.sql.parser.ConstraintDefinitionNode;
import com.akiban.sql.parser.FKConstraintDefinitionNode;
import com.akiban.sql.parser.ModifyColumnNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.ResultColumnList;
import com.akiban.sql.parser.TableElementList;
import com.akiban.sql.parser.TableElementNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.akiban.ais.util.TableChange.ChangeType;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction.ALTER_TABLE_TEMP_TABLE;
import static com.akiban.sql.aisddl.DDLHelper.convertName;
import static com.akiban.sql.parser.ConstraintDefinitionNode.ConstraintType;
import static com.akiban.util.Exceptions.throwAlways;

public class AlterTableDDL {
    private static final String MULTI_GROUP_ERROR_MSG = "Cannot add table %s to multiple groups";
    private static final String NON_LEAF_ERROR_MSG = "Cannot drop group from %s table %s";
    private static final String GROUP_CHANGE_ERROR_MSG = "Cannot change column %s from %s (primary or grouping key)";
    static final String TEMP_TABLE_NAME_NEW = "__ak_alter_temp_new";
    static final String TEMP_TABLE_NAME_OLD = "__ak_alter_temp_old";

    private AlterTableDDL() {}

    public static void alterTable(DXLFunctionsHook hook,
                                  DDLFunctions ddlFunctions,
                                  DMLFunctions dmlFunctions,
                                  Session session,
                                  TableCopier tableCopier,
                                  String defaultSchemaName,
                                  AlterTableNode alterTable) {
        AkibanInformationSchema curAIS = ddlFunctions.getAIS(session);
        final TableName tableName = convertName(defaultSchemaName, alterTable.getObjectName());
        final UserTable table = curAIS.getUserTable(tableName);
        checkExists(tableName, table);

        if (alterTable.isUpdateStatistics()) {
            Collection<String> indexes = null;
            if (!alterTable.isUpdateStatisticsAll())
                indexes = Collections.singletonList(alterTable.getIndexNameForUpdateStatistics());
            ddlFunctions.updateTableStatistics(session, tableName, indexes);
            return;
        }

        if (alterTable.isTruncateTable()) {
            dmlFunctions.truncateTable(session, table.getTableId());
            return;
        }

        if (doGenericAlter(session, ddlFunctions, table, alterTable.tableElementList)) {
            return;
        }

        FKConstraintDefinitionNode fkNode = getOnlyAddGFKNode(alterTable);
        ConstraintDefinitionNode conNode = getOnlyDropGFKNode(alterTable);
        if((fkNode != null) || (conNode != null)) {
            Throwable thrown = null;
            hook.hookFunctionIn(session, ALTER_TABLE_TEMP_TABLE);
            try {
                if(fkNode != null) {
                    TableName refName = convertName(defaultSchemaName, fkNode.getRefTableName());
                    doAddGroupingFK(fkNode, tableName, refName, session, ddlFunctions, tableCopier);
                } else {
                    doDropGroupingFK(tableName, session, ddlFunctions, tableCopier);
                }
            } catch(Throwable t) {
                thrown = t;
                hook.hookFunctionCatch(session, ALTER_TABLE_TEMP_TABLE, t);
                throw throwAlways(t);
            } finally {
                hook.hookFunctionFinally(session, ALTER_TABLE_TEMP_TABLE, thrown);
            }
            return;
        }

        throw new UnsupportedSQLException (alterTable.statementToString(), alterTable);
    }

    private static void checkExists(TableName tableName, UserTable table) {
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
    }

    private static void doAddGroupingFK(FKConstraintDefinitionNode fk, TableName tableName, TableName refName,
                                        Session session, DDLFunctions ddl, TableCopier copier) {
        // Reacquire and check, now inside lock
        AkibanInformationSchema ais = ddl.getAIS(session);
        final UserTable table = ais.getUserTable(tableName);
        checkExists(tableName, table);

        if(!table.isRoot() || !table.getChildJoins().isEmpty()) {
            throw new UnsupportedSQLException(String.format(MULTI_GROUP_ERROR_MSG, tableName), null);
        }
        final UserTable refTable = ais.getUserTable(refName);
        if(refTable == null) {
            throw new JoinToUnknownTableException(tableName, refName);
        }
        if(TableName.INFORMATION_SCHEMA.equals(refName.getSchemaName())) {
            throw new JoinToProtectedTableException(tableName, refName);
        }

        AkibanInformationSchema aisCopy = AISCloner.clone(
                ais,
                new ProtobufWriter.TableSelector() {
                    @Override
                    public boolean isSelected(Columnar columnar) {
                        if(columnar.isView()) return false;
                        UserTable uTable = (UserTable)columnar;
                        return (columnar == table) || (uTable.getGroup() == refTable.getGroup());
                    }
                }
        );

        TableName tempName1 = new TableName(tableName.getSchemaName(), TEMP_TABLE_NAME_NEW);

        UserTable newTable = aisCopy.getUserTable(tableName);
        UserTable newRefTable = aisCopy.getUserTable(refName);
        new AISTableNameChanger(newTable, tempName1).doChange();

        Join join = Join.create(aisCopy, "temp_name", newRefTable, newTable);
        join.setGroup(newRefTable.getGroup());
        newTable.setGroup(join.getGroup());

        String[] columns = columnNamesFromListOrPK(fk.getColumnList(), null); // No defaults for child table
        String[] refColumns = columnNamesFromListOrPK(fk.getRefResultColumnList(), refTable.getPrimaryKey());
        if(columns.length != refColumns.length) {
            throw new JoinColumnMismatchException(columns.length, tableName, refName, refColumns.length);
        }

        for(int i = 0; i < refColumns.length;++i) {
            JoinColumn.create(join, checkGetColumn(newRefTable, refColumns[i]), checkGetColumn(newTable, columns[i]));
        }

        createRenameCopyDrop(session, ddl, copier, newTable, tableName);
    }

    private static void doDropGroupingFK(TableName tableName, Session session, DDLFunctions ddl, TableCopier copier) {
        // Reacquire and check, now inside lock
        AkibanInformationSchema ais = ddl.getAIS(session);
        final UserTable table = ais.getUserTable(tableName);
        checkExists(tableName, table);

        if(table.isRoot()) {
            throw new UnsupportedSQLException(String.format(NON_LEAF_ERROR_MSG, "root", tableName), null);
        }
        if(!table.getChildJoins().isEmpty()) {
            throw new UnsupportedSQLException(String.format(NON_LEAF_ERROR_MSG, "non-leaf", tableName), null);
        }

        AkibanInformationSchema aisCopy = AISCloner.clone(
                ais,
                new ProtobufWriter.TableSelector() {
                    @Override
                    public boolean isSelected(Columnar columnar) {
                        if(columnar.isView()) return false;
                        UserTable uTable = (UserTable)columnar;
                        return uTable.getGroup() == table.getGroup();
                    }
                }
        );

        TableName tempName1 = new TableName(tableName.getSchemaName(), TEMP_TABLE_NAME_NEW);
        UserTable newTable = aisCopy.getUserTable(tableName);
        new AISTableNameChanger(newTable, tempName1).doChange();
        // Dis-associate the tables
        Join join = newTable.getParentJoin();
        join.getParent().removeCandidateChildJoin(join);
        newTable.removeCandidateParentJoin(join);
        newTable.setGroup(null);

        createRenameCopyDrop(session, ddl, copier, newTable, tableName);
    }

    private static void createRenameCopyDrop(Session session, DDLFunctions ddl, TableCopier copier,
                                             UserTable newTable, TableName originalName) {
        TableName tempName1 = newTable.getName();
        TableName tempName2 = new TableName(originalName.getSchemaName(), TEMP_TABLE_NAME_OLD);
        ddl.createTable(session, newTable);
        AkibanInformationSchema newAIS = ddl.getAIS(session); // create just changed it
        copier.copyFullTable(newAIS, originalName, newTable.getName());
        ddl.renameTable(session, originalName, tempName2);
        ddl.renameTable(session, tempName1, originalName);
        ddl.dropTable(session, tempName2);
    }

    private static boolean doGenericAlter(Session session, DDLFunctions ddl, UserTable table, TableElementList elementList) {
        if(elementList == null) {
            return false;
        }

        List<TableChange> columnChanges = new ArrayList<TableChange>();
        List<TableChange> indexChanges = new ArrayList<TableChange>();
        List<ColumnDefinitionNode> columnDefNodes = new ArrayList<ColumnDefinitionNode>();
        List<ConstraintDefinitionNode> indexDefNodes = new ArrayList<ConstraintDefinitionNode>();

        boolean primaryChanging = false;
        for(TableElementNode node : elementList) {
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

                case NodeTypes.MODIFY_COLUMN_TYPE_NODE: {
                    String columnName = ((ModifyColumnNode)node).getColumnName();
                    columnChanges.add(TableChange.createModify(columnName, columnName));
                    columnDefNodes.add((ColumnDefinitionNode) node);
                } break;

                case NodeTypes.FK_CONSTRAINT_DEFINITION_NODE: {
                    FKConstraintDefinitionNode fkNode = (FKConstraintDefinitionNode) node;
                    if(fkNode.isGrouping()) {
                        return false; // Not yet supported by generic
                    }
                }
                // Fall
                case NodeTypes.CONSTRAINT_DEFINITION_NODE: {
                    ConstraintDefinitionNode cdn = (ConstraintDefinitionNode) node;
                    if(cdn.getConstraintType() == ConstraintType.DROP) {
                        String name = cdn.getName();
                        switch(cdn.getVerifyType()) {
                            case PRIMARY_KEY:
                                primaryChanging = true;
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
                        indexDefNodes.add((ConstraintDefinitionNode)node);
                    }
                } break;

                default:
                    return false; // Something unsupported
            }
        }

        UserTable tableCopy = copyTable(table, columnChanges, indexChanges);
        AISBuilder builder = new AISBuilder(tableCopy.getAIS());

        for(ColumnDefinitionNode cdn : columnDefNodes) {
            if(cdn instanceof ModifyColumnNode) {
                // TODO: Assumes SET DATA TYPE, expand as needed
                assert cdn.getNodeType() == NodeTypes.MODIFY_COLUMN_TYPE_NODE;
                Column column = tableCopy.dropColumn(cdn.getColumnName());
                int pos = column.getPosition();
                TableDDL.addColumn(builder, table.getName().getSchemaName(), table.getName().getTableName(),
                                   column.getName(), pos, cdn.getType(), column.getNullable(),
                                   column.getInitialAutoIncrementValue() != null);
            } else {
                int pos = table.getColumns().size();
                TableDDL.addColumn(builder, cdn, table.getName().getSchemaName(), table.getName().getTableName(), pos);
            }
        }

        for(ConstraintDefinitionNode cdn : indexDefNodes) {
            assert cdn.getConstraintType() != ConstraintType.DROP;
            String name = TableDDL.addIndex(builder, cdn, table.getName().getSchemaName(), table.getName().getTableName());
            indexChanges.add(TableChange.createAdd(name));
            primaryChanging |= Index.PRIMARY_KEY_CONSTRAINT.equals(name);
        }

        if(primaryChanging) {
            for(Index index : tableCopy.getIndexes()) {
                String name = index.getIndexName().getName();
                if(!containsNewName(indexChanges, name)) {
                    indexChanges.add(TableChange.createModify(name, name));
                }
            }
        }

        ddl.alterTable(session, table.getName(), tableCopy, columnChanges, indexChanges);
        return true;
    }

    private static void checkColumnChange(UserTable table, String columnName) {
        Column column = table.getColumn(columnName);
        if(column == null) {
            throw new NoSuchColumnException(columnName);
        }
        // Reject until automatic grouping changes supported
        Join join = table.getParentJoin();
        if(join != null) {
            for(JoinColumn joinColumn : join.getJoinColumns()) {
                if(columnName.equals(joinColumn.getChild().getName())) {
                    throw new UnsupportedSQLException(String.format(GROUP_CHANGE_ERROR_MSG, columnName, table.getName()), null);
                }
            }
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

    private static UserTable copyTable(UserTable origTable, List<TableChange> columnChanges, List<TableChange> indexChanges) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        UserTable tableCopy = UserTable.create(ais, origTable);

        for(TableChange change : columnChanges) {
            if(change.getChangeType() != ChangeType.ADD) {
                checkColumnChange(origTable, change.getOldName());
            }
        }

        for(TableChange change : indexChanges) {
            checkIndexChange(origTable, change.getOldName(), change.getChangeType() == ChangeType.ADD);
        }

        int colPos = 0;
        for(Column origColumn : origTable.getColumns()) {
            String columnName = origColumn.getName();
            if(findOldName(columnChanges, columnName) != ChangeType.DROP) {
                Column.create(tableCopy, origColumn, colPos++);
            }
        }

        Collection<TableIndex> indexesToDrop = new ArrayList<TableIndex>();
        for(TableIndex origIndex : origTable.getIndexes()) {
            ChangeType indexChange = findOldName(indexChanges, origIndex.getIndexName().getName());
            if(indexChange == ChangeType.DROP) {
                continue;
            }
            TableIndex indexCopy = TableIndex.create(tableCopy, origIndex);
            boolean didModify = false;
            int pos = 0;
            for(IndexColumn indexColumn : origIndex.getKeyColumns()) {
                ChangeType change = findOldName(columnChanges, indexColumn.getColumn().getName());
                if(change != ChangeType.DROP) {
                    didModify |= (change == ChangeType.MODIFY);
                    IndexColumn.create(indexCopy, tableCopy.getColumn(indexColumn.getColumn().getName()), indexColumn, pos++);
                } else {
                    didModify = true;
                }
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

        if(!indexesToDrop.isEmpty()) {
            tableCopy.removeIndexes(indexesToDrop);
        }

        Join origJoin = origTable.getParentJoin();
        if(origJoin != null) {
            UserTable origParent = origJoin.getParent();
            // Need just a stub, only need referenced columns
            UserTable parentCopy = UserTable.create(ais, origParent);
            Join joinCopy = Join.create(ais, origJoin.getName(), parentCopy, tableCopy);
            for(JoinColumn origJoinCol : origJoin.getJoinColumns()) {
                Column origParentCol = origParent.getColumn(origJoinCol.getParent().getName());
                Column parentColCopy = Column.create(parentCopy, origParentCol, null);
                JoinColumn.create(joinCopy, parentColCopy, tableCopy.getColumn(origJoinCol.getChild().getName()));
            }

            Group groupCopy = Group.create(ais, origParent.getGroup().getName());
            parentCopy.setGroup(groupCopy);
            tableCopy.setGroup(groupCopy);
            joinCopy.setGroup(groupCopy);
        }

        return tableCopy;
    }

    private static boolean containsNewName(List<TableChange> changes, String name) {
        for(TableChange change : changes) {
            if(name.equals(change.getNewName())) {
                return true;
            }
        }
        return false;
    }

    private static FKConstraintDefinitionNode getOnlyAddGFKNode(AlterTableNode node) {
        if(node.tableElementList == null) {
            return null;
        }
        if(node.tableElementList.size() != 1) {
            return null;
        }
        TableElementNode elementNode = node.tableElementList.get(0);
        if(elementNode instanceof FKConstraintDefinitionNode) {
            FKConstraintDefinitionNode fkNode = (FKConstraintDefinitionNode)elementNode;
            if((fkNode.getConstraintType() == ConstraintType.FOREIGN_KEY) &&
               fkNode.isGrouping()) {
                return fkNode;
            }
        }
        return null;
    }

    private static ConstraintDefinitionNode getOnlyDropGFKNode(AlterTableNode node) {
        if(node.tableElementList == null) {
            return null;
        }
        if(node.tableElementList.size() != 1) {
            return null;
        }
        TableElementNode elementNode = node.tableElementList.get(0);
        if(elementNode instanceof FKConstraintDefinitionNode) {
            FKConstraintDefinitionNode fkNode = (FKConstraintDefinitionNode)elementNode;
            if((fkNode.getConstraintType() == ConstraintType.DROP) &&
               fkNode.isGrouping()) {
                return fkNode;
            }
        }
        return null;
    }

    private static Column checkGetColumn(UserTable table, String columnName) {
        Column column = table.getColumn(columnName);
        if(column == null) {
            throw new NoSuchColumnException(columnName);
        }
        return column;
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
}
