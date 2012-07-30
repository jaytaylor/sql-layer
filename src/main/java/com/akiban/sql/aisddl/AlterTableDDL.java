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
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.server.api.AlterTableChange;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.JoinColumnMismatchException;
import com.akiban.server.error.JoinToProtectedTableException;
import com.akiban.server.error.JoinToUnknownTableException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;
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

import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction.ALTER_TABLE_TEMP_TABLE;
import static com.akiban.sql.aisddl.DDLHelper.convertName;
import static com.akiban.util.Exceptions.throwAlways;
import static com.akiban.server.api.AlterTableChange.ChangeType;

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
                new ProtobufWriter.TableAllIndexSelector() {
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
                new ProtobufWriter.TableAllIndexSelector() {
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

        List<AlterTableChange> columnChanges = new ArrayList<AlterTableChange>();
        List<AlterTableChange> indexChanges = new ArrayList<AlterTableChange>();
        List<ColumnDefinitionNode> addsOrChanges = new ArrayList<ColumnDefinitionNode>();
        for(TableElementNode node : elementList) {
            // Modify extends Definition, must come first
            if((node instanceof ModifyColumnNode)) {
                String columnName = ((ModifyColumnNode)node).getColumnName();
                switch(node.getNodeType()) {
                    case NodeTypes.DROP_COLUMN_NODE:
                        columnChanges.add(AlterTableChange.createDrop(columnName));
                    break;
                    case NodeTypes.MODIFY_COLUMN_TYPE_NODE:
                        columnChanges.add(AlterTableChange.createModify(columnName, columnName));
                        addsOrChanges.add((ColumnDefinitionNode) node);
                    break;
                    default:
                        return false; // TODO: Some column metadata change
                }
            }
            else if(node instanceof ColumnDefinitionNode) {
                ColumnDefinitionNode cdn = (ColumnDefinitionNode) node;
                columnChanges.add(AlterTableChange.createAdd(cdn.getColumnName()));
                addsOrChanges.add(cdn);
            } else {
                return false; // TODO: Other alters
            }
        }

        UserTable tableCopy = copyTable(table, columnChanges, indexChanges);
        AISBuilder builder = new AISBuilder(tableCopy.getAIS());

        for(ColumnDefinitionNode cdn : addsOrChanges) {
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

        ddl.alterTable(session, table.getName(), tableCopy, columnChanges, indexChanges);
        return true;
    }

    private static void checkColumnChange(UserTable table, String columnName) {
        Column column = table.getColumn(columnName);
        if(column == null) {
            throw new NoSuchColumnException(columnName);
        }
        // Reject PK changes until supported
        PrimaryKey pk = table.getPrimaryKey();
        if(pk != null) {
            for(IndexColumn indexColumn : pk.getIndex().getKeyColumns()) {
                if(columnName.equals(indexColumn.getColumn().getName())) {
                    throw new UnsupportedSQLException(String.format(GROUP_CHANGE_ERROR_MSG, columnName, table.getName()), null);
                }
            }
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

    private static boolean containsColumn(List<AlterTableChange> changes, String columnName, ChangeType type) {
        for(AlterTableChange change : changes) {
            if(columnName.equals(change.getOldName())) {
                return change.getChangeType() == type;
            }
        }
        return false;
    }

    private static UserTable copyTable(UserTable origTable, List<AlterTableChange> columnChanges, List<AlterTableChange> indexChanges) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        UserTable tableCopy = UserTable.create(ais, origTable);

        for(AlterTableChange change : columnChanges) {
            if(change.getChangeType() != ChangeType.ADD) {
                checkColumnChange(origTable, change.getOldName());
            }
        }

        int colPos = 0;
        for(Column origColumn : origTable.getColumns()) {
            String columnName = origColumn.getName();
            if(!containsColumn(columnChanges, columnName, ChangeType.DROP)) {
                Column.create(tableCopy, origColumn, colPos++);
            }
        }

        Collection<TableIndex> indexesToDrop = new ArrayList<TableIndex>();
        for(TableIndex origIndex : origTable.getIndexes()) {
            TableIndex indexCopy = TableIndex.create(tableCopy, origIndex);
            boolean didModify = false;
            int pos = 0;
            for(IndexColumn indexColumn : origIndex.getKeyColumns()) {
                if(!containsColumn(columnChanges, indexColumn.getColumn().getName(), ChangeType.DROP)) {
                    didModify |= containsColumn(columnChanges, indexColumn.getColumn().getName(), ChangeType.MODIFY);
                    IndexColumn.create(indexCopy, tableCopy.getColumn(indexColumn.getColumn().getName()), indexColumn, pos++);
                } else {
                    didModify = true;
                }
            }

            // Automatically mark indexes for drop or modification
            if(indexCopy.getKeyColumns().isEmpty()) {
                indexesToDrop.add(indexCopy);
                indexChanges.add(AlterTableChange.createDrop(indexCopy.getIndexName().getName()));
            } else if(didModify) {
                String indexName = indexCopy.getIndexName().getName();
                indexChanges.add(AlterTableChange.createModify(indexName, indexName));
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
            if(fkNode.isGrouping()) {
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
        if(elementNode instanceof ConstraintDefinitionNode) {
            ConstraintDefinitionNode conNode = (ConstraintDefinitionNode)elementNode;
            if(ConstraintDefinitionNode.ConstraintType.DROP.equals(conNode.getConstraintType())) {
                // No access to verifyType (which would be FOREIGN_KEY) and not otherwise marked GROUPING
                // Assume any name (even no name) is DROP GFK for now
                return conNode;
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
