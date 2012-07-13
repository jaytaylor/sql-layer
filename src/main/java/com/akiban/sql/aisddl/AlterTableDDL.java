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
import com.akiban.ais.model.AISTableNameChanger;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.JoinColumnMismatchException;
import com.akiban.server.error.JoinToProtectedTableException;
import com.akiban.server.error.JoinToUnknownTableException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.dxl.DXLFunctionsHook;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.ConstraintDefinitionNode;
import com.akiban.sql.parser.FKConstraintDefinitionNode;
import com.akiban.sql.parser.TableElementNode;

import java.util.Collection;
import java.util.Collections;

import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction.ALTER_TABLE_TEMP_TABLE;
import static com.akiban.sql.aisddl.DDLHelper.convertName;
import static com.akiban.util.Exceptions.throwAlways;

public class AlterTableDDL {
    private static final String MULTI_GROUP_ERROR_MSG = "Cannot add table %s to multiple groups";
    private static final String NON_LEAF_ERROR_MSG = "Cannot drop group from %s table %s";
    static final String TEMP_TABLE_NAME_NEW = "__ak_alter_temp_new";
    static final String TEMP_TABLE_NAME_OLD = "__ak_alter_temp_old";

    private AlterTableDDL() {}

    public static void alterTable(DXLFunctionsHook hook,
                                  DDLFunctions ddlFunctions,
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

        String[] columns = fk.getColumnList().getColumnNames();
        String[] refColumns = fk.getRefResultColumnList().getColumnNames();
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
        newTable.clearGrouping();

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
}
