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
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.FKConstraintDefinitionNode;
import com.akiban.sql.parser.ResultColumn;
import com.akiban.sql.parser.TableElementNode;

import java.util.Collection;
import java.util.Collections;

public class AlterTableDDL {
    static final String TEMP_TABLE_NAME_1 = "__ak_temp_1";
    static final String TEMP_TABLE_NAME_2 = "__ak_temp_2";

    private AlterTableDDL() {}
    
    public static void alterTable(DDLFunctions ddlFunctions,
                                  Session session,
                                  String defaultSchemaName,
                                  AlterTableNode alterTable) {
        final AkibanInformationSchema curAIS = ddlFunctions.getAIS(session);
        final TableName tableName = convertName(defaultSchemaName, alterTable.getObjectName());
        final UserTable table = curAIS.getUserTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName.getSchemaName(), 
                                           tableName.getTableName());
        }

        if (alterTable.isUpdateStatistics()) {
            Collection<String> indexes = null;
            if(!alterTable.isUpdateStatisticsAll()) {
                indexes = Collections.singletonList(alterTable.getIndexNameForUpdateStatistics());
            }
            ddlFunctions.updateTableStatistics(session, tableName, indexes);
            return;
        }

        FKConstraintDefinitionNode fkNode = getOnlyGroupingFKNode(alterTable);
        if(fkNode != null) {
            if(table.getParentJoin() != null) {
                throw new UnsupportedSQLException("Table cannot exist in multiple groups", null);
            }
            if(!table.getChildJoins().isEmpty()) {
                throw new UnsupportedSQLException("Cannot add non-leaf table to a group", null);
            }

            TableName refName = convertName(defaultSchemaName, fkNode.getRefTableName());
            final UserTable refTable = curAIS.getUserTable(refName);
            if(refTable == null) {
                throw new NoSuchTableException(refName);
            }

            AkibanInformationSchema aisCopy = AISCloner.clone(
                    curAIS,
                    new ProtobufWriter.TableAllIndexSelector() {
                        @Override
                        public boolean isSelected(Columnar columnar) {
                            return (columnar == table) || (columnar == refTable);
                        }
                    }
            );

            TableName tempName1 = new TableName(tableName.getSchemaName(), TEMP_TABLE_NAME_1);
            TableName tempName2 = new TableName(tableName.getSchemaName(), TEMP_TABLE_NAME_2);

            UserTable newTable = aisCopy.getUserTable(tableName);
            UserTable newRefTable = aisCopy.getUserTable(refName);
            new AISTableNameChanger(newTable, tempName1).doChange();

            Join join = Join.create(aisCopy, "temp_name", newRefTable, newTable);
            join.setGroup(newRefTable.getGroup());
            newTable.setGroup(join.getGroup());

            String[] columns = fkNode.getColumnList().getColumnNames();
            String[] refColumns = fkNode.getRefResultColumnList().getColumnNames();
            for(int i = 0; (i < refColumns.length) && (i < columns.length); ++i) {
                JoinColumn.create(join, newRefTable.getColumn(refColumns[i]), newTable.getColumn(columns[i]));
            }

            ddlFunctions.createTable(session, newTable);
            // TODO: Copy data
            ddlFunctions.renameTable(session, tableName, tempName2);
            ddlFunctions.renameTable(session, tempName1, tableName);
            ddlFunctions.dropTable(session, tempName2);
            return;
        }

        throw new UnsupportedSQLException (alterTable.statementToString(), alterTable);
    }

    private static FKConstraintDefinitionNode getOnlyGroupingFKNode(AlterTableNode node) {
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

    private static TableName convertName(String defaultSchema, com.akiban.sql.parser.TableName parserName) {
        final String schema = parserName.hasSchema() ? parserName.getSchemaName() : defaultSchema;
        return new TableName(schema, parserName.getTableName());
    }
}
