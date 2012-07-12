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

import com.akiban.ais.model.AISTableNameChanger;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.dxl.IndexCheckSummary;
import com.akiban.server.service.session.Session;
import com.akiban.sql.StandardException;
import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AlterTableDDLTest {
    private static final String SCHEMA = "test";
    private static final TableName TEMP_NAME_1 = new TableName(SCHEMA, AlterTableDDL.TEMP_TABLE_NAME_1);
    private static final TableName TEMP_NAME_2 = new TableName(SCHEMA, AlterTableDDL.TEMP_TABLE_NAME_2);

    private SQLParser parser;
    private DDLFunctionsMock ddlFunctions;
    private NewAISBuilder builder;

    @Before
    public void before() {
        parser = new SQLParser();
        builder = AISBBasedBuilder.create();
        ddlFunctions = new DDLFunctionsMock(builder.unvalidatedAIS());
    }

    @After
    public void after() {
        parser = null;
        builder = null;
        ddlFunctions = null;
    }

    @Test
    public void simpleAddSingleToSingle() throws StandardException {
        builder.userTable(SCHEMA, "c")
                .colBigInt("cid", false)
                .pk("cid");
        builder.userTable(SCHEMA, "o")
                .colBigInt("oid", false)
                .colBigInt("cid")
                .pk("oid");
        builder.unvalidatedAIS();

        parseAndRun("ALTER TABLE c ADD GROUPING FOREIGN KEY(cid) REFERENCES c(cid)");

        assertEquals("Create order", list(TEMP_NAME_1), ddlFunctions.createdTables);

        TableNamePair rename1 = new TableNamePair(tn(SCHEMA, "o"), TEMP_NAME_1);
        TableNamePair rename2 = new TableNamePair(TEMP_NAME_2, tn(SCHEMA, "o"));
        assertEquals("Rename order", list(rename1, rename2), ddlFunctions.renamedTables);

        assertEquals("Drop order", list(TEMP_NAME_2), ddlFunctions.droppedTables);
    }

    private void parseAndRun(String sqlText) throws StandardException {
        StatementNode node = parser.parseStatement(sqlText);
        assertEquals("Was alter", AlterTableNode.class, node.getClass());
        AlterTableDDL.alterTable(ddlFunctions, null, SCHEMA, (AlterTableNode)node);
    }

    private static class TableNamePair {
        private final TableName tn1;
        private final TableName tn2;

        public TableNamePair(TableName tn1, TableName tn2) {
            assertNotNull("tn1", tn1);
            assertNotNull("tn2", tn2);
            this.tn1 = tn1;
            this.tn2 = tn2;
        }

        @Override
        public String toString() {
            return "{" + tn1 + "," + tn2 + "}";
        }
    }

    private static class DDLFunctionsMock implements DDLFunctions {
        private final AkibanInformationSchema ais;
        final List<TableName> createdTables = new ArrayList<TableName>();
        final List<TableName> droppedTables = new ArrayList<TableName>();
        final List<TableNamePair> renamedTables = new ArrayList<TableNamePair>();

        public DDLFunctionsMock(AkibanInformationSchema ais) {
            this.ais = ais;
        }

        @Override
        public void createTable(Session session, UserTable table) {
            if(ais.getUserTable(table.getName()) != null) {
                throw new DuplicateTableNameException(table.getName());
            }
            createdTables.add(table.getName());
            ais.addUserTable(table);
        }

        @Override
        public void renameTable(Session session, TableName currentName, TableName newName) {
            if(ais.getUserTable(newName) != null) {
                throw new DuplicateTableNameException(newName);
            }
            UserTable currentTable = ais.getUserTable(currentName);
            if(currentTable == null) {
                throw new NoSuchTableException(currentName);
            }
            renamedTables.add(new TableNamePair(currentName, newName));
            AISTableNameChanger changer = new AISTableNameChanger(currentTable, newName.getSchemaName(), newName.getTableName());
            changer.doChange();
        }

        @Override
        public void dropTable(Session session, TableName tableName) {
            if(ais.getUserTable(tableName) == null) {
                throw new NoSuchTableException(tableName);
            }
            droppedTables.add(tableName);
            ais.getUserTables().remove(tableName);
        }

        @Override
        public AkibanInformationSchema getAIS(Session session) {
            return ais;
        }

        @Override
        public void createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropGroup(Session session, String groupName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropGroupIndexes(Session session, String groupName, Collection<String> indexesToDrop) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createView(Session session, View newView) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropView(Session session, TableName viewName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropSchema(Session session, String schemaName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropTableIndexes(Session session, TableName tableName, Collection<String> indexesToDrop) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getDDLs(Session session) throws InvalidOperationException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getGeneration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowDef getRowDef(int tableId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table getTable(Session session, int tableId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table getTable(Session session, TableName tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTableId(Session session, TableName tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableName getTableName(Session session, int tableId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public UserTable getUserTable(Session session, TableName tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexCheckSummary checkAndFixIndexes(Session session, String schemaRegex, String tableRegex) {
            throw new UnsupportedOperationException();
        }
    }

    private static TableName tn(String schema, String table) {
        return new TableName(schema, table);
    }

    private static <T> List<T> list(T... names) {
        return Arrays.asList(names);
    }

}
