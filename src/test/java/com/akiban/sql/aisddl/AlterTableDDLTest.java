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
import com.akiban.server.error.JoinColumnMismatchException;
import com.akiban.server.error.JoinToUnknownTableException;
import com.akiban.server.error.JoinToWrongColumnsException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.UnsupportedSQLException;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AlterTableDDLTest {
    private static final String SCHEMA = "test";
    private static final TableName TEMP_NAME_1 = new TableName(SCHEMA, AlterTableDDL.TEMP_TABLE_NAME_1);
    private static final TableName TEMP_NAME_2 = new TableName(SCHEMA, AlterTableDDL.TEMP_TABLE_NAME_2);
    private static final TableName C_NAME = tn(SCHEMA, "c");
    private static final TableName O_NAME = tn(SCHEMA, "o");
    private static final TableName I_NAME = tn(SCHEMA, "i");
    private static final TableName A_NAME = tn(SCHEMA, "a");

    private static final TableCopier NOP_COPIER = new TableCopier() {
        @Override
        public void copyFullTable(AkibanInformationSchema ais, TableName source, TableName destination) {
        }
    };


    private SQLParser parser;
    private DDLFunctionsMock ddlFunctions;
    private NewAISBuilder builder;

    @Before
    public void before() {
        parser = new SQLParser();
        builder = AISBBasedBuilder.create();
        ddlFunctions = null;
    }

    @After
    public void after() {
        parser = null;
        builder = null;
        ddlFunctions = null;
    }


    //
    // ADD
    //

    @Test(expected=NoSuchTableException.class)
    public void cannotAddGFKToUnknownTable() throws StandardException {
        parseAndRun("ALTER TABLE ha1 ADD GROUPING FOREIGN KEY(ha) REFERENCES ha2(ha)");
    }

    @Test(expected=JoinToUnknownTableException.class)
    public void cannotAddGFKToUnknownParent() throws StandardException {
        builder.userTable(C_NAME).colBigInt("cid", false).colBigInt("other").pk("cid");
        parseAndRun("ALTER TABLE c ADD GROUPING FOREIGN KEY(other) REFERENCES zap(id)");
    }

    @Test(expected=UnsupportedSQLException.class)
    public void cannotAddGFKToTableWithParent() throws StandardException {
        builder.userTable(C_NAME).colBigInt("cid", false).pk("cid");
        builder.userTable(O_NAME).colBigInt("oid", false).colBigInt("cid").pk("oid").joinTo(C_NAME).on("cid", "cid");
        parseAndRun("ALTER TABLE o ADD GROUPING FOREIGN KEY(cid) REFERENCES c(cid)");
    }

    @Test(expected=UnsupportedSQLException.class)
    public void cannotAddGFKToTableWithChild() throws StandardException {
        builder.userTable(A_NAME).colBigInt("aid", false).pk("aid");
        builder.userTable(C_NAME).colBigInt("cid", false).colBigInt("aid").pk("cid");
        builder.userTable(O_NAME).colBigInt("oid", false).colBigInt("cid").pk("oid").joinTo(C_NAME).on("cid", "cid");
        parseAndRun("ALTER TABLE c ADD GROUPING FOREIGN KEY(aid) REFERENCES a(aid)");
    }

    @Test(expected=NoSuchColumnException.class)
    public void cannotAddGFKToUnknownParentColumns() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(aid) REFERENCES c(banana)");
    }

    @Test(expected=NoSuchColumnException.class)
    public void cannotAddGFKToUnknownChildColumns() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(banana) REFERENCES c(id)");
    }

    @Test(expected= JoinColumnMismatchException.class)
    public void cannotAddGFKToTooManyChildColumns() throws StandardException {
        builder.userTable(C_NAME).colBigInt("id", false).pk("id");
        builder.userTable(A_NAME).colBigInt("id", false).colBigInt("y").pk("id");
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(id,y) REFERENCES c(id)");
    }

    @Test(expected=JoinColumnMismatchException.class)
    public void cannotAddGFKToTooManyParentColumns() throws StandardException {
        builder.userTable(C_NAME).colBigInt("id", false).colBigInt("x").pk("id");
        builder.userTable(A_NAME).colBigInt("id", false).colBigInt("y").pk("id");
        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(y) REFERENCES c(id,x)");
    }

    @Test
    public void addGFKToSingleTableOnSingleTable() throws StandardException {
        builder.userTable(C_NAME).colBigInt("cid", false).pk("cid");
        builder.userTable(O_NAME).colBigInt("oid", false).colBigInt("cid").pk("oid");

        parseAndRun("ALTER TABLE o ADD GROUPING FOREIGN KEY(cid) REFERENCES c(cid)");

        expectCreated(TEMP_NAME_1);
        expectRenamed(O_NAME, TEMP_NAME_2, TEMP_NAME_1, O_NAME);
        expectDropped(TEMP_NAME_2);
        expectGroupIsSame(C_NAME, O_NAME, true);
        expectChildOf(C_NAME, O_NAME);
    }

    @Test
    public void addGFKToPkLessTable() throws StandardException {
        builder.userTable(C_NAME).colBigInt("cid", false).pk("cid");
        builder.userTable(O_NAME).colBigInt("oid", false).colBigInt("cid");

        parseAndRun("ALTER TABLE o ADD GROUPING FOREIGN KEY(cid) REFERENCES c(cid)");

        expectCreated(TEMP_NAME_1);
        expectRenamed(O_NAME, TEMP_NAME_2, TEMP_NAME_1, O_NAME);
        expectDropped(TEMP_NAME_2);
        expectGroupIsSame(C_NAME, O_NAME, true);
        expectChildOf(C_NAME, O_NAME);
    }

    @Test
    public void addGFKToSingleTableOnRootOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();

        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id) REFERENCES c(id)");

        expectCreated(TEMP_NAME_1);
        expectRenamed(A_NAME, TEMP_NAME_2, TEMP_NAME_1, A_NAME);
        expectDropped(TEMP_NAME_2);
        expectGroupIsSame(C_NAME, A_NAME, true);
        expectGroupIsSame(C_NAME, O_NAME, true);
        expectGroupIsSame(C_NAME, I_NAME, true);
        expectChildOf(C_NAME, A_NAME);
    }

    @Test
    public void addGFKToSingleTableOnMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();

        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id) REFERENCES o(id)");

        expectCreated(TEMP_NAME_1);
        expectRenamed(A_NAME, TEMP_NAME_2, TEMP_NAME_1, A_NAME);
        expectDropped(TEMP_NAME_2);
        expectGroupIsSame(C_NAME, A_NAME, true);
        expectGroupIsSame(C_NAME, O_NAME, true);
        expectGroupIsSame(C_NAME, I_NAME, true);
        expectChildOf(O_NAME, A_NAME);
    }

    @Test
    public void addGFKToSingleTableOnLeafOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();

        parseAndRun("ALTER TABLE a ADD GROUPING FOREIGN KEY(other_id) REFERENCES i(id)");

        expectCreated(TEMP_NAME_1);
        expectRenamed(A_NAME, TEMP_NAME_2, TEMP_NAME_1, A_NAME);
        expectDropped(TEMP_NAME_2);
        expectGroupIsSame(C_NAME, A_NAME, true);
        expectGroupIsSame(C_NAME, O_NAME, true);
        expectGroupIsSame(C_NAME, I_NAME, true);
        expectChildOf(I_NAME, A_NAME);
    }


    //
    // DROP
    //

    @Test(expected=NoSuchTableException.class)
    public void cannotDropGFKFromUnknownTable() throws StandardException {
        parseAndRun("ALTER TABLE c DROP GROUPING FOREIGN KEY");
    }

    @Test(expected=UnsupportedSQLException.class)
    public void cannotDropGFKFromSingleTableGroup() throws StandardException {
        builder.userTable(C_NAME).colBigInt("id", false).pk("id");
        parseAndRun("ALTER TABLE c DROP GROUPING FOREIGN KEY");
    }

    @Test(expected=UnsupportedSQLException.class)
    public void cannotDropGFKFromRootOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE c DROP GROUPING FOREIGN KEY");
    }

    @Test(expected=UnsupportedSQLException.class)
    public void cannotDropGFKFromMiddleOfGroup() throws StandardException {
        buildCOIJoinedAUnJoined();
        parseAndRun("ALTER TABLE o DROP GROUPING FOREIGN KEY");
    }

    @Test
    public void dropGFKLeafFromTwoTableGroup() throws StandardException {
        builder.userTable(C_NAME).colBigInt("id", false).pk("id");
        builder.userTable(A_NAME).colBigInt("id", false).colBigInt("cid").pk("id").joinTo(C_NAME).on("cid", "id");

        parseAndRun("ALTER TABLE a DROP GROUPING FOREIGN KEY");

        expectCreated(TEMP_NAME_1);
        expectRenamed(A_NAME, TEMP_NAME_2, TEMP_NAME_1, A_NAME);
        expectDropped(TEMP_NAME_2);
        expectGroupIsSame(C_NAME, A_NAME, false);
    }

    @Test
    public void dropGFKLeafFromGroup() throws StandardException {
        buildCOIJoinedAUnJoined();

        parseAndRun("ALTER TABLE i DROP GROUPING FOREIGN KEY");

        expectCreated(TEMP_NAME_1);
        expectRenamed(I_NAME, TEMP_NAME_2, TEMP_NAME_1, I_NAME);
        expectDropped(TEMP_NAME_2);
        expectGroupIsSame(C_NAME, I_NAME, false);
    }

    private void parseAndRun(String sqlText) throws StandardException {
        StatementNode node = parser.parseStatement(sqlText);
        assertEquals("Was alter", AlterTableNode.class, node.getClass());
        ddlFunctions = new DDLFunctionsMock(builder.unvalidatedAIS());
        AlterTableDDL.alterTable(ddlFunctions, NOP_COPIER, null, SCHEMA, (AlterTableNode)node);
    }


    private void expectCreated(TableName... names) {
        assertEquals("Creation order",
                     Arrays.asList(names).toString(),
                     ddlFunctions.createdTables.toString());
    }

    private void expectRenamed(TableName... pairs) {
        assertTrue("Even number of names", (pairs.length % 2) == 0);
        List<TableNamePair> pairList = new ArrayList<TableNamePair>();
        for(int i = 0; i < pairs.length; i += 2) {
            pairList.add(new TableNamePair(pairs[i], pairs[i + 1]));
        }
        assertEquals("Renamed order",
                     pairList.toString(),
                     ddlFunctions.renamedTables.toString());
    }

    private void expectDropped(TableName... names) {
        assertEquals("Dropped order",
                     Arrays.asList(names).toString(),
                     ddlFunctions.droppedTables.toString());
    }

    private void expectGroupIsSame(TableName t1, TableName t2, boolean equal) {
        // Only check the name of the group, DDLFunctionsMock doesn't re-serialize
        UserTable table1 = ddlFunctions.ais.getUserTable(t1);
        UserTable table2 = ddlFunctions.ais.getUserTable(t2);
        String groupName1 = ((table1 != null) && (table1.getGroup() != null)) ? table1.getGroup().getName() : "<NO_GROUP>1";
        String groupName2 = ((table2 != null) && (table2.getGroup() != null)) ? table2.getGroup().getName() : "<NO_GROUP>2";
        if(equal) {
            assertEquals("Same group for tables " + t1 + "," + t2, groupName1, groupName2);
        } else if(groupName1.equals(groupName2)) {
            fail("Expected different group for tables " + t1 + "," + t2);
        }
    }

    private void expectChildOf(TableName t1, TableName t2) {
        // Only check the names of tables, DDLFunctionsMock doesn't re-serialize
        UserTable table1 = ddlFunctions.ais.getUserTable(t2);
        UserTable parent = (table1.getParentJoin() != null) ? table1.getParentJoin().getParent() : null;
        TableName parentName = (parent != null) ? parent.getName() : null;
        assertEquals(t2 + " parent name", t1, parentName);
    }


    private void buildCOIJoinedAUnJoined() {
        builder.userTable(C_NAME).colBigInt("id", false).pk("id");
        builder.userTable(O_NAME).colBigInt("id", false).colBigInt("cid").pk("id").joinTo(C_NAME).on("cid", "id");
        builder.userTable(I_NAME).colBigInt("id", false).colBigInt("oid").pk("id").joinTo(O_NAME).on("oid", "id");
        builder.userTable(A_NAME).colBigInt("id", false).colBigInt("other_id").pk("id");
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
        final AkibanInformationSchema ais;
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
            ais.getUserTables().remove(currentName);
            ais.getUserTables().put(newName, currentTable);
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
}
