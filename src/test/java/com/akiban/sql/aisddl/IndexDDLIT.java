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

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index.JoinType;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.DDLFunctions;
import com.akiban.sql.pg.PostgresServerITBase;

public class IndexDDLIT extends PostgresServerITBase {

    @Test
    public void createKey() throws Exception {
        String sql = "CREATE INDEX test1 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        getConnection().createStatement().execute(sql);
        
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNotNull (table.getIndex("test1"));
        assertFalse (table.getIndex("test1").isUnique());
        assertEquals (3, table.getIndex("test1").getKeyColumns().size());
        assertEquals ("c1", table.getIndex("test1").getKeyColumns().get(0).getColumn().getName());
        assertEquals ("c2", table.getIndex("test1").getKeyColumns().get(1).getColumn().getName());
        assertEquals ("c3", table.getIndex("test1").getKeyColumns().get(2).getColumn().getName());
    }
    
    @Test
    public void createUnique() throws Exception {
        String sql = "CREATE UNIQUE INDEX test2 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        getConnection().createStatement().execute(sql);
        UserTable table=ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNotNull (table.getIndex("test2"));
        assertTrue   (table.getIndex("test2").isUnique());
        assertEquals (3, table.getIndex("test2").getKeyColumns().size());
        assertEquals ("c1", table.getIndex("test2").getKeyColumns().get(0).getColumn().getName());
        assertEquals ("c2", table.getIndex("test2").getKeyColumns().get(1).getColumn().getName());
        assertEquals ("c3", table.getIndex("test2").getKeyColumns().get(2).getColumn().getName());
        
    }
    
    @Test 
    public void createGroupKey() throws Exception {
        String sql = "CREATE INDEX test4 on test.t2 (t1.c1, t2.c1) USING LEFT JOIN";
        createJoinedTables();
        
        getConnection().createStatement().execute(sql);

        UserTable table1 = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table1);

        UserTable table2 = ddlServer().getAIS(session()).getUserTable("test", "t2");
        assertNotNull (table2);
        
        assertNull (table1.getIndex("test4"));
        assertNull (table2.getIndex("test4"));
        
        assertEquals (table1.getGroup().getName(), table2.getGroup().getName());
        Group group = table1.getGroup();

        assertNotNull (group.getIndex("test4"));
        assertFalse   (group.getIndex("test4").isUnique());
        assertEquals  (JoinType.LEFT, group.getIndex("test4").getJoinType());
        assertEquals  (2, group.getIndex("test4").getKeyColumns().size());
        assertEquals  ("c1", group.getIndex("test4").getKeyColumns().get(0).getColumn().getName());
        assertEquals  ("c1", group.getIndex("test4").getKeyColumns().get(0).getColumn().getName());
        
    }
    
    @Test 
    public void createGroupRight() throws Exception {
        String sql = "CREATE INDEX test4 on test.t2 (t1.c1, t2.c1) USING RIGHT JOIN";
        createJoinedTables();
        
        getConnection().createStatement().execute(sql);

        UserTable table1 = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table1);

        UserTable table2 = ddlServer().getAIS(session()).getUserTable("test", "t2");
        assertNotNull (table2);
        
        assertNull (table1.getIndex("test4"));
        assertNull (table2.getIndex("test4"));
        
        assertEquals (table1.getGroup().getName(), table2.getGroup().getName());
        Group group = table1.getGroup();

        assertNotNull (group.getIndex("test4"));
        assertEquals  (JoinType.RIGHT, group.getIndex("test4").getJoinType());
        assertFalse   (group.getIndex("test4").isUnique());
        assertEquals  (2, group.getIndex("test4").getKeyColumns().size());
        assertEquals  ("c1", group.getIndex("test4").getKeyColumns().get(0).getColumn().getName());
        assertEquals  ("c1", group.getIndex("test4").getKeyColumns().get(0).getColumn().getName());
        
    }
    
    @Test (expected=PSQLException.class)
    public void createUniqueGroupKey () throws Exception {
        String sql = "CREATE UNIQUE INDEX test4 on test.t2 (t1.c1, t2.c1) USING LEFT JOIN";
        createJoinedTables();
        
        getConnection().createStatement().execute(sql);
        
        
    }
       
    @Test
    public void createTableKeyonGroup() throws Exception {
        String sql = "CREATE INDEX test6 on test.t2 (t2.c1, c2, test.t2.c3)";
        createJoinedTables();
        
        getConnection().createStatement().execute(sql);
        UserTable table2 = ddlServer().getAIS(session()).getUserTable("test", "t2");
        assertNotNull (table2);
        assertNotNull (table2.getIndex("test6"));
        assertNull    (table2.getGroup().getIndex("test6"));
    }
    
   
    @Test (expected=PSQLException.class)
    public void createIndexErrorTable () throws Exception {
        String sql = "CREATE INDEX test10 on test.bad (c1, c2)";
        createJoinedTables();
        
        getConnection().createStatement().execute(sql);
    }
    
    
    @Test (expected=PSQLException.class)
    public void createIndexErrorCol() throws Exception {
        String sql = "CREATE INDEX test11 on test.t1 (c1, colBad)";
        createJoinedTables();
        
        getConnection().createStatement().execute(sql);
    }
    
    @Test (expected=PSQLException.class)
    public void createIndexErrorBranching() throws Exception {
        String sql ="CREATE INDEX test12 on test.t1 (t1.c1, t2.c3, t3.c1) USING LEFT JOIN";
        createBranchedGroup();
        getConnection().createStatement().execute(sql);
    }

    @Test (expected=PSQLException.class)
    public void createIndexErrorTableCol() throws Exception {
        String sql = "CREATE INDEX test13 on test.t1 (t1.c1, t.c1) USING LEFT JOIN";
        createJoinedTables();
        getConnection().createStatement().execute(sql);
    }

    @Test (expected=PSQLException.class)
    public void createIndexTableWithJoin () throws Exception {
        String sql = "CREATE INDEX test4 on test.t1 (t1.c1, t1.c2) USING LEFT JOIN";
        createJoinedTables();
        
        getConnection().createStatement().execute(sql);
    }

    @Test (expected=PSQLException.class)
    public void createIndexGroupWithoutJoin () throws Exception {
        String sql = "CREATE INDEX test4 on test.t2 (t1.c1, t2.c1)";
        createJoinedTables();
        
        getConnection().createStatement().execute(sql);
    }

    @Test
    public void dropIndexSimple() throws Exception {
        String sql = "CREATE INDEX test114 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        getConnection().createStatement().execute(sql);
        String sql1 = "DROP INDEX test114";
        
        getConnection().createStatement().execute(sql1);
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test114"));
    }
     
    
    @Test(expected=PSQLException.class)
    public void dropNonExistingIndexError() throws Exception 
    {
        String sql = "CREATE INDEX test114 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        getConnection().createStatement().execute(sql);
        String sql1 = "DROP INDEX test114b";
        
        getConnection().createStatement().execute(sql1);
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test114b"));
    }
    
    @Test
    public void dropNonExistingIndex() throws Exception 
    {
        String sql = "CREATE INDEX test114 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        getConnection().createStatement().execute(sql);
        String sql1 = "DROP INDEX IF EXISTS test114b";
        
        getConnection().createStatement().execute(sql1);
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test114b"));
    }
    
    
    @Test @Ignore // - disabled because the SET SCHEMA doesn't work? 
    public void dropIndexTable() throws Exception {
        String sql = "CREATE INDEX test115 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        getConnection().createStatement().execute(sql);
        
        getConnection().createStatement().execute("SET SCHEMA test; DROP INDEX t1.test115");
        getConnection().createStatement().execute("DROP INDEX t1.test115");
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test115"));
    }

    @Test
    public void dropIndexSchema() throws Exception {
        String sql = "CREATE INDEX test116 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        getConnection().createStatement().execute(sql);
        String sql1 = "DROP INDEX test.t1.test116";
        
        getConnection().createStatement().execute(sql1);
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test116"));
    }
    
    @Test (expected=PSQLException.class)
    public void dropIndexFailure() throws Exception {
        String sql = "CREATE INDEX test15 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        getConnection().createStatement().execute(sql);
        String sql1 = "DROP INDEX bad_index";
        
        getConnection().createStatement().execute(sql1);
    }
    
    @Test 
    public void dropGroupIndex () throws Exception { 
        String sql = "CREATE INDEX test16 on test.t2 (t1.c1, t2.c1) USING LEFT JOIN";
        createJoinedTables();
        getConnection().createStatement().execute(sql);
        
        String sql1 = "DROP INDEX test16";
        getConnection().createStatement().execute(sql1);
        UserTable table1 = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table1);
        Group group = table1.getGroup();
        assertNotNull (group);
        assertNull (group.getIndex("test16"));
    }
    
    @Test (expected=PSQLException.class)
    public void dropDuplicateIndexes () throws Exception {
        createJoinedTables();
        String sql1 = "CREATE INDEX test17 on test.t2 (t2.c1)";
        String sql2 = "CREATE INDEX test17 on test.t1 (t1.c1)";
        getConnection().createStatement().execute(sql1);
        getConnection().createStatement().execute(sql2);
        
        String sql3 = "DROP INDEX test17";
        getConnection().createStatement().execute(sql3);
    }

    @Test 
    public void dropCorrectDuplicateIndexes () throws Exception {
        createJoinedTables();
        String sql1 = "CREATE INDEX test18 on test.t2 (t2.c1)";
        String sql2 = "CREATE INDEX test18 on test.t1 (t1.c1)";
        getConnection().createStatement().execute(sql1);
        getConnection().createStatement().execute(sql2);
        
        String sql3 = "DROP INDEX test.t1.test18";
        getConnection().createStatement().execute(sql3);
        UserTable table1 = ddlServer().getAIS(session()).getUserTable("test", "t2");
        assertNotNull (table1);
        assertNotNull (table1.getIndex("test18"));
    }

    @Test (expected=PSQLException.class)
    public void dropPrimaryKeyFails() throws Exception {
        createTable(); 
        
        String sql = "DROP INDEX test.t1.\"PRIMARY\"";
        getConnection().createStatement().execute(sql);
    }
    
    @Test (expected=PSQLException.class)
    public void createCrossGroupIndex() throws Exception {
        createUngroupedTables(); 
        
        String sql = "CREATE INDEX t1_t2 ON test.t1(c1, test.t2.c1) USING LEFT JOIN";
        getConnection().createStatement().execute(sql);
    }
    
    private void createTable () throws Exception {
        String sql = "CREATE TABLE test.t1 (c1 integer not null primary key, " +
            " c2 integer, c3 integer, c4 integer, c5 integer)";
        
        getConnection().createStatement().execute(sql);
    }
    
    private void createJoinedTables() throws Exception {
        String sql1 = "CREATE TABLE test.t1 (c1 integer not null primary key," +
                "c2 integer not null, c3 integer)";
        String sql2 = "CREATE TABLE test.t2 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        
        getConnection().createStatement().execute(sql1);
        getConnection().createStatement().execute(sql2);
    }
    
    private void createBranchedGroup () throws Exception {
        String sql1 = "CREATE TABLE test.t1 (c1 integer not null primary key," +
                "c2 integer not null, c3 integer)";
        String sql2 = "CREATE TABLE test.t2 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        String sql3 = "CREATE TABLE test.t3 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        getConnection().createStatement().execute(sql1);
        getConnection().createStatement().execute(sql2);
        getConnection().createStatement().execute(sql3);
    }

    private void createSchemaJoinedTables() throws Exception {
        String sql1 = "CREATE TABLE test1.t1 (c1 integer not null primary key," +
                "c2 integer not null, c3 integer)";
        String sql2 = "CREATE TABLE test2.t1 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        
        getConnection().createStatement().execute(sql1);
        getConnection().createStatement().execute(sql2);
    }

    private void createUngroupedTables()  throws Exception {
        String sql1 = "CREATE TABLE test.t1 (c1 integer not null primary key," +
                "c2 integer not null)";
        String sql2 = "CREATE TABLE test.t2 (c1 integer not null primary key, " +
                "c2 integer not null)";
        
        getConnection().createStatement().execute(sql1);
        getConnection().createStatement().execute(sql2);
    }
    
    protected DDLFunctions ddlServer() {
        return serviceManager().getDXL().ddlFunctions();
    }
    
}
