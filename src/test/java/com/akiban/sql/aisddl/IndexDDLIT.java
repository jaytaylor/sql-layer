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

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index.JoinType;
import com.akiban.ais.model.UserTable;

public class IndexDDLIT extends AISDDLITBase {

    @Test
    public void createKey() throws Exception {
        String sql = "CREATE INDEX test1 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        
        UserTable table = ais().getUserTable("test", "t1");
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
        
        executeDDL(sql);
        UserTable table=ais().getUserTable("test", "t1");
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
        
        executeDDL(sql);

        UserTable table1 = ais().getUserTable("test", "t1");
        assertNotNull (table1);

        UserTable table2 = ais().getUserTable("test", "t2");
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
        
        executeDDL(sql);

        UserTable table1 = ais().getUserTable("test", "t1");
        assertNotNull (table1);

        UserTable table2 = ais().getUserTable("test", "t2");
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
    
    @Test (expected=NullPointerException.class)
    public void createUniqueGroupKey () throws Exception {
        String sql = "CREATE UNIQUE INDEX test4 on test.t2 (t1.c1, t2.c1) USING LEFT JOIN";
        createJoinedTables();
        
        executeDDL(sql);
        
        
    }
       
    @Test
    public void createTableKeyonGroup() throws Exception {
        String sql = "CREATE INDEX test6 on test.t2 (t2.c1, c2, test.t2.c3)";
        createJoinedTables();
        
        executeDDL(sql);
        UserTable table2 = ais().getUserTable("test", "t2");
        assertNotNull (table2);
        assertNotNull (table2.getIndex("test6"));
        assertNull    (table2.getGroup().getIndex("test6"));
    }
    
   
    @Test (expected=NullPointerException.class)
    public void createIndexErrorTable () throws Exception {
        String sql = "CREATE INDEX test10 on test.bad (c1, c2)";
        createJoinedTables();
        
        executeDDL(sql);
    }
    
    
    @Test (expected=NullPointerException.class)
    public void createIndexErrorCol() throws Exception {
        String sql = "CREATE INDEX test11 on test.t1 (c1, colBad)";
        createJoinedTables();
        
        executeDDL(sql);
    }
    
    @Test (expected=NullPointerException.class)
    public void createIndexErrorBranching() throws Exception {
        String sql ="CREATE INDEX test12 on test.t1 (t1.c1, t2.c3, t3.c1) USING LEFT JOIN";
        createBranchedGroup();
        executeDDL(sql);
    }

    @Test (expected=NullPointerException.class)
    public void createIndexErrorTableCol() throws Exception {
        String sql = "CREATE INDEX test13 on test.t1 (t1.c1, t.c1) USING LEFT JOIN";
        createJoinedTables();
        executeDDL(sql);
    }

    @Test (expected=NullPointerException.class)
    public void createIndexTableWithJoin () throws Exception {
        String sql = "CREATE INDEX test4 on test.t1 (t1.c1, t1.c2) USING LEFT JOIN";
        createJoinedTables();
        
        executeDDL(sql);
    }

    @Test (expected=NullPointerException.class)
    public void createIndexGroupWithoutJoin () throws Exception {
        String sql = "CREATE INDEX test4 on test.t2 (t1.c1, t2.c1)";
        createJoinedTables();
        
        executeDDL(sql);
    }

    @Test
    public void dropIndexSimple() throws Exception {
        String sql = "CREATE INDEX test114 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        String sql1 = "DROP INDEX test114";
        
        executeDDL(sql1);
        UserTable table = ais().getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test114"));
    }
     
    
    @Test(expected=NullPointerException.class)
    public void dropNonExistingIndexError() throws Exception 
    {
        String sql = "CREATE INDEX test114 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        String sql1 = "DROP INDEX test114b";
        
        executeDDL(sql1);
        UserTable table = ais().getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test114b"));
    }
    
    @Test
    public void dropNonExistingIndex() throws Exception 
    {
        String sql = "CREATE INDEX test114 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        String sql1 = "DROP INDEX IF EXISTS test114b";
        
        executeDDL(sql1);
        UserTable table = ais().getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test114b"));
    }
    
    
    @Test @Ignore // - disabled because the SET SCHEMA doesn't work? 
    public void dropIndexTable() throws Exception {
        String sql = "CREATE INDEX test115 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        
        executeDDL("SET SCHEMA test; DROP INDEX t1.test115");
        executeDDL("DROP INDEX t1.test115");
        UserTable table = ais().getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test115"));
    }

    @Test
    public void dropIndexSchema() throws Exception {
        String sql = "CREATE INDEX test116 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        String sql1 = "DROP INDEX test.t1.test116";
        
        executeDDL(sql1);
        UserTable table = ais().getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test116"));
    }
    
    @Test (expected=NullPointerException.class)
    public void dropIndexFailure() throws Exception {
        String sql = "CREATE INDEX test15 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        String sql1 = "DROP INDEX bad_index";
        
        executeDDL(sql1);
    }
    
    @Test 
    public void dropGroupIndex () throws Exception { 
        String sql = "CREATE INDEX test16 on test.t2 (t1.c1, t2.c1) USING LEFT JOIN";
        createJoinedTables();
        executeDDL(sql);
        
        String sql1 = "DROP INDEX test16";
        executeDDL(sql1);
        UserTable table1 = ais().getUserTable("test", "t1");
        assertNotNull (table1);
        Group group = table1.getGroup();
        assertNotNull (group);
        assertNull (group.getIndex("test16"));
    }
    
    @Test (expected=NullPointerException.class)
    public void dropDuplicateIndexes () throws Exception {
        createJoinedTables();
        String sql1 = "CREATE INDEX test17 on test.t2 (t2.c1)";
        String sql2 = "CREATE INDEX test17 on test.t1 (t1.c1)";
        executeDDL(sql1);
        executeDDL(sql2);
        
        String sql3 = "DROP INDEX test17";
        executeDDL(sql3);
    }

    @Test 
    public void dropCorrectDuplicateIndexes () throws Exception {
        createJoinedTables();
        String sql1 = "CREATE INDEX test18 on test.t2 (t2.c1)";
        String sql2 = "CREATE INDEX test18 on test.t1 (t1.c1)";
        executeDDL(sql1);
        executeDDL(sql2);
        
        String sql3 = "DROP INDEX test.t1.test18";
        executeDDL(sql3);
        UserTable table1 = ais().getUserTable("test", "t2");
        assertNotNull (table1);
        assertNotNull (table1.getIndex("test18"));
    }

    @Test (expected=NullPointerException.class)
    public void dropPrimaryKeyFails() throws Exception {
        createTable(); 
        
        String sql = "DROP INDEX test.t1.\"PRIMARY\"";
        executeDDL(sql);
    }
    
    @Test (expected=NullPointerException.class)
    public void createCrossGroupIndex() throws Exception {
        createUngroupedTables(); 
        
        String sql = "CREATE INDEX t1_t2 ON test.t1(c1, test.t2.c1) USING LEFT JOIN";
        executeDDL(sql);
    }
    
    private void createTable () throws Exception {
        String sql = "CREATE TABLE test.t1 (c1 integer not null primary key, " +
            " c2 integer, c3 integer, c4 integer, c5 integer)";
        
        executeDDL(sql);
    }
    
    private void createJoinedTables() throws Exception {
        String sql1 = "CREATE TABLE test.t1 (c1 integer not null primary key," +
                "c2 integer not null, c3 integer)";
        String sql2 = "CREATE TABLE test.t2 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        
        executeDDL(sql1);
        executeDDL(sql2);
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
        executeDDL(sql1);
        executeDDL(sql2);
        executeDDL(sql3);
    }

    private void createSchemaJoinedTables() throws Exception {
        String sql1 = "CREATE TABLE test1.t1 (c1 integer not null primary key," +
                "c2 integer not null, c3 integer)";
        String sql2 = "CREATE TABLE test2.t1 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        
        executeDDL(sql1);
        executeDDL(sql2);
    }

    private void createUngroupedTables()  throws Exception {
        String sql1 = "CREATE TABLE test.t1 (c1 integer not null primary key," +
                "c2 integer not null)";
        String sql2 = "CREATE TABLE test.t2 (c1 integer not null primary key, " +
                "c2 integer not null)";
        
        executeDDL(sql1);
        executeDDL(sql2);
    }
    
}
