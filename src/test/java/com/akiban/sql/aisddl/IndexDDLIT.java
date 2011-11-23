/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */
package com.akiban.sql.aisddl;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

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
    public void createKey() throws SQLException {
        String sql = "CREATE INDEX test1 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        connection.createStatement().execute(sql);
        
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNotNull (table.getIndex("test1"));
        assertFalse (table.getIndex("test1").isUnique());
        assertEquals (3, table.getIndex("test1").getColumns().size());
        assertEquals ("c1", table.getIndex("test1").getColumns().get(0).getColumn().getName());
        assertEquals ("c2", table.getIndex("test1").getColumns().get(1).getColumn().getName());
        assertEquals ("c3", table.getIndex("test1").getColumns().get(2).getColumn().getName());
    }
    
    @Test
    public void createUnique() throws SQLException {
        String sql = "CREATE UNIQUE INDEX test2 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        connection.createStatement().execute(sql);
        UserTable table=ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNotNull (table.getIndex("test2"));
        assertTrue   (table.getIndex("test2").isUnique());
        assertEquals (3, table.getIndex("test2").getColumns().size());
        assertEquals ("c1", table.getIndex("test2").getColumns().get(0).getColumn().getName());
        assertEquals ("c2", table.getIndex("test2").getColumns().get(1).getColumn().getName());
        assertEquals ("c3", table.getIndex("test2").getColumns().get(2).getColumn().getName());
        
    }
    
    @Test 
    public void createGroupKey() throws SQLException {
        String sql = "CREATE INDEX test4 on test.t2 (t1.c1, t2.c1) USING LEFT JOIN";
        createJoinedTables();
        
        connection.createStatement().execute(sql);

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
        assertEquals  (2, group.getIndex("test4").getColumns().size());
        assertEquals  ("c1", group.getIndex("test4").getColumns().get(0).getColumn().getName());
        assertEquals  ("c1", group.getIndex("test4").getColumns().get(0).getColumn().getName());
        
    }
    
    @Test 
    public void createGroupRight() throws SQLException {
        String sql = "CREATE INDEX test4 on test.t2 (t1.c1, t2.c1) USING RIGHT JOIN";
        createJoinedTables();
        
        connection.createStatement().execute(sql);

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
        assertEquals  (2, group.getIndex("test4").getColumns().size());
        assertEquals  ("c1", group.getIndex("test4").getColumns().get(0).getColumn().getName());
        assertEquals  ("c1", group.getIndex("test4").getColumns().get(0).getColumn().getName());
        
    }
    
    @Test (expected=PSQLException.class)
    public void createUniqueGroupKey () throws SQLException {
        String sql = "CREATE UNIQUE INDEX test4 on test.t2 (t1.c1, t2.c1) USING LEFT JOIN";
        createJoinedTables();
        
        connection.createStatement().execute(sql);
        
        
    }
       
    @Test
    public void createTableKeyonGroup() throws SQLException {
        String sql = "CREATE INDEX test6 on test.t2 (t2.c1, c2, test.t2.c3)";
        createJoinedTables();
        
        connection.createStatement().execute(sql);
        UserTable table2 = ddlServer().getAIS(session()).getUserTable("test", "t2");
        assertNotNull (table2);
        assertNotNull (table2.getIndex("test6"));
        assertNull    (table2.getGroup().getIndex("test6"));
    }
    
   
    @Test (expected=PSQLException.class)
    public void createIndexErrorTable () throws SQLException {
        String sql = "CREATE INDEX test10 on test.bad (c1, c2)";
        createJoinedTables();
        
        connection.createStatement().execute(sql);
    }
    
    
    @Test (expected=PSQLException.class)
    public void createIndexErrorCol() throws SQLException {
        String sql = "CREATE INDEX test11 on test.t1 (c1, colBad)";
        createJoinedTables();
        
        connection.createStatement().execute(sql);
    }
    
    @Test (expected=PSQLException.class)
    public void createIndexErrorBranching() throws SQLException {
        String sql ="CREATE INDEX test12 on test.t1 (t1.c1, t2.c3, t3.c1) USING LEFT JOIN";
        createBranchedGroup();
        connection.createStatement().execute(sql);
    }

    @Test (expected=PSQLException.class)
    public void createIndexErrorTableCol() throws SQLException {
        String sql = "CREATE INDEX test13 on test.t1 (t1.c1, t.c1) USING LEFT JOIN";
        createJoinedTables();
        connection.createStatement().execute(sql);
    }

    @Test (expected=PSQLException.class)
    public void createIndexTableWithJoin () throws SQLException {
        String sql = "CREATE INDEX test4 on test.t1 (t1.c1, t1.c2) USING LEFT JOIN";
        createJoinedTables();
        
        connection.createStatement().execute(sql);
    }

    @Test (expected=PSQLException.class)
    public void createIndexGroupWithoutJoin () throws SQLException {
        String sql = "CREATE INDEX test4 on test.t2 (t1.c1, t2.c1)";
        createJoinedTables();
        
        connection.createStatement().execute(sql);
    }

    @Test
    public void dropIndexSimple() throws SQLException {
        String sql = "CREATE INDEX test114 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        connection.createStatement().execute(sql);
        String sql1 = "DROP INDEX test114";
        
        connection.createStatement().execute(sql1);
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test114"));
    }
    
    @Test @Ignore // - disabled because the SET SCHEMA doesn't work? 
    public void dropIndexTable() throws SQLException {
        String sql = "CREATE INDEX test115 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        connection.createStatement().execute(sql);
        
        connection.createStatement().execute("SET SCHEMA test; DROP INDEX t1.test115");
        connection.createStatement().execute("DROP INDEX t1.test115");
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test115"));
    }

    @Test
    public void dropIndexSchema() throws SQLException {
        String sql = "CREATE INDEX test116 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        connection.createStatement().execute(sql);
        String sql1 = "DROP INDEX test.t1.test116";
        
        connection.createStatement().execute(sql1);
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test116"));
    }
    
    @Test (expected=PSQLException.class)
    public void dropIndexFailure() throws SQLException {
        String sql = "CREATE INDEX test15 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        connection.createStatement().execute(sql);
        String sql1 = "DROP INDEX bad_index";
        
        connection.createStatement().execute(sql1);
    }
    
    @Test 
    public void dropGroupIndex () throws SQLException { 
        String sql = "CREATE INDEX test16 on test.t2 (t1.c1, t2.c1) USING LEFT JOIN";
        createJoinedTables();
        connection.createStatement().execute(sql);
        
        String sql1 = "DROP INDEX test16";
        connection.createStatement().execute(sql1);
        UserTable table1 = ddlServer().getAIS(session()).getUserTable("test", "t1");
        assertNotNull (table1);
        Group group = table1.getGroup();
        assertNotNull (group);
        assertNull (group.getIndex("test16"));
    }
    
    @Test (expected=PSQLException.class)
    public void dropDuplicateIndexes () throws SQLException {
        createJoinedTables();
        String sql1 = "CREATE INDEX test17 on test.t2 (t2.c1)";
        String sql2 = "CREATE INDEX test17 on test.t1 (t1.c1)";
        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        
        String sql3 = "DROP INDEX test17";
        connection.createStatement().execute(sql3);
    }

    @Test 
    public void dropCorrectDuplicateIndexes () throws SQLException {
        createJoinedTables();
        String sql1 = "CREATE INDEX test18 on test.t2 (t2.c1)";
        String sql2 = "CREATE INDEX test18 on test.t1 (t1.c1)";
        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        
        String sql3 = "DROP INDEX test.t1.test18";
        connection.createStatement().execute(sql3);
        UserTable table1 = ddlServer().getAIS(session()).getUserTable("test", "t2");
        assertNotNull (table1);
        assertNotNull (table1.getIndex("test18"));
    }

    @Test (expected=PSQLException.class)
    public void dropPrimaryKeyFails() throws SQLException {
        createTable(); 
        
        String sql = "DROP INDEX test.t1.\"PRIMARY\"";
        connection.createStatement().execute(sql);
    }
    
    @Test (expected=PSQLException.class)
    public void createCrossGroupIndex() throws SQLException {
        createUngroupedTables(); 
        
        String sql = "CREATE INDEX t1_t2 ON test.t1(c1, test.t2.c1) USING LEFT JOIN";
        connection.createStatement().execute(sql);
    }
    
    private void createTable () throws SQLException {
        String sql = "CREATE TABLE test.t1 (c1 integer not null primary key, " +
            " c2 integer, c3 integer, c4 integer, c5 integer)";
        
        connection.createStatement().execute(sql);
    }
    
    private void createJoinedTables() throws SQLException {
        String sql1 = "CREATE TABLE test.t1 (c1 integer not null primary key," +
                "c2 integer not null, c3 integer)";
        String sql2 = "CREATE TABLE test.t2 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        
        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
    }
    
    private void createBranchedGroup () throws SQLException {
        String sql1 = "CREATE TABLE test.t1 (c1 integer not null primary key," +
                "c2 integer not null, c3 integer)";
        String sql2 = "CREATE TABLE test.t2 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        String sql3 = "CREATE TABLE test.t3 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        connection.createStatement().execute(sql3);
    }

    private void createSchemaJoinedTables() throws SQLException {
        String sql1 = "CREATE TABLE test1.t1 (c1 integer not null primary key," +
                "c2 integer not null, c3 integer)";
        String sql2 = "CREATE TABLE test2.t1 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1, " +
            "c3 integer not null)";
        
        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
    }

    private void createUngroupedTables()  throws SQLException {
        String sql1 = "CREATE TABLE test.t1 (c1 integer not null primary key," +
                "c2 integer not null)";
        String sql2 = "CREATE TABLE test.t2 (c1 integer not null primary key, " +
                "c2 integer not null)";
        
        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
    }
    
    protected DDLFunctions ddlServer() {
        return serviceManager().getDXL().ddlFunctions();
    }
    
}
