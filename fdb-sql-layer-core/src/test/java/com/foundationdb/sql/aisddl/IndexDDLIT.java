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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.foundationdb.server.error.UnsupportedFunctionInIndexException;
import org.junit.Test;

import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index.JoinType;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.error.BranchingGroupIndexException;
import com.foundationdb.server.error.IndexTableNotInGroupException;
import com.foundationdb.server.error.IndistinguishableIndexException;
import com.foundationdb.server.error.MissingGroupIndexJoinTypeException;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchIndexException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.ProtectedIndexException;
import com.foundationdb.server.error.TableIndexJoinTypeException;
import com.foundationdb.server.error.UnsupportedUniqueGroupIndexException;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.text.FullTextIndexService;
import com.foundationdb.server.service.text.FullTextIndexServiceImpl;

import java.util.Map;

public class IndexDDLIT extends AISDDLITBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(TableDDLIT.class);
    }

    @Test
    public void createKey() throws Exception {
        String sql = "CREATE INDEX test1 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        
        Table table = ais().getTable("test", "t1");
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
        Table table=ais().getTable("test", "t1");
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

        Table table1 = ais().getTable("test", "t1");
        assertNotNull (table1);

        Table table2 = ais().getTable("test", "t2");
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

        Table table1 = ais().getTable("test", "t1");
        assertNotNull (table1);

        Table table2 = ais().getTable("test", "t2");
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
    
    @Test (expected=UnsupportedUniqueGroupIndexException.class)
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
        Table table2 = ais().getTable("test", "t2");
        assertNotNull (table2);
        assertNotNull (table2.getIndex("test6"));
        assertNull    (table2.getGroup().getIndex("test6"));
    }
    
   
    @Test (expected=NoSuchTableException.class)
    public void createIndexErrorTable () throws Exception {
        String sql = "CREATE INDEX test10 on test.bad (c1, c2)";
        createJoinedTables();
        
        executeDDL(sql);
    }
    
    
    @Test (expected=NoSuchColumnException.class)
    public void createIndexErrorCol() throws Exception {
        String sql = "CREATE INDEX test11 on test.t1 (c1, colBad)";
        createJoinedTables();
        
        executeDDL(sql);
    }
    
    @Test (expected=BranchingGroupIndexException.class)
    public void createIndexErrorBranching() throws Exception {
        String sql ="CREATE INDEX test12 on test.t1 (t1.c1, t2.c3, t3.c1) USING LEFT JOIN";
        createBranchedGroup();
        executeDDL(sql);
    }

    @Test (expected=NoSuchTableException.class)
    public void createIndexErrorTableCol() throws Exception {
        String sql = "CREATE INDEX test13 on test.t1 (t1.c1, t.c1) USING LEFT JOIN";
        createJoinedTables();
        executeDDL(sql);
    }

    @Test (expected=TableIndexJoinTypeException.class)
    public void createIndexTableWithJoin () throws Exception {
        String sql = "CREATE INDEX test4 on test.t1 (t1.c1, t1.c2) USING LEFT JOIN";
        createJoinedTables();
        
        executeDDL(sql);
    }

    @Test (expected=MissingGroupIndexJoinTypeException.class)
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
        Table table = ais().getTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test114"));
    }
     
    
    @Test(expected=NoSuchIndexException.class)
    public void dropNonExistingIndexError() throws Exception 
    {
        String sql = "CREATE INDEX test114 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        String sql1 = "DROP INDEX test114b";
        
        executeDDL(sql1);
        Table table = ais().getTable("test", "t1");
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
        Table table = ais().getTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test114b"));
    }
    
    
    @Test
    public void dropIndexTable() throws Exception {
        String sql = "CREATE INDEX test115 on test.t1 (test.t1.c1, t1.c2, c3)";
        createTable();
        
        executeDDL(sql);
        
        executeDDL("DROP INDEX t1.test115");
        Table table = ais().getTable("test", "t1");
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
        Table table = ais().getTable("test", "t1");
        assertNotNull (table);
        assertNull (table.getIndex("test116"));
    }
    
    @Test (expected=NoSuchIndexException.class)
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
        Table table1 = ais().getTable("test", "t1");
        assertNotNull (table1);
        Group group = table1.getGroup();
        assertNotNull (group);
        assertNull (group.getIndex("test16"));
    }
    
    @Test (expected=IndistinguishableIndexException.class)
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
        Table table1 = ais().getTable("test", "t2");
        assertNotNull (table1);
        assertNotNull (table1.getIndex("test18"));
    }

    @Test (expected=ProtectedIndexException.class)
    public void dropPrimaryKeyFails() throws Exception {
        createTable(); 
        
        String sql = "DROP INDEX test.t1.\"PRIMARY\"";
        executeDDL(sql);
    }
    
    @Test (expected=IndexTableNotInGroupException.class)
    public void createCrossGroupIndex() throws Exception {
        createUngroupedTables(); 
        
        String sql = "CREATE INDEX t1_t2 ON test.t1(c1, test.t2.c1) USING LEFT JOIN";
        executeDDL(sql);
    }
    
    @Test
    public void createSpatialIndex() throws Exception {
        String sql = "CREATE TABLE test.t16 (c1 decimal(11,7), c2 decimal(11,7))";
        executeDDL(sql);
        sql = "CREATE INDEX t16_space on test.t16(geo_lat_lon(c1, c2))";
        executeDDL(sql);
        TableIndex index = ais().getTable("test", "t16").getIndex("t16_space");
        assertNotNull(index);
        assertTrue(index.isSpatial());
        
    }

    @Test (expected=UnsupportedFunctionInIndexException.class)
    public void createIndexWithUnsupportedFunction() throws Exception {
        executeDDL("create table t(x decimal(10, 5), y decimal(10, 5))");
        executeDDL("create index t_z_order_lat_lon_is_oldspeak on t(z_order_lat_lon(x, y))");
    }
    
    @Test
    public void createFullTextIndex() throws Exception {
        String sql = "CREATE TABLE test.t17 (c1 varchar(1000))";
        executeDDL(sql);
        sql = "CREATE INDEX t17_ft on test.t17 (FULL_TEXT(c1))";
        executeDDL(sql);
        FullTextIndex index = ais().getTable("test", "t17").getFullTextIndex("t17_ft");
        assertNotNull(index);
        assertNull (ais().getTable("test","t17").getIndex("t17_ft"));
    }

    @Test
    public void createIfNotExists() throws Exception {
        executeDDL("CREATE TABLE t(x INT)");
        executeDDL("CREATE INDEX t_x ON t(x)");
        assertNotNull(ais().getTable("test", "t").getIndex("t_x"));
        executeDDL("CREATE INDEX IF NOT EXISTS t_x ON t(x)");
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
