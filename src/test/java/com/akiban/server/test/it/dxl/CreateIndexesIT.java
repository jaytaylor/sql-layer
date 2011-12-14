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

package com.akiban.server.test.it.dxl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.IndexLacksColumnsException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.JoinColumnTypesMismatchException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.ProtectedIndexException;

import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.dxl.DXLServiceImpl;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.util.GroupIndexCreator;
import com.google.inject.Inject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public final class CreateIndexesIT extends ITBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider().bind(DXLService.class, StartHookDxlService.class);
    }

    private AkibanInformationSchema createAISWithTable(Integer... tableIds) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        for(Integer id : tableIds) {
            final UserTable curTable = getUserTable(id);
            UserTable.create(ais, curTable.getName().getSchemaName(), curTable.getName().getTableName(), id);
        }
        return ais;
    }

    private TableIndex addIndex(AkibanInformationSchema ais, Integer tableId, String indexName, boolean isUnique,
                                String... refColumns) {
        final UserTable newTable = ais.getUserTable(tableId);
        assertNotNull(newTable);
        final UserTable curTable = getUserTable(tableId);
        final TableIndex index = TableIndex.create(ais, newTable, indexName, -1, isUnique, isUnique ? "UNIQUE" : "KEY");

        int pos = 0;
        for (String colName : refColumns) {
            Column col = curTable.getColumn(colName);
            Column refCol = Column.create(newTable, col.getName(), col.getPosition(), col.getType());
            refCol.setTypeParameter1(col.getTypeParameter1());
            refCol.setTypeParameter2(col.getTypeParameter2());
            Integer indexedLen = col.getMaxStorageSize().intValue();
            index.addColumn(new IndexColumn(index, refCol, pos++, true, indexedLen));
        }
        return index;
    }

    private void checkDDL(Integer tableId, String expected) {
        final UserTable table = getUserTable(tableId);
        DDLGenerator gen = new DDLGenerator();
        String actual = gen.createTable(table);
        assertEquals(table.getName() + "'s create statement", expected, actual);
    }

    private void checkIndexIDsInGroup(Group group) {
        final Map<Integer,Index> idMap = new TreeMap<Integer,Index>();
        for(UserTable table : ddl().getAIS(session()).getUserTables().values()) {
            if(table.getGroup().equals(group)) {
                for(Index index : table.getIndexesIncludingInternal()) {
                    final Integer id = index.getIndexId();
                    final Index prevIndex = idMap.get(id);
                    if(prevIndex != null) {
                        Assert.fail(String.format("%s and %s have the same ID: %d", index, prevIndex, id));
                    }
                    idMap.put(id, index);
                }
            }
        }
    }

    @Before
    public void logRecreatedGis() {
        recreatedGiNames = new ArrayList<String>();
    }

    @After
    public void clearRecreatedGis() {
        recreatedGiNames = null;
    }
    
    @Test
    public void emptyIndexList() throws InvalidOperationException {
        ArrayList<Index> indexes = new ArrayList<Index>();
        ddl().createIndexes(session(), indexes);
    }

    @Test(expected=NoSuchTableException.class)
    public void unknownTable() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "index", false);
        ddl().dropTable(session(), new TableName("test", "t"));
        ddl().createIndexes(session(), Arrays.asList(index));
    }

    @Test(expected=IndexLacksColumnsException.class)
    public void noIndexColumns() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, foo int");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "Failure", false);
        ddl().createIndexes(session(), Arrays.asList(index));
    }

    @Test(expected=ProtectedIndexException.class) 
    public void createPrimaryKey() throws InvalidOperationException {
        int tId = createTable("test", "atable", "id int");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getUserTable("test", "atable");
        Index index = TableIndex.create(ais, table, "PRIMARY", 1, false, "PRIMARY");
        ddl().createIndexes(session(), Arrays.asList(index));
    }
    
    @Test(expected=DuplicateIndexException.class) 
    public void duplicateIndexName() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "PRIMARY", false, "id");
        ddl().createIndexes(session(), Arrays.asList(index));
    }
    
    @Test(expected=NoSuchColumnException.class) 
    public void unknownColumnName() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "t");
        Index index = TableIndex.create(ais, table, "id", -1, false, "KEY");
        Column refCol = Column.create(table, "foo", 0, Types.INT);
        index.addColumn(new IndexColumn(index, refCol, 0, true, 0));
        ddl().createIndexes(session(), Arrays.asList(index));
    }
  
    @Test(expected=JoinColumnTypesMismatchException.class) 
    public void mismatchedColumnType() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "t");
        Index index = TableIndex.create(ais, table, "id", -1, false, "KEY");
        Column refCol = Column.create(table, "id", 0, Types.BLOB);
        index.addColumn(new IndexColumn(index, refCol, 0, true, 0));
        ddl().createIndexes(session(), Arrays.asList(index));
    }
    
    @Test
    public void basicConfirmInAIS() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, name varchar(255)");
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "name", false, "name");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        
        // Index should exist on the UserTable
        UserTable uTable = getUserTable("test", "t");
        assertNotNull(uTable);
        assertNotNull(uTable.getIndex("name"));
        
        // Index should exist on the GroupTable
        GroupTable gTable = uTable.getGroup().getGroupTable();
        assertNotNull(gTable);
        assertNotNull(gTable.getIndex("t$name"));
    }

    @Test
    public void rightJoinPersistence() throws Exception {
        createTable("test", "c", "cid int primary key, name varchar(255)");
        int oid = createTable("test", "o", "oid int key, c_id int, priority int, " + akibanFK("c_id", "c", "cid"));
        AkibanInformationSchema ais = ddl().getAIS(session());
        String groupName = ais.getUserTable(oid).getGroup().getName();
        GroupIndex createdGI = GroupIndexCreator.createIndex(
                ais,
                groupName,
                "my_gi",
                "c.name,o.priority",
                Index.JoinType.RIGHT
        );
        assertEquals("join type", Index.JoinType.RIGHT, createdGI.getJoinType());
        ddl().createIndexes(session(), Collections.singleton(createdGI));

        GroupIndex confirmationGi = ddl().getAIS(session()).getGroup(groupName).getIndex("my_gi");
        assertNotNull("gi not found", confirmationGi);

        safeRestartTestServices();

        GroupIndex reconstructedGi = ddl().getAIS(session()).getGroup(groupName).getIndex("my_gi");
        assertNotSame("GIs were same instance", createdGI, reconstructedGi);
        assertEquals("join type", Index.JoinType.RIGHT, reconstructedGi.getJoinType());
    }

    @Test
    public void invalidGiRecreatedOnStartup() throws Exception {
        createTable("test", "c", "cid int primary key, name varchar(255)");
        int oid = createTable("test", "o", "oid int key, c_id int, priority int, " + akibanFK("c_id", "c", "cid"));
        AkibanInformationSchema ais = ddl().getAIS(session());
        String groupName = ais.getUserTable(oid).getGroup().getName();
        GroupIndex invalidIndex = GroupIndexCreator.createIndex(
                ais,
                groupName,
                "my_gi",
                "c.name,o.priority",
                Index.JoinType.LEFT
        );
        ddl().createIndexes(session(), Collections.singleton(invalidIndex));

        recreatedGiNames.clear();
        safeRestartTestServices();

        assertEquals("recreated GIs", Collections.singletonList(recreatingString("my_gi", false)), recreatedGiNames);
    }
    
    @Test
    public void nonUniqueVarchar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, name varchar(255)");
        dml().writeRow(session(), createNewRow(tId, 1, "bob"));
        dml().writeRow(session(), createNewRow(tId, 2, "jim"));
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "name", false, "name");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        
        checkDDL(tId, "create table `test`.`t`(`id` int, `name` varchar(255), PRIMARY KEY(`id`), KEY `name`(`name`)) engine=akibandb");
        
        List<NewRow> rows = scanAllIndex(getUserTable(tId).getIndex("name"));
        assertEquals("rows from index scan", 2, rows.size());
    }
    
    @Test
    public void nonUniqueVarcharMiddleOfGroup() throws InvalidOperationException {
        int cId = createTable("coi", "c", "cid int key, name varchar(32)");
        int oId = createTable("coi", "o", "oid int key, c_id int, tag varchar(32), CONSTRAINT __akiban_fk_c FOREIGN KEY __akiban_fk_c (c_id) REFERENCES c(cid)");
        int iId = createTable("coi", "i", "iid int key, o_id int, idesc varchar(32), CONSTRAINT __akiban_fk_i FOREIGN KEY __akiban_fk_i (o_id) REFERENCES o(oid)");
        
        // One customer 
        dml().writeRow(session(), createNewRow(cId, 1, "bob"));

        // Two orders
        dml().writeRow(session(), createNewRow(oId, 1, 1, "supplies"));
        dml().writeRow(session(), createNewRow(oId, 2, 1, "random"));

        // Two/three items per order
        dml().writeRow(session(), createNewRow(iId, 1, 1, "foo"));
        dml().writeRow(session(), createNewRow(iId, 2, 1, "bar"));
        dml().writeRow(session(), createNewRow(iId, 3, 2, "zap"));
        dml().writeRow(session(), createNewRow(iId, 4, 2, "fob"));
        dml().writeRow(session(), createNewRow(iId, 5, 2, "baz"));
        
        // Create index on an varchar (note: in the "middle" of a group, shifts IDs after, etc)
        AkibanInformationSchema ais = createAISWithTable(oId);
        Index index = addIndex(ais, oId, "tag", false, "tag");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        checkDDL(oId, "create table `coi`.`o`(`oid` int, `c_id` int, `tag` varchar(32), PRIMARY KEY(`oid`), "+
                      "KEY `tag`(`tag`), CONSTRAINT `__akiban_fk_c` FOREIGN KEY `__akiban_fk_c`(`c_id`) REFERENCES `c`(`cid`)) engine=akibandb");

        // Get all customers
        List<NewRow> rows = scanAll(scanAllRequest(cId));
        assertEquals("customers from table scan", 1, rows.size());
        // Get all orders
        rows = scanAll(scanAllRequest(oId));
        assertEquals("orders from table scan", 2, rows.size());
        // Get all items
        rows = scanAll(scanAllRequest(iId));
        assertEquals("items from table scan", 5, rows.size());
        // Index scan on new index
        rows = scanAllIndex(getUserTable(oId).getIndex("tag"));
        assertEquals("orders from index scan", 2, rows.size());
    }
    
    @Test
    public void nonUniqueCompoundVarcharVarchar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, first varchar(255), last varchar(255)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, "foo", "bar"));
        dml().writeRow(session(), createNewRow(tId, 2, "zap", "snap"));
        dml().writeRow(session(), createNewRow(tId, 3, "baz", "fob"));
        expectRowCount(tId, 3);
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "name", false, "first", "last");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        
        checkDDL(tId, "create table `test`.`t`(`id` int, `first` varchar(255), `last` varchar(255), "+
                      "PRIMARY KEY(`id`), KEY `name`(`first`, `last`)) engine=akibandb");

        List<NewRow> rows = scanAllIndex(getUserTable(tId).getIndex("name"));
        assertEquals("rows from index scan", 3, rows.size());
    }
    
    @Test
    public void uniqueChar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, state char(2)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, "IA"));
        dml().writeRow(session(), createNewRow(tId, 2, "WA"));
        dml().writeRow(session(), createNewRow(tId, 3, "MA"));
        expectRowCount(tId, 3);
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "state", true, "state");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        
        checkDDL(tId, "create table `test`.`t`(`id` int, `state` char(2), "+
                      "PRIMARY KEY(`id`), UNIQUE `state`(`state`)) engine=akibandb");

        List<NewRow> rows = scanAllIndex(getUserTable(tId).getIndex("state"));
        assertEquals("rows from index scan", 3, rows.size());
    }

    @Test
    public void uniqueCharHasDuplicates() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, state char(2)");

        dml().writeRow(session(), createNewRow(tId, 1, "IA"));
        dml().writeRow(session(), createNewRow(tId, 2, "WA"));
        dml().writeRow(session(), createNewRow(tId, 3, "MA"));
        dml().writeRow(session(), createNewRow(tId, 4, "IA"));

        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "state", true, "state");

        try {
            ddl().createIndexes(session(), Arrays.asList(index));
            Assert.fail("DuplicateKeyException expected!");
        } catch(DuplicateKeyException e) {
            // Expected
        }
        updateAISGeneration();

        // Make sure index is not in AIS
        Table table = getUserTable(tId);
        assertNull("state index exists", table.getIndex("state"));
        assertNotNull("pk index doesn't exist", table.getIndex("PRIMARY"));
        assertEquals("Index count", 1, table.getIndexes().size());

        List<NewRow> rows = scanAll(scanAllRequest(tId));
        assertEquals("rows from table scan", 4, rows.size());
    }
    
    @Test
    public void uniqueIntNonUniqueDecimal() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, otherId int, price decimal(10,2)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, 1337, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 2, 5000, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 3, 47000, "9.99"));
        expectRowCount(tId, 3);
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index1 = addIndex(ais, tId, "otherId", true, "otherId");
        Index index2 = addIndex(ais, tId, "price", false, "price");
        ddl().createIndexes(session(), Arrays.asList(index1, index2));
        updateAISGeneration();
        
        checkDDL(tId, "create table `test`.`t`(`id` int, `otherId` int, `price` decimal(10, 2), "+
                      "PRIMARY KEY(`id`), UNIQUE `otherId`(`otherId`), KEY `price`(`price`)) engine=akibandb");

        List<NewRow> rows = scanAllIndex(getUserTable(tId).getIndex("otherId"));
        assertEquals("rows from index scan", 3, rows.size());

        rows = scanAllIndex(getUserTable(tId).getIndex("price"));
        assertEquals("rows from index scan", 3, rows.size());
    }

    @Test
    public void uniqueIntNonUniqueIntWithDuplicates() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, i1 int, i2 int, price decimal(10,2), index(i1)");

        dml().writeRow(session(), createNewRow(tId, 1, 10, 1337, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 2, 20, 5000, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 3, 30, 47000, "9.99"));
        dml().writeRow(session(), createNewRow(tId, 4, 40, 47000, "9.99"));

        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index1 = addIndex(ais, tId, "otherId", true, "i2");
        Index index2 = addIndex(ais, tId, "price", false, "price");

        try {
            ddl().createIndexes(session(), Arrays.asList(index1, index2));
            Assert.fail("DuplicateKeyException expected!");
        } catch(DuplicateKeyException e) {
            // Expected
        }
        updateAISGeneration();

        // Make sure index is not in AIS
        Table table = getUserTable(tId);
        assertNull("i2 index exists", table.getIndex("i2"));
        assertNull("price index exists", table.getIndex("price"));
        assertNotNull("pk index doesn't exist", table.getIndex("PRIMARY"));
        assertNotNull("i1 index doesn't exist", table.getIndex("i1"));
        assertEquals("Index count", 2, table.getIndexes().size());

        List<NewRow> rows = scanAll(scanAllRequest(tId));
        assertEquals("rows from table scan", 4, rows.size());
    }

    @Test
    public void multipleTablesNonUniqueIntNonUniqueInt() throws InvalidOperationException {
        int tid = createTable("test", "t", "id int primary key, foo int");
        int uid = createTable("test", "u", "id int primary key, bar int");
        dml().writeRow(session(), createNewRow(tid, 1, 42));
        dml().writeRow(session(), createNewRow(tid, 2, 43));
        dml().writeRow(session(), createNewRow(uid, 1, 44));
        
        AkibanInformationSchema ais = createAISWithTable(tid, uid);
        Index index1 = addIndex(ais, tid, "foo", false, "foo");
        Index index2 = addIndex(ais, uid, "bar", false, "bar");
        ddl().createIndexes(session(), Arrays.asList(index1, index2));
        updateAISGeneration();

        checkDDL(tid, "create table `test`.`t`(`id` int, `foo` int, PRIMARY KEY(`id`), KEY `foo`(`foo`)) engine=akibandb");
        checkDDL(uid, "create table `test`.`u`(`id` int, `bar` int, PRIMARY KEY(`id`), KEY `bar`(`bar`)) engine=akibandb");

        List<NewRow> rows = scanAllIndex(getUserTable(tid).getIndex("foo"));
        assertEquals("t rows from index scan", 2, rows.size());
        
        rows = scanAllIndex(getUserTable(uid).getIndex("bar"));
        assertEquals("u rows from index scan", 1, rows.size());
    }

    @Test
    public void oneIndexCheckIDs() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int key, foo int");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "foo", false, "foo");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        checkIndexIDsInGroup(getUserTable(tId).getGroup());
    }

    @Test
    public void twoIndexesAtOnceCheckIDs() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int key, foo int, bar int");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index1 = addIndex(ais, tId, "foo", false, "foo");
        Index index2 = addIndex(ais, tId, "bar", false, "bar");
        ddl().createIndexes(session(), Arrays.asList(index1, index2));
        updateAISGeneration();
        checkIndexIDsInGroup(getUserTable(tId).getGroup());
    }

    @Test
    public void twoIndexSeparatelyCheckIDs() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int key, foo int, bar int");
        final AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "foo", false, "foo");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        checkIndexIDsInGroup(getUserTable(tId).getGroup());

        Index index2 = addIndex(ais, tId, "bar", false, "bar");
        ddl().createIndexes(session(), Arrays.asList(index2));
        updateAISGeneration();
        checkIndexIDsInGroup(getUserTable(tId).getGroup());
    }

    public static class StartHookDxlService extends DXLServiceImpl {

        @Inject
        public StartHookDxlService(SchemaManager schemaManager, Store store, TreeService treeService,
                                   SessionService sessionService, IndexStatisticsService indexStatisticsService) {
            super(schemaManager, store, treeService, sessionService, indexStatisticsService);
        }

        @Override
        protected void groupIndexMayNeedRecreating(GroupIndex groupIndex, boolean needsRecreating) {
            recreatedGiNames.add(recreatingString(groupIndex.getIndexName().getName(), needsRecreating));
        }
    }

    private static String recreatingString(String giName, boolean needsRecreating) {
        return giName + ", " + needsRecreating;
    }

    private static volatile List<String> recreatedGiNames = null;
}
