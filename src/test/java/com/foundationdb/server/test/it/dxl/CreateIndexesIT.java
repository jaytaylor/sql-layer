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

package com.foundationdb.server.test.it.dxl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.IndexLacksColumnsException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.ProtectedIndexException;
import com.foundationdb.server.error.DuplicateIndexException;

import com.foundationdb.server.test.it.ITBase;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertEquals;

public final class CreateIndexesIT extends ITBase
{
    private AkibanInformationSchema createAISWithTable(Integer... tableIds) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        for(Integer id : tableIds) {
            final Table curTable = getTable(id);
            Table.create(ais, curTable.getName().getSchemaName(), curTable.getName().getTableName(), id);
        }
        return ais;
    }

    private TableIndex addIndex(AkibanInformationSchema ais, Integer tableId, String indexName, boolean isUnique,
                                String... refColumns) {
        final Table newTable = ais.getTable(tableId);
        assertNotNull(newTable);
        final Table curTable = getTable(tableId);
        
        final TableIndex index = TableIndex.create(ais, newTable, indexName, 0, isUnique, false, isUnique ? new TableName(curTable.getName().getSchemaName(), "ukey") : null);

        int pos = 0;
        for (String colName : refColumns) {
            Column col = curTable.getColumn(colName);
            Column refCol = Column.create(newTable, col.getName(), col.getPosition(), col.getType());
            IndexColumn.create(index, refCol, pos++, true, null);
        }
        return index;
    }

    private void checkIndexIDsInGroup(Group group) {
        final Map<Integer,Index> idMap = new TreeMap<>();
        for(Table table : ddl().getAIS(session()).getTables().values()) {
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

    @Test
    public void emptyIndexList() throws InvalidOperationException {
        ArrayList<Index> indexes = new ArrayList<>();
        ddl().createIndexes(session(), indexes);
    }

    @Test(expected=NoSuchTableException.class)
    public void unknownTable() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "index", false);
        ddl().dropTable(session(), new TableName("test", "t"));
        ddl().createIndexes(session(), Arrays.asList(index));
    }

    @Test(expected=IndexLacksColumnsException.class)
    public void noIndexColumns() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, foo int");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "Failure", false);
        ddl().createIndexes(session(), Arrays.asList(index));
    }

    @Test(expected=ProtectedIndexException.class) 
    public void createPrimaryKey() throws InvalidOperationException {
        int tId = createTable("test", "atable", "id int");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "atable");
        Index index = TableIndex.create(ais, table, "PRIMARY", 1, true, true);
        ddl().createIndexes(session(), Arrays.asList(index));
    }
    
    @Test(expected=DuplicateIndexException.class) 
    public void duplicateIndexName() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "PRIMARY", false, "id");
        ddl().createIndexes(session(), Arrays.asList(index));
    }
    
    @Test(expected=NoSuchColumnException.class) 
    public void unknownColumnName() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "t");
        Index index = TableIndex.create(ais, table, "id", 0, false, false);
        Column refCol = Column.create(table, "foo", 0, typesRegistry().getTypeClass("MCOMPAT", "INT").instance(true));
        IndexColumn.create(index, refCol, 0, true, 0);
        ddl().createIndexes(session(), Arrays.asList(index));
    }
  
    @Test(expected=IllegalArgumentException.class)
    public void mismatchedColumnType() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "t");
        Index index = TableIndex.create(ais, table, "id", 0, false, false);
        Column refCol = Column.create(table, "id", 0, typesRegistry().getTypeClass("MCOMPAT", "BLOB").instance(true));
        IndexColumn.create(index, refCol, 0, true, 0);
        ddl().createIndexes(session(), Arrays.asList(index));
    }
    
    @Test
    public void basicConfirmInAIS() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "name", false, "name");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        
        // Index should exist on the Table
        Table table = getTable("test", "t");
        assertNotNull(table);
        assertNotNull(table.getIndex("name"));
    }

    @Test
    public void rightJoinPersistence() throws Exception {
        createTable("test", "c", "cid int not null primary key, name varchar(255)");
        int oid = createTable("test", "o", "oid int not null primary key, c_id int, priority int, " + akibanFK("c_id", "c", "cid"));
        AkibanInformationSchema ais = ddl().getAIS(session());
        TableName groupName = ais.getTable(oid).getGroup().getName();
        GroupIndex createdGI = createRightGroupIndex(groupName, "my_gi", "c.name", "o.priority");
        assertEquals("join type", Index.JoinType.RIGHT, createdGI.getJoinType());

        GroupIndex confirmationGi = ddl().getAIS(session()).getGroup(groupName).getIndex("my_gi");
        assertNotNull("gi not found", confirmationGi);

        safeRestartTestServices();

        GroupIndex reconstructedGi = ddl().getAIS(session()).getGroup(groupName).getIndex("my_gi");
        assertNotSame("GIs were same instance", createdGI, reconstructedGi);
        assertEquals("join type", Index.JoinType.RIGHT, reconstructedGi.getJoinType());
    }

    @Test
    public void nonUniqueVarchar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        writeRow(tId, 1, "bob");
        writeRow(tId, 2, "jim");
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "name", false, "name");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        
        AkibanInformationSchema aisCheck = ais();
        Index indexCheck = aisCheck.getTable(tId).getIndex("name");
        assertNotNull(indexCheck);
        assertFalse(indexCheck.isUnique());
        assertFalse(indexCheck.isPrimaryKey());
        assertFalse(indexCheck.isConnectedToFK());
        assertEquals("Index count", 1, indexCheck.getKeyColumns().size());
        
        List<Row> rows = scanAllIndex(getTable(tId).getIndex("name"));
        assertEquals("rows from index scan", 2, rows.size());
    }
    
    @Test
    public void nonUniqueVarcharMiddleOfGroup() throws InvalidOperationException {
        int cId = createTable("coi", "c", "cid int not null primary key, name varchar(32)");
        int oId = createTable("coi", "o", "oid int not null primary key, c_id int, tag varchar(32), FOREIGN KEY (c_id) REFERENCES c(cid)");
        int iId = createTable("coi", "i", "iid int not null primary key, o_id int, idesc varchar(32), FOREIGN KEY (o_id) REFERENCES o(oid)");
        
        // One customer 
        writeRow(cId, 1, "bob");

        // Two orders
        writeRow(oId, 1, 1, "supplies");
        writeRow(oId, 2, 1, "random");

        // Two/three items per order
        writeRow(iId, 1, 1, "foo");
        writeRow(iId, 2, 1, "bar");
        writeRow(iId, 3, 2, "zap");
        writeRow(iId, 4, 2, "fob");
        writeRow(iId, 5, 2, "baz");
        
        // Create index on an varchar (note: in the "middle" of a group, shifts IDs after, etc)
        AkibanInformationSchema ais = createAISWithTable(oId);
        Index index = addIndex(ais, oId, "tag", false, "tag");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        AkibanInformationSchema aisCheck = ais();
        Index indexCheck = aisCheck.getTable(oId).getIndex("tag");
        assertNotNull(indexCheck);
        assertFalse(indexCheck.isUnique());
        assertFalse(indexCheck.isPrimaryKey());
        assertFalse(indexCheck.isConnectedToFK());
        assertEquals("Index count", 1, indexCheck.getKeyColumns().size());
        
        // Get all customers
        List<Row> rows = scanAll(cId);
        assertEquals("customers from table scan", 1, rows.size());
        // Get all orders
        rows = scanAll(oId);
        assertEquals("orders from table scan", 2, rows.size());
        // Get all items
        rows = scanAll(iId);
        assertEquals("items from table scan", 5, rows.size());
        // Index scan on new index
        rows = scanAllIndex(getTable(oId).getIndex("tag"));
        assertEquals("orders from index scan", 2, rows.size());
    }
    
    @Test
    public void nonUniqueCompoundVarcharVarchar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, \"first\" varchar(250), \"last\" varchar(250)");
        
        expectRowCount(tId, 0);
        writeRow(tId, 1, "foo", "bar");
        writeRow(tId, 2, "zap", "snap");
        writeRow(tId, 3, "baz", "fob");
        expectRowCount(tId, 3);
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "name", false, "first", "last");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        
        AkibanInformationSchema aisCheck = ais();
        Index indexCompound = aisCheck.getTable(tId).getIndex("name");
        List<IndexColumn> ids = indexCompound.getKeyColumns();
        assertEquals("column first", "first", ids.get(0).getColumn().getName());
        assertEquals("column first", "last", ids.get(1).getColumn().getName());
        assertEquals(ids.size(), 2);
             
        List<Row> rows = scanAllIndex(getTable(tId).getIndex("name"));
        assertEquals("rows from index scan", 3, rows.size());
    }
    
    @Test
    public void uniqueChar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, state char(2)");
        
        expectRowCount(tId, 0);
        writeRow(tId, 1, "IA");
        writeRow(tId, 2, "WA");
        writeRow(tId, 3, "MA");
        expectRowCount(tId, 3);
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "state", true, "state");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();

        AkibanInformationSchema aisCheck = ais();
        Index indexCheck = aisCheck.getTable(tId).getIndex("state");
        assertNotNull(indexCheck);
        assertTrue(indexCheck.isUnique());
        assertFalse(indexCheck.isPrimaryKey());
        assertFalse(indexCheck.isConnectedToFK());
        assertEquals("column name: state", "state", indexCheck.getKeyColumns().get(0).getColumn().getName());
        assertEquals("Index count", 1, indexCheck.getKeyColumns().size());

        List<Row> rows = scanAllIndex(getTable(tId).getIndex("state"));
        assertEquals("rows from index scan", 3, rows.size());
    }

    @Test
    public void uniqueCharHasDuplicates() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, state char(2)");

        writeRow(tId, 1, "IA");
        writeRow(tId, 2, "WA");
        writeRow(tId, 3, "MA");
        writeRow(tId, 4, "IA");

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
        Table table = getTable(tId);
        assertNull("state index exists", table.getIndex("state"));
        assertNotNull("pk index doesn't exist", table.getIndex("PRIMARY"));
        assertEquals("Index count", 1, table.getIndexes().size());

        List<Row> rows = scanAll(tId);
        assertEquals("rows from table scan", 4, rows.size());
    }
    
    @Test
    public void uniqueIntNonUniqueDecimal() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, \"otherId\" int, price decimal(10,2)");
        
        expectRowCount(tId, 0);
        writeRow(tId, 1, 1337, "10.50");
        writeRow(tId, 2, 5000, "10.50");
        writeRow(tId, 3, 47000, "9.99");
        expectRowCount(tId, 3);
        
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index1 = addIndex(ais, tId, "otherId", true, "otherId");
        Index index2 = addIndex(ais, tId, "price", false, "price");
        ddl().createIndexes(session(), Arrays.asList(index1, index2));
        updateAISGeneration();

        AkibanInformationSchema aisCheck = ais();
        Index indexCheck = aisCheck.getTable(tId).getIndex("otherId");
        assertNotNull(indexCheck);
        assertTrue(indexCheck.isUnique());
        assertFalse(indexCheck.isPrimaryKey());
        assertFalse(indexCheck.isConnectedToFK());
        assertEquals("column name: otherId", "otherId", indexCheck.getKeyColumns().get(0).getColumn().getName());
        assertEquals("Index count", 1, indexCheck.getKeyColumns().size());
        
        Index indexCheck2 = aisCheck.getTable(tId).getIndex("price");
        assertNotNull(indexCheck2);
        assertFalse(indexCheck2.isUnique());
        assertFalse(indexCheck2.isPrimaryKey());
        assertFalse(indexCheck2.isConnectedToFK());
        assertEquals("column name: price", "price", indexCheck2.getKeyColumns().get(0).getColumn().getName());
        assertEquals("Index count", 1, indexCheck2.getKeyColumns().size());
        
        List<Row> rows = scanAllIndex(getTable(tId).getIndex("otherId"));
        assertEquals("rows from index scan", 3, rows.size());

        rows = scanAllIndex(getTable(tId).getIndex("price"));
        assertEquals("rows from index scan", 3, rows.size());
    }

    @Test
    public void uniqueIntNonUniqueIntWithDuplicates() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, i1 int, i2 int, price decimal(10,2)");
        createIndex("test", "t", "i1", "i1");

        writeRow(tId, 1, 10, 1337, "10.50");
        writeRow(tId, 2, 20, 5000, "10.50");
        writeRow(tId, 3, 30, 47000, "9.99");
        writeRow(tId, 4, 40, 47000, "9.99");

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
        Table table = getTable(tId);
        assertNull("i2 index exists", table.getIndex("i2"));
        assertNull("price index exists", table.getIndex("price"));
        assertNotNull("pk index doesn't exist", table.getIndex("PRIMARY"));
        assertNotNull("i1 index doesn't exist", table.getIndex("i1"));
        assertEquals("Index count", 2, table.getIndexes().size());

        List<Row> rows = scanAll(tId);
        assertEquals("rows from table scan", 4, rows.size());
    }

    @Test
    public void multipleTablesNonUniqueIntNonUniqueInt() throws InvalidOperationException {
        int tid = createTable("test", "t", "id int not null primary key, foo int");
        int uid = createTable("test", "u", "id int not null primary key, bar int");
        writeRow(tid, 1, 42);
        writeRow(tid, 2, 43);
        writeRow(uid, 1, 44);
        
        AkibanInformationSchema ais = createAISWithTable(tid, uid);
        Index index1 = addIndex(ais, tid, "foo", false, "foo");
        Index index2 = addIndex(ais, uid, "bar", false, "bar");
        ddl().createIndexes(session(), Arrays.asList(index1, index2));
        updateAISGeneration();

        AkibanInformationSchema aisCheck = ais();
        Index tidIndex = aisCheck.getTable(tid).getIndex("foo"); 
        Index uidIndex = aisCheck.getTable(uid).getIndex("bar");
        assertNotNull(tidIndex);
        assertTrue(tidIndex.isTableIndex());
        assertFalse(tidIndex.isUnique());
        assertNotNull(uidIndex);
        assertTrue(uidIndex.isTableIndex());
        assertFalse(uidIndex.isUnique());
        
        List<Row> rows = scanAllIndex(getTable(tid).getIndex("foo"));
        assertEquals("t rows from index scan", 2, rows.size());
        
        rows = scanAllIndex(getTable(uid).getIndex("bar"));
        assertEquals("u rows from index scan", 1, rows.size());
    }

    @Test
    public void oneIndexCheckIDs() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, foo int");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "foo", false, "foo");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        checkIndexIDsInGroup(getTable(tId).getGroup());
    }

    @Test
    public void twoIndexesAtOnceCheckIDs() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, foo int, bar int");
        AkibanInformationSchema ais = createAISWithTable(tId);
        Index index1 = addIndex(ais, tId, "foo", false, "foo");
        Index index2 = addIndex(ais, tId, "bar", false, "bar");
        ddl().createIndexes(session(), Arrays.asList(index1, index2));
        updateAISGeneration();
        checkIndexIDsInGroup(getTable(tId).getGroup());
    }

    @Test
    public void twoIndexSeparatelyCheckIDs() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, foo int, bar int");
        final AkibanInformationSchema ais = createAISWithTable(tId);
        Index index = addIndex(ais, tId, "foo", false, "foo");
        ddl().createIndexes(session(), Arrays.asList(index));
        updateAISGeneration();
        checkIndexIDsInGroup(getTable(tId).getGroup());

        Index index2 = addIndex(ais, tId, "bar", false, "bar");
        ddl().createIndexes(session(), Arrays.asList(index2));
        updateAISGeneration();
        checkIndexIDsInGroup(getTable(tId).getGroup());
    }
}
