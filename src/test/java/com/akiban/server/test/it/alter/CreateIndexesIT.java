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

package com.akiban.server.test.it.alter;

import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.ddl.IndexAlterException;
import com.akiban.server.api.dml.DuplicateKeyException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


public final class CreateIndexesIT extends AlterTestBase {
    /*
     * Test DDL.createIndexes() API 
     */

    @Test
    public void createIndexNoIndexes() throws InvalidOperationException {
        // Passing an empty list should work
        ArrayList<Index> indexes = new ArrayList<Index>();
        ddl().createIndexes(session(), indexes);
    }
    
    @Test(expected=NoSuchTableException.class)
    public void createIndexInvalidTable() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key");
        // Attempt to add index to unknown table
        AkibanInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "index", null, false);
        ddl().getAIS(session()).getUserTables().remove(new TableName("test", "t"));
        ddl().createIndexes(session(), getAllIndexes(ais));
    }
    
    @Test(expected=IndexAlterException.class) 
    public void createIndexMultipleTables() throws InvalidOperationException {
        createTable("test", "t1", "id int key");
        createTable("test", "t2", "id int key");
        AkibanInformationSchema ais = ddl().getAIS(session());
        // Attempt to add indexes to multiple tables
        addIndexToAIS(ais, "test", "t1", "index", null, false);
        addIndexToAIS(ais, "test", "t2", "index", null, false);
        ddl().createIndexes(session(), getAllIndexes(ais));
    }

    @Test(expected=IndexAlterException.class) 
    public void createIndexInvalidTableId() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key");
        // Attempt to add to unknown table id
        AkibanInformationSchema ais = createAISWithTable(tId);
        ais.getUserTables().values().iterator().next().setTableId(-1);
        addIndexToAIS(ais, "test", "t", "id", null, false);
        ddl().createIndexes(session(), getAllIndexes(ais));
    }
    
    @Test(expected=IndexAlterException.class) 
    public void createIndexPrimaryKey() throws InvalidOperationException {
        int tId = createTable("test", "atable", "id int");
        // Attempt to a primary key
        AkibanInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "atable");
        Index.create(ais, table, "PRIMARY", 1, false, "PRIMARY");
        ddl().createIndexes(session(), getAllIndexes(ais));
    }
    
    @Test(expected=IndexAlterException.class) 
    public void createIndexDuplicateIndexName() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key");
        // Attempt to add duplicate index name
        AkibanInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "PRIMARY", new String[]{"id"}, false);
        ddl().createIndexes(session(), getAllIndexes(ais));
    }
    
    @Test(expected=IndexAlterException.class) 
    public void createIndexWrongColumName() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key");
        // Attempt to add duplicate index name
        AkibanInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "t");
        Table curTable = ddl().getAIS(session()).getTable("test", "t");
        Index index = Index.create(ais, table, "id", -1, false, "KEY");
        Column refCol = Column.create(table, "foo", 0, Types.INT);
        index.addColumn(new IndexColumn(index, refCol, 0, true, 0));
        ddl().createIndexes(session(), getAllIndexes(ais));
    }
  
    @Test(expected=IndexAlterException.class) 
    public void createIndexWrongColumType() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key");
        // Attempt to add duplicate index name
        AkibanInformationSchema ais = createAISWithTable(tId);
        Table table = ais.getTable("test", "t");
        Table curTable = ddl().getAIS(session()).getTable("test", "t");
        Index index = Index.create(ais, table, "id", -1, false, "KEY");
        Column refCol = Column.create(table, "id", 0, Types.BLOB);
        index.addColumn(new IndexColumn(index, refCol, 0, true, 0));
        ddl().createIndexes(session(), getAllIndexes(ais));
    }
    
    
    /* 
     * Test creating various types of indexes
     */
    
    @Test
    public void createIndexConfirmAIS() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, name varchar(255)");
        
        // Create non-unique index on varchar
        AkibanInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "name", new String[]{"name"}, false);
        ddl().createIndexes(session(), getAllIndexes(ais));
        
        // Index should exist on the UserTable
        UserTable uTable = ddl().getAIS(session()).getUserTable("test", "t");
        assertNotNull(uTable);
        assertNotNull(uTable.getIndex("name"));
        
        // Index should exist on the GroupTable
        GroupTable gTable = uTable.getGroup().getGroupTable();
        assertNotNull(gTable);
        assertNotNull(gTable.getIndex("t$name"));
    }
    
    @Test
    public void createIndexSimple() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, name varchar(255)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, "bob"));
        dml().writeRow(session(), createNewRow(tId, 2, "jim"));
        expectRowCount(tId, 2);
        
        // Create non-unique index on varchar
        AkibanInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "name", new String[]{"name"}, false);
        ddl().createIndexes(session(), getAllIndexes(ais));
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `name` varchar(255), PRIMARY KEY(`id`), KEY `name`(`name`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session()).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 2, rows.size());
    }
    
    @Test
    public void createIndexMiddleOfGroup() throws InvalidOperationException {
        int cId = createTable("coi", "c", "cid int key, name varchar(32)");
        int oId = createTable("coi", "o", "oid int key, c_id int, tag varchar(32), CONSTRAINT __akiban_fk_c FOREIGN KEY __akiban_fk_c (c_id) REFERENCES c(cid)");
        int iId = createTable("coi", "i", "iid int key, o_id int, idesc varchar(32), CONSTRAINT __akiban_fk_i FOREIGN KEY __akiban_fk_i (o_id) REFERENCES o(oid)");
        
        // One customer 
        expectRowCount(cId, 0);
        dml().writeRow(session(), createNewRow(cId, 1, "bob"));
        expectRowCount(cId, 1);
        
        // Two orders
        expectRowCount(oId, 0);
        dml().writeRow(session(), createNewRow(oId, 1, 1, "supplies"));
        dml().writeRow(session(), createNewRow(oId, 2, 1, "random"));
        expectRowCount(oId, 2);
        
        // Two/three items per order
        expectRowCount(iId, 0);
        dml().writeRow(session(), createNewRow(iId, 1, 1, "foo"));
        dml().writeRow(session(), createNewRow(iId, 2, 1, "bar"));
        dml().writeRow(session(), createNewRow(iId, 3, 2, "zap"));
        dml().writeRow(session(), createNewRow(iId, 4, 2, "fob"));
        dml().writeRow(session(), createNewRow(iId, 5, 2, "baz"));
        expectRowCount(iId, 5);
        
        // Create index on an varchar (note: in the "middle" of a group, shifts IDs after, etc)
        AkibanInformationSchema ais = createAISWithTable(oId);
        addIndexToAIS(ais, "coi", "o", "tag", new String[]{"tag"}, false);
        ddl().createIndexes(session(), getAllIndexes(ais));
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `coi`.`o`(`oid` int, `c_id` int, `tag` varchar(32), PRIMARY KEY(`oid`), KEY `tag`(`tag`), CONSTRAINT `__akiban_fk_c` FOREIGN KEY `__akiban_fk_c`(`c_id`) REFERENCES `c`(`cid`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session()).getUserTable("coi", "o")));
        
        // Get all customers
        List<NewRow> rows = scanAll(new ScanAllRequest(cId, null));
        assertEquals("Customer rows scanned", 1, rows.size());
        
        // Get all orders
        rows = scanAll(new ScanAllRequest(oId, null));
        assertEquals("Order rows scanned", 2, rows.size());
        
        // Get all items
        rows = scanAll(new ScanAllRequest(iId, null));
        assertEquals("Item rows scanned", 5, rows.size());
    }
    
    @Test
    public void createIndexCompound() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, first varchar(255), last varchar(255)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, "foo", "bar"));
        dml().writeRow(session(), createNewRow(tId, 2, "zap", "snap"));
        dml().writeRow(session(), createNewRow(tId, 3, "baz", "fob"));
        expectRowCount(tId, 3);
        
        // Create non-unique compound index on two varchars
        AkibanInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "name", new String[]{"first","last"}, false);
        ddl().createIndexes(session(), getAllIndexes(ais));
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `first` varchar(255), `last` varchar(255), PRIMARY KEY(`id`), KEY `name`(`first`, `last`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session()).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 3, rows.size());
    }
    
    @Test
    public void createIndexUnique() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, state char(2)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, "IA"));
        dml().writeRow(session(), createNewRow(tId, 2, "WA"));
        dml().writeRow(session(), createNewRow(tId, 3, "MA"));
        expectRowCount(tId, 3);
        
        // Create unique index on a char(2)
        AkibanInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "state", new String[]{"state"}, true);
        ddl().createIndexes(session(), getAllIndexes(ais));
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `state` char(2), PRIMARY KEY(`id`), UNIQUE `state`(`state`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session()).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 3, rows.size());
    }

    @Test
    public void createIndexUniqueWithDuplicate() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, state char(2)");

        dml().writeRow(session(), createNewRow(tId, 1, "IA"));
        dml().writeRow(session(), createNewRow(tId, 2, "WA"));
        dml().writeRow(session(), createNewRow(tId, 3, "MA"));
        dml().writeRow(session(), createNewRow(tId, 4, "IA"));

        // Create unique index on a char(2) with a duplicate
        AkibanInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "state", new String[]{"state"}, true);

        try {
            ddl().createIndexes(session(), getAllIndexes(ais));
            Assert.fail("DuplicateKeyExcpetion expected!");
        } catch(DuplicateKeyException e) {
            // Expected
        }
        updateAISGeneration();

        // Make sure index is not in AIS
        Table table = getUserTable(tId);
        assertNull("state index exists", table.getIndex("state"));
        assertNotNull("pk index doesn't exist", table.getIndex("PRIMARY"));
        assertEquals("Index count", 1, table.getIndexes().size());

        // Check that we can still get old rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 4, rows.size());
    }
    
    @Test
    public void createMultipleIndexes() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, otherId int, price decimal(10,2)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, 1337, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 2, 5000, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 3, 47000, "9.99"));
        expectRowCount(tId, 3);
        
        // Create unique index on an int, non-unique index on decimal
        AkibanInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "otherId", new String[]{"otherId"}, true);
        addIndexToAIS(ais, "test", "t", "price", new String[]{"price"}, false);
        ddl().createIndexes(session(), getAllIndexes(ais));
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `otherId` int, `price` decimal(10, 2), PRIMARY KEY(`id`), UNIQUE `otherId`(`otherId`), KEY `price`(`price`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session()).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 3, rows.size());
    }

    @Test
    public void createMultipleIndexesWithFailure() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, i1 int, i2 int, price decimal(10,2), index(i1)");

        dml().writeRow(session(), createNewRow(tId, 1, 10, 1337, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 2, 20, 5000, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 3, 30, 47000, "9.99"));
        dml().writeRow(session(), createNewRow(tId, 4, 40, 47000, "9.99"));

        // Create unique index on an int, non-unique index on decimal
        AkibanInformationSchema ais = createAISWithTable(tId);
        addIndexToAIS(ais, "test", "t", "otherId", new String[]{"i2"}, true);
        addIndexToAIS(ais, "test", "t", "price", new String[]{"price"}, false);

        try {
            ddl().createIndexes(session(), getAllIndexes(ais));
            Assert.fail("DuplicateKeyExcpetion expected!");
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

        // Check that we can still get old rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 4, rows.size());
    }
}
