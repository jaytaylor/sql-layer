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

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.ddl.IndexAlterException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;


public final class DropIndexesIT extends AlterTestBase {
    /*
     * Test DDL.dropIndexes() API 
     */
    
    @Test
    public void dropIndexNoIndexes() throws InvalidOperationException {
        // Passing an empty list should work
        ArrayList<String> indexes = new ArrayList<String>();
        ddl().dropIndexes(session(), null, indexes);
    }
    
    @Test(expected=IndexAlterException.class) 
    public void dropIndexUnknownTable() throws InvalidOperationException {
        // Attempt to add index to unknown table
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("foo");
        ddl().dropIndexes(session(), tableName("test","bar"), indexes);
    }
    
    @Test(expected=IndexAlterException.class)
    public void dropIndexUnkownIndex() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, name varchar(255)");
        // Attempt to drop unknown index
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("name");
        ddl().dropIndexes(session(), tableName(tId), indexes);
    }

    @Test(expected=IndexAlterException.class)
    public void dropIndexImplicitPkey() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int, name varchar(255)");
        // Attempt to drop implicit primary key
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("PRIMARY");
        ddl().dropIndexes(session(), tableName(tId), indexes);
    }
    
    @Test(expected=IndexAlterException.class)
    public void dropIndexExplicitPkey() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, name varchar(255)");
        // Attempt to drop implicit primary key
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("PRIMARY");
        ddl().dropIndexes(session(), tableName(tId), indexes);
    }

    
    /* 
     * Test dropping various types of indexes
     */
    
    @Test
    public void dropIndexConfirmAIS() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, name varchar(255), index name(name)");

        // Drop non-unique index on varchar
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("name");
        ddl().dropIndexes(session(), tableName(tId), indexes);

        // Index should be gone from UserTable
        UserTable uTable = ddl().getAIS(session()).getUserTable("test", "t");
        assertNotNull(uTable);
        assertNull(uTable.getIndex("name"));

        // Index should exist on the GroupTable
        GroupTable gTable = uTable.getGroup().getGroupTable();
        assertNotNull(gTable);
        assertNull(gTable.getIndex("t$name"));
    }
    
    @Test
    public void dropIndexSimple() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, name varchar(255), index name(name)");

        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, "bob"));
        dml().writeRow(session(), createNewRow(tId, 2, "jim"));
        expectRowCount(tId, 2);

        // Drop non-unique index on varchar
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("name");
        ddl().dropIndexes(session(), tableName(tId), indexes);
        updateAISGeneration();

        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals(
                "New DDL",
                "create table `test`.`t`(`id` int, `name` varchar(255), PRIMARY KEY(`id`)) engine=akibandb",
                gen.createTable(ddl().getAIS(session()).getUserTable("test", "t")));

        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 2, rows.size());
    }
    
    @Test
    public void dropIndexMiddleOfGroup() throws InvalidOperationException {
        int cId = createTable("coi", "c", "cid int key, name varchar(32)");
        int oId = createTable("coi", "o", "oid int key, c_id int, tag varchar(32), key tag(tag), CONSTRAINT __akiban_fk_c FOREIGN KEY __akiban_fk_c (c_id) REFERENCES c(cid)");
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
        
        // Drop index on an varchar (note: in the "middle" of a group, shifts IDs after, etc)
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("tag");
        ddl().dropIndexes(session(), tableName(oId), indexes);
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `coi`.`o`(`oid` int, `c_id` int, `tag` varchar(32), PRIMARY KEY(`oid`), CONSTRAINT `__akiban_fk_c` FOREIGN KEY `__akiban_fk_c`(`c_id`) REFERENCES `c`(`cid`)) engine=akibandb",
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
    public void dropIndexCompound() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, first varchar(255), last varchar(255), key name(first,last)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, "foo", "bar"));
        dml().writeRow(session(), createNewRow(tId, 2, "zap", "snap"));
        dml().writeRow(session(), createNewRow(tId, 3, "baz", "fob"));
        expectRowCount(tId, 3);
        
        // Drop non-unique compound index on two varchars
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("name");
        ddl().dropIndexes(session(), tableName(tId), indexes);
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `first` varchar(255), `last` varchar(255), PRIMARY KEY(`id`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session()).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 3, rows.size());
    }
    
    @Test
    public void dropIndexUnique() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, state char(2), unique state(state)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, "IA"));
        dml().writeRow(session(), createNewRow(tId, 2, "WA"));
        dml().writeRow(session(), createNewRow(tId, 3, "MA"));
        expectRowCount(tId, 3);
        
        // Drop unique index on a char(2)
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("state");
        ddl().dropIndexes(session(), tableName(tId), indexes);
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `state` char(2), PRIMARY KEY(`id`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session()).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 3, rows.size());
    }
    
    @Test
    public void dropMultipleIndexes() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int primary key, otherId int, price decimal(10,2), unique otherId(otherId), key price(price)");
        
        expectRowCount(tId, 0);
        dml().writeRow(session(), createNewRow(tId, 1, 1337, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 2, 5000, "10.50"));
        dml().writeRow(session(), createNewRow(tId, 3, 47000, "9.99"));
        expectRowCount(tId, 3);
        
        // Drop unique index on an int, non-unique index on decimal
        ArrayList<String> indexes = new ArrayList<String>();
        indexes.add("otherId");
        indexes.add("price");
        ddl().dropIndexes(session(), tableName(tId), indexes);
        updateAISGeneration();
        
        // Check that AIS was updated and DDL gets created correctly
        DDLGenerator gen = new DDLGenerator();
        assertEquals("New DDL",
                     "create table `test`.`t`(`id` int, `otherId` int, `price` decimal(10, 2), PRIMARY KEY(`id`)) engine=akibandb",
                     gen.createTable(ddl().getAIS(session()).getUserTable("test", "t")));
        
        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tId, null));
        assertEquals("Rows scanned", 3, rows.size());
    }
}
