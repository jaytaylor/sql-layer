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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.NoSuchIndexException;
import com.foundationdb.server.error.ProtectedIndexException;

import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public final class DropIndexesIT extends ITBase {
        
    @Test
    public void emptyIndexList() throws InvalidOperationException {
        int tid = createTable("test", "t", "id int not null primary key");
        ddl().dropTableIndexes(session(), tableName(tid), Collections.<String>emptyList());
    }
    
    @Test(expected=NoSuchTableException.class)
    public void unknownTable() throws InvalidOperationException {
        ddl().dropTableIndexes(session(), tableName("test", "bar"), Arrays.asList("bar"));
    }
    
    @Test(expected=NoSuchIndexException.class)
    public void unknownIndex() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("name"));
    }

    @Test(expected=NoSuchIndexException.class)
    public void hiddenPrimaryKey() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int, name varchar(255)");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("PRIMARY"));
    }
    
    @Test(expected=ProtectedIndexException.class)
    public void declaredPrimaryKey() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("PRIMARY"));
    }

    @Test
    public void basicConfirmNotInAIS() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        createIndex("test", "t", "name", "name");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("name"));

        // Index should be gone from Table
        Table table = getTable("test", "t");
        assertNotNull(table);
        assertNull(table.getIndex("name"));
    }
    
    @Test
    public void nonUniqueVarchar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, name varchar(255)");
        createIndex("test", "t", "name", "name");
        writeRow(tId, 1, "bob");
        writeRow(tId, 2, "jim");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("name"));
        updateAISGeneration();

        AkibanInformationSchema aisCheck = ais();
        Index indexCheck = aisCheck.getTable(tId).getIndex("name");
        assertNull(indexCheck);
        assertEquals("number of indexes", 1, aisCheck.getTable(tId).getIndexes().size());
        List<Row> rows = scanAll(tId);
        assertEquals("rows from table scan", 2, rows.size());
    }
    
    @Test
    public void nonUniqueVarcharMiddleOfGroup() throws InvalidOperationException {
        int cId = createTable("coi", "c", "cid int not null primary key, name varchar(32)");
        int oId = createTable("coi", "o", "oid int not null primary key, c_id int, tag varchar(32), FOREIGN KEY (c_id) REFERENCES c(cid)");
        createIndex("coi", "o", "tag", "tag");
        int iId = createTable("coi", "i", "iid int not null primary key, o_id int, idesc varchar(32), GROUPING FOREIGN KEY (o_id) REFERENCES o(oid)");

        // One customer, two orders, 5 items
        writeRow(cId, 1, "bob");
        writeRow(oId, 1, 1, "supplies");
        writeRow(oId, 2, 1, "random");
        writeRow(iId, 1, 1, "foo");
        writeRow(iId, 2, 1, "bar");
        writeRow(iId, 3, 2, "zap");
        writeRow(iId, 4, 2, "fob");
        writeRow(iId, 5, 2, "baz");
        
        ddl().dropTableIndexes(session(), tableName(oId), Arrays.asList("tag"));
        updateAISGeneration();

        AkibanInformationSchema aisCheck = ais();
        Index indexCheck = aisCheck.getTable(oId).getIndex("tag");
        assertNull(indexCheck);
        assertEquals("number of indexes", 2, aisCheck.getTable(oId).getIndexes().size());
        assertNotNull(aisCheck.getTable(oId).getIndex("PRIMARY"));
        assertNotNull(aisCheck.getTable(oId).getIndex("o_fkey"));
        assertNull(aisCheck.getTable(oId).getIndex("tag"));
                
        List<Row> rows = scanAll(cId);
        assertEquals("customers from table scan", 1, rows.size());
        rows = scanAll(oId);
        assertEquals("orders from table scan", 2, rows.size());
        rows = scanAll(iId);
        assertEquals("items from table scan", 5, rows.size());
    }
    
    @Test
    public void nonUniqueCompoundVarcharVarchar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, \"first\" varchar(250), \"last\" varchar(250)");
        createIndex("test", "t", "name", "\"first\"", "\"last\"");
        writeRow(tId, 1, "foo", "bar");
        writeRow(tId, 2, "zap", "snap");
        writeRow(tId, 3, "baz", "fob");
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("name"));
        updateAISGeneration();

        AkibanInformationSchema aisCheck = ais();
        Index indexCheck = aisCheck.getTable(tId).getIndex("name");
        assertNull(indexCheck);
        assertEquals("number of indexes", 1, aisCheck.getTable(tId).getIndexes().size());
        assertNotNull(aisCheck.getTable(tId).getIndex("PRIMARY"));
        
        List<Row> rows = scanAll(tId);
        assertEquals("rows from table scan", 3, rows.size());
    }
    
    @Test
    public void uniqueChar() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, state char(2), unique(state)");
        writeRow(tId, 1, "IA");
        writeRow(tId, 2, "WA");
        writeRow(tId, 3, "MA");
        
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("state"));
        updateAISGeneration();

        AkibanInformationSchema aisCheck = ais();
        assertEquals("number of indexes", 1, aisCheck.getTable(tId).getIndexes().size());
        assertNull(aisCheck.getTable(tId).getIndex("state"));
        assertNotNull(aisCheck.getTable(tId).getIndex("PRIMARY"));
        List<Row> rows = scanAll(tId);
        assertEquals("rows from table scan", 3, rows.size());
    }
    
    @Test
    public void uniqueIntNonUniqueDecimal() throws InvalidOperationException {
        int tId = createTable("test", "t", "id int not null primary key, otherid int, price decimal(10,2), unique(otherid)");
        createIndex("test", "t", "price", "price");
        writeRow(tId, 1, 1337, "10.50");
        writeRow(tId, 2, 5000, "10.50");
        writeRow(tId, 3, 47000, "9.99");
        
        ddl().dropTableIndexes(session(), tableName(tId), Arrays.asList("otherid", "price"));
        updateAISGeneration();

        AkibanInformationSchema aisCheck = ais();
        assertEquals("number of indexes", 1, aisCheck.getTable(tId).getIndexes().size());
        assertNull(aisCheck.getTable(tId).getIndex("otherid"));
        assertNull(aisCheck.getTable(tId).getIndex("price"));
        assertNotNull(aisCheck.getTable(tId).getIndex("PRIMARY"));
        
        List<Row> rows = scanAll(tId);
        assertEquals("rows from table scan", 3, rows.size());
    }
}
