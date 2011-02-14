/*
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

package com.akiban.server.itests.d_lfunctions;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.ddl.UnsupportedDataTypeException;
import com.akiban.server.itests.ApiTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public final class CreateTableIT extends ApiTestBase {
    // TODO: ENUM/SET parsing and AIS support (unsupported for Halo)
    @Test
    public void bug686972() throws InvalidOperationException {
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 enum('a','b','c')");
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 ENUM('a','b','c')");
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 set('a','b','c')");
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 SET('a','b','c')");
        // Expand test when supporting ENUM and SEt
    }

    // Using 'text' as a column identifier causes parse error
    @Test
    public void bug687146() throws InvalidOperationException {
        int tid = createTable("test", "t", "entry int primary key, text varchar (2000)");
    }


    // Column or table comment causes parse error
    @Test
    public void bug687220() throws InvalidOperationException {
        createTable("test", "t1", "id int key COMMENT 'Column comment activate'");
        ddl().createTable(session, "test", "create table t2(id int key) COMMENT='A table comment'");
        tableId("test", "t2");
    }

    // Initial auto increment value is incorrect
    @Test
    public void bug696169() throws Exception {
        ddl().createTable(session, "test", "CREATE TABLE t(c1 INT AUTO_INCREMENT KEY) AUTO_INCREMENT=10");
        final int tid = tableId("test", "t");
        // This value gets sent as last_row_id so everything lines up on the adapter, where all auto_inc stuff is done
        assertEquals(9, store().getTableStatistics(session, tid).getAutoIncrementValue());
    }

    // FIXED data type causes parse error
    @Test
    public void bug696321() throws InvalidOperationException {
        createCheckColumnDrop("c1 FIXED NULL", Types.DECIMAL, 10L, 0L);
    }

    // REAL data type causes NPE
    @Test
    public void bug696325() throws InvalidOperationException {
        createCheckColumnDrop("c1 REAL(1,0) NULL", Types.DOUBLE, 1L, 0L);
    }

    // Short create statement causes StringIndexOutOfBoundsException from SchemaDef.canonicalStatement
    @Test
    public void bug705920() throws InvalidOperationException {
        createTable("test", "t", "c1 int"); // Bug case
        createTable("x", "y", "z int");     // As short as you could get
    }

    // TODO: BIT data type support (unsupported for Halo)
    @Test
    public void bug705980() throws InvalidOperationException {
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 bit(8)");
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 BIT(8)");
        // Expand test when supporting BIT (min, max, default type param, etc)
    }
    
    // CHAR(0) data type fails, assert during AIS construction
    @Test
    public void bug705993() throws InvalidOperationException {
        int tid = createCheckColumn("c1 CHAR(0) NULL", Types.CHAR, 0L, null);
        // We support a superset of the inserts for CHAR(0) compared to MySQL, which should be OK
        writeRows(createNewRow(tid, "a", -1L),
                  createNewRow(tid, null, -1L));
        expectFullRows(tid,
                       createNewRow(tid, "a"),
                       createNewRow(tid, (Object)null));
    }

    // SERIAL data types are parse errors
    @Test
    public void bug706008() throws InvalidOperationException {
        // SERIAL => BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
        int tid1 = createCheckColumn("c1 SERIAL", Types.U_BIGINT, null, null);
        Table table = getUserTable(tid1);
        assertFalse(table.getColumn("c1").getNullable());
        Index index = table.getIndex("c1");
        assertNotNull(index);
        assertTrue(index.isUnique());   

        // [int type] SERIAL DEFAULT VALUE => [int type] NOT NULL AUTO_INCREMENT UNIQUE.
        //int tid2 = createTable("test", "t", "c1 int SERIAL DEFAULT VALUE");
    }

    // CREATE TABLE .. LIKE .. is parse error
    @Test
    public void bug706344() throws InvalidOperationException {
        int tid1 = createTable("test", "src", "c1 INT NOT NULL AUTO_INCREMENT, c2 CHAR(10) NULL, PRIMARY KEY(c1)");
        ddl().createTable(session, "test", "CREATE TABLE dst LIKE src");
        int tid2 = tableId("test", "dst");
    }

    // CREATE TABLE .. SELECT .. FROM .. is parse error
    @Test
    public void bug706347() throws InvalidOperationException {
        int tid1 = createTable("test", "src", "c1 INT NOT NULL AUTO_INCREMENT, c2 INT NULL, PRIMARY KEY(c1))");
        ddl().createTable(session, "test", "c1 INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(c1)) SELECT c1,c2 FROM src");
        int tid2 = tableId("test", "dst");
    }

    // BLOB(L) always created as type blob
    @Test
    public void bug717842() throws InvalidOperationException {
        createCheckColumnDrop("c1 blob(0)", Types.BLOB, null, null);
        createCheckColumnDrop("c1 blob(1)", Types.TINYBLOB, null, null);
        createCheckColumnDrop("c1 blob(255)", Types.TINYBLOB, null, null);
        createCheckColumnDrop("c1 blob(256)", Types.BLOB, null, null);
        createCheckColumnDrop("c1 blob(65535)", Types.BLOB, null, null);
        createCheckColumnDrop("c1 blob(65536)", Types.MEDIUMBLOB, null, null);
        createCheckColumnDrop("c1 blob(16777215)", Types.MEDIUMBLOB, null, null);
        createCheckColumnDrop("c1 blob(16777216)", Types.LONGBLOB, null, null);
        createCheckColumnDrop("c1 blob(4294967295)", Types.LONGBLOB, null, null);
        
        // Text should be the same
        createCheckColumnDrop("c1 text(0)", Types.TEXT, null, null);
        createCheckColumnDrop("c1 text(1)", Types.TINYTEXT, null, null);
        createCheckColumnDrop("c1 text(255)", Types.TINYTEXT, null, null);
        createCheckColumnDrop("c1 text(256)", Types.TEXT, null, null);
        createCheckColumnDrop("c1 text(65535)", Types.TEXT, null, null);
        createCheckColumnDrop("c1 text(65536)", Types.MEDIUMTEXT, null, null);
        createCheckColumnDrop("c1 text(16777215)", Types.MEDIUMTEXT, null, null);
        createCheckColumnDrop("c1 text(16777216)", Types.LONGTEXT, null, null);
        createCheckColumnDrop("c1 text(4294967295)", Types.LONGTEXT, null, null);
    }
    
    @Test
    public void charTypeDefaultParameter() throws InvalidOperationException {
        int tid = createCheckColumn("c1 CHAR NULL", Types.CHAR, 1L, null);
        writeRows(createNewRow(tid, "a", -1L));
        expectFullRows(tid, createNewRow(tid, "a"));
    }

    @Test
    public void charTypeWithParameter() throws InvalidOperationException {
        int tid = createCheckColumn("c1 CHAR(5) NULL", Types.CHAR, 5L, null);
        writeRows(createNewRow(tid, "a", -1L),
                  createNewRow(tid, "abc", -1L),
                  createNewRow(tid, "xxxxx", -1L));
        expectFullRows(tid,
                       createNewRow(tid, "a"),
                       createNewRow(tid, "abc"),
                       createNewRow(tid, "xxxxx"));
    }

    @Test
    public void floatSingleParamConversions() throws InvalidOperationException {
        createCheckColumnDrop("c1 float(0)", Types.FLOAT, null, null);
        createCheckColumnDrop("c1 float(24)", Types.FLOAT, null, null);
        createCheckColumnDrop("c1 float(25)", Types.DOUBLE, null, null);
        createCheckColumnDrop("c1 float(53)", Types.DOUBLE, null, null);
    }

    @Test
    public void realTypeDefaultParam() throws InvalidOperationException {
        createCheckColumn("c1 REAL NULL", Types.DOUBLE, null, null);
    }

    
    private void createExpectException(Class c, String schema, String table, String definition) {
        try {
            createTable(schema, table, definition);
            Assert.fail("Expected exception " + c.getName());
        }
        catch(Throwable t) {
            assertEquals(c, t.getClass());
        }
    }

    private int createCheckColumn(String columnDecl, Type type, Long typeParam1, Long typeParam2)
            throws InvalidOperationException {
        final int tid = createTable("test", "t", columnDecl);
        final Table table = getUserTable(tid);
        final Collection<Column> columns = table.getColumns();
        assertEquals(1, columns.size());
        final Column column = columns.iterator().next();
        assertEquals(type, column.getType());
        assertEquals(typeParam1, column.getTypeParameter1());
        assertEquals(typeParam2, column.getTypeParameter2());
        return tid;
    }

    private void createCheckColumnDrop(String columnDecl, Type type, Long typeParam1, Long typeParam2)
            throws InvalidOperationException {
        int tid = createCheckColumn(columnDecl, type, typeParam1, typeParam2);
        ddl().dropTable(session, tableName(tid));
    }
}

