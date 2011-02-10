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

package com.akiban.cserver.itests.d_lfunctions;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Types;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.ddl.UnsupportedDataTypeException;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public final class CreateTableIT extends ApiTestBase {
    void createExpectException(Class c, String schema, String table, String definition) {
        try {
            createTable(schema, table, definition);
            Assert.fail("Expected exception " + c.getName());
        }
        catch(Throwable t) {
            assertEquals(c, t.getClass());
        }
    }
    

    /*
     * TODO: ENUM parsing and AIS support (unsupported for Halo)
     */
    @Test
    public void bug686972_enum() throws InvalidOperationException {
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 enum('a','b','c')");
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 ENUM('a','b','c')");

        // Expand test when supporting ENUM
    }

    /*
     * TODO: SET parsing and AIS support (unsupported for Halo)
     */
    @Test
    public void bug686972_set() throws InvalidOperationException {
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 set('a','b','c')");
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 SET('a','b','c')");
        
        // Expand test when supporting SET
    }

    /*
     * Using 'text' as a column identifier causes parse error
     */
    @Test
    public void bug687146() throws InvalidOperationException {
        int tid = createTable("test", "t", "entry int primary key, text varchar (2000)");
    }


    /*
     * Column or table comment causes parse error
     */
    @Test
    public void bug687220() throws InvalidOperationException {
        int tid1 = createTable("test", "t1", "id int key COMMENT 'Column comment activate'");

        ddl().createTable(session, "test", "create table t2(id int key) COMMENT='A table comment'");
        int tid2 = tableId("test", "t2");
    }

    /*
     * Initial auto increment value is incorrect
     */
    @Test
    public void bug696169() throws Exception {
        ddl().createTable(session, "test", "CREATE TABLE t(c1 INT AUTO_INCREMENT KEY) AUTO_INCREMENT=10");
        int tid = tableId("test", "t");
        
        assertEquals("auto inc is n-1",
                     9,
                     store().getTableStatistics(session, tid).getAutoIncrementValue());
    }

    /*
     * FIXED data type causes parse error
     */
    @Test
    public void bug696321_defaultParams() throws InvalidOperationException {
        final int tid = createTable("test", "t", "c1 FIXED NULL");
        final Table table = getUserTable(tid);
        final Collection<Column> columns = table.getColumns();
        assertEquals(1, columns.size());
        final Column col = columns.iterator().next();
        assertEquals(Types.DECIMAL, col.getType());
        assertEquals(10, col.getTypeParameter1().intValue());
        assertEquals(0, col.getTypeParameter2().intValue());
        ddl().dropTable(session, table.getName());
    }

    @Test
    public void bug696321_specifiedParams() throws InvalidOperationException {
        final int tid = createTable("test", "t", "c1 FIXED(23,5)");
        final Table table = getUserTable(tid);
        final Collection<Column> columns = table.getColumns();
        assertEquals(1, columns.size());
        final Column col = columns.iterator().next();
        assertEquals(Types.DECIMAL, col.getType());
        assertEquals(23, col.getTypeParameter1().intValue());
        assertEquals(5, col.getTypeParameter2().intValue());
        ddl().dropTable(session, table.getName());
    }

    /*
     * REAL data type causes NPE
     */
    @Test
    public void bug696325_defaultParams() throws InvalidOperationException {
        final int tid = createTable("test", "t", "c1 REAL NULL");
        final Table table = getUserTable(tid);
        final Collection<Column> columns = table.getColumns();
        assertEquals(1, columns.size());
        final Column col = columns.iterator().next();
        assertEquals(Types.DOUBLE, col.getType());
        assertNull(col.getTypeParameter1());
        assertNull(col.getTypeParameter2());
        ddl().dropTable(session, table.getName());
    }

    @Test
    public void bug696325_specifiedParams() throws InvalidOperationException {
        final int tid = createTable("test", "t", "c1 REAL(1,0) NULL");
        final Table table = getUserTable(tid);
        final Collection<Column> columns = table.getColumns();
        assertEquals(1, columns.size());
        final Column col = columns.iterator().next();
        assertEquals(Types.DOUBLE, col.getType());
        assertEquals(1, col.getTypeParameter1().intValue());
        assertEquals(0, col.getTypeParameter2().intValue());
        ddl().dropTable(session, table.getName());
    }

    /*
     * Short create statement causes StringIndexOutOfBoundsException from SchemaDef.canonicalStatement
     */
    @Test
    public void bug705920() throws InvalidOperationException {
        int tid = createTable("test", "t", "c1 int");
    }

    /*
     * TODO: BIT data type support (unsupported for Halo)
     */
    @Test
    public void bug705980() throws InvalidOperationException {
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 bit(8)");
        createExpectException(UnsupportedDataTypeException.class, "test", "t", "c1 BIT(8)");
        
        // Expand test when supporting BIT (min, max, default type param, etc)
    }
    
    /*
     * CHAR(0) data type fails, assert during AIS construction
     */
    @Test
    public void bug705993() throws InvalidOperationException {
        int tid = createTable("test", "t", "c1 CHAR(0) NULL");
    }

    /*
     * SERIAL data types are parse errors
     */
    @Test
    public void bug706008() throws InvalidOperationException {
        // SERIAL => BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
        int tid1 = createTable("test", "t", "c1 SERIAL");

        // [int type] SERIAL DEFAULT VALUE => [int type] NOT NULL AUTO_INCREMENT UNIQUE.
        int tid2 = createTable("test", "t", "c1 int SERIAL DEFAULT VALUE");
    }

    /*
     * CREATE TABLE .. LIKE .. is parse error
     */
    @Test
    public void bug706344() throws InvalidOperationException {
        int tid1 = createTable("test", "src", "c1 INT NOT NULL AUTO_INCREMENT, c2 CHAR(10) NULL, PRIMARY KEY(c1)");
        ddl().createTable(session, "test", "CREATE TABLE dst LIKE src");
        int tid2 = tableId("test", "dst");
    }

    /*
     * CREATE TABLE .. SELECT .. FROM .. is parse error
     */
    @Test
    public void bug706347() throws InvalidOperationException {
        int tid1 = createTable("test", "src", "c1 INT NOT NULL AUTO_INCREMENT, c2 INT NULL, PRIMARY KEY(c1))");
        ddl().createTable(session, "test", "c1 INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(c1)) SELECT c1,c2 FROM src");
        int tid2 = tableId("test", "dst");
    }
}
