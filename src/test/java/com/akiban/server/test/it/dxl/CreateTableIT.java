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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.message.ErrorCode;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.ddl.DuplicateTableNameException;
import com.akiban.server.api.ddl.JoinToMultipleParentsException;
import com.akiban.server.api.ddl.JoinToWrongColumnsException;
import com.akiban.server.api.ddl.ParseException;
import com.akiban.server.api.ddl.UnsupportedDataTypeException;
import com.akiban.server.api.ddl.UnsupportedIndexDataTypeException;
import com.akiban.server.api.ddl.UnsupportedIndexSizeException;
import com.akiban.server.test.it.ITBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public final class CreateTableIT extends ITBase {
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
        writeRows(createNewRow(tid, 1, "foo"),
                  createNewRow(tid, 2, "bar"));
        expectFullRows(tid,
                       createNewRow(tid, 1L, "foo"),
                       createNewRow(tid, 2L, "bar"));
    }


    // Column or table comment causes parse error
    @Test
    public void bug687220() throws InvalidOperationException {
        createTable("test", "t1", "id int key COMMENT 'Column comment activate'");
        ddl().createTable(session(), "test", "create table t2(id int key) COMMENT='A table comment'");
        tableId("test", "t2");
    }

    // Initial auto increment value is incorrect
    @Test
    public void bug696169() throws Exception {
        ddl().createTable(session(), "test", "CREATE TABLE t(c1 INT AUTO_INCREMENT KEY) AUTO_INCREMENT=10");
        final int tid = tableId("test", "t");
        // This value gets sent as last_row_id so everything lines up on the adapter, where all auto_inc stuff is done
        assertEquals(9, store().getTableStatistics(session(), tid).getAutoIncrementValue());
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
        writeRows(createNewRow(tid, "", -1L),
                  createNewRow(tid, null, -1L));
        expectFullRows(tid,
                       createNewRow(tid, ""),
                       createNewRow(tid, (Object)null));
    }

    // SERIAL data types are parse errors
    @Test
    public void bug706008() throws InvalidOperationException {
        // SERIAL => BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
        final int tid1 = createCheckColumn("c1 SERIAL", Types.U_BIGINT, null, null);
        final Table table1 = getUserTable(tid1);
        assertFalse(table1.getColumn("c1").getNullable());
        assertNotNull(table1.getColumn("c1").getInitialAutoIncrementValue());
        assertTrue(table1.getIndex("c1").isUnique());
        ddl().dropTable(session(), tableName(tid1));

        // [int type] SERIAL DEFAULT VALUE => [int type] NOT NULL AUTO_INCREMENT UNIQUE.
        final int tid2 = createCheckColumn("c1 tinyint SERIAL DEFAULT VALUE", Types.TINYINT, null, null);
        final Table table2 = getUserTable(tid2);
        assertFalse(table2.getColumn("c1").getNullable());
        assertNotNull(table2.getColumn("c1").getInitialAutoIncrementValue());
        assertTrue(table2.getIndex("c1").isUnique());
        dropAllTables();

        createCheckColumnDrop("c1 smallint SERIAL DEFAULT VALUE", Types.SMALLINT, null, null);
        createCheckColumnDrop("c1 int SERIAL DEFAULT VALUE", Types.INT, null, null);
        createCheckColumnDrop("c1 mediumint SERIAL DEFAULT VALUE", Types.MEDIUMINT, null, null);
        createCheckColumnDrop("c1 bigint SERIAL DEFAULT VALUE", Types.BIGINT, null, null);
    }

    // CREATE TABLE .. LIKE .. is parse error
    @Test
    public void bug706344() throws InvalidOperationException {
        createTable("test", "src", "c1 INT NOT NULL AUTO_INCREMENT, c2 CHAR(10) NULL, PRIMARY KEY(c1)");
        ddl().createTable(session(), "test", "CREATE TABLE dst LIKE src");
        final int tid = tableId("test", "dst");
        final Table table = getUserTable(tid);
        assertEquals(table.getColumns().size(), 2);
        final Column c1 = table.getColumn("c1");
        assertNotNull(c1);
        assertEquals(c1.getType(), Types.INT);
        assertNull(c1.getTypeParameter1());
        assertNull(c1.getTypeParameter2());
        assertFalse(c1.getNullable());
        assertEquals(0L, c1.getInitialAutoIncrementValue().longValue());
        final Column c2 = table.getColumn("c2");
        assertNotNull(c2);
        assertEquals(c2.getType(), Types.CHAR);
        assertEquals(c2.getTypeParameter1().longValue(), 10L);
        assertNull(c2.getTypeParameter2());
        assertTrue(c2.getNullable());
        assertNull(c2.getInitialAutoIncrementValue());
        assertEquals(table.getIndexes().size(), 1);
        final Index index = table.getIndex("PRIMARY");
        assertNotNull(index);
        assertEquals(1, index.getColumns().size());

        dropAllTables();

        // Different schemas
        final int origTid = createTable("schema1", "orig", "c1 int key");
        ddl().createTable(session(), "schema2", "create table copy like schema1.orig");
        assertEquals(origTid, tableId("schema1", "orig"));
        tableId("schema2", "copy");

        try {
            ddl().createTable(session(), "foo", "create table atable like orig");
            Assert.fail("Expected InvalidOperationException exception");
        } catch(InvalidOperationException e) {
            assertEquals(ErrorCode.NO_SUCH_TABLE, e.getCode());
        }
    }

    // CREATE TABLE .. SELECT .. FROM .. is parse error
    // Note: Fixing would require parsing/computing the result column set of any valid SELECT statement
    @Test(expected=ParseException.class)
    public void bug706347() throws InvalidOperationException {
        int tid1 = createTable("test", "src", "c1 INT NOT NULL AUTO_INCREMENT, c2 INT NULL, PRIMARY KEY(c1))");
        ddl().createTable(session(), "test", "create table dst(c1 INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(c1)) SELECT c1,c2 FROM src");
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

    @Test
    public void createStatementsWithComments() throws InvalidOperationException {
        // Failed on second one with NPE in refreshSchema, found in mtr/engine/funcs/rpl_trigger
        ddl().createTable(session(), "test", "create table t210 (f1 int, f2 int) /* slave local */");
        tableName("test", "t210");
        ddl().createTable(session(), "test", "create table t310 (f3 int) /* slave local */");
        tableName("test", "t310");

        // Long comment (with and without embedded newlines)
        ddl().createTable(session(), "test", "create table t1(id int key /*pkey*/, name varchar(32) /* fname \n with two line comment*/) engine=akibandb");
        assertEquals(2, getUserTable(tableId("test","t1")).getColumns().size());

        //TODO: FIX! Disabled since newlines MUST be stripped from DDL due to refresh/schemectomy interface
        /*
        // Line comments (only with trailing newlines, parses cleanly)
        ddl().createTable(session, "test", "create table t2(id int key, -- pkey \nname varchar(32)\n) engine=akibandb -- after comment\n");
        assertEquals(2, getUserTable(tableId("test","t2")).getColumns().size());

        // Line comment (no traling newline, parse warning due to no EOL but should still succeed)
        ddl().createTable(session, "test", "create table t3(id int key) engine=akibandb -- eolcomment");
        assertEquals(1, getUserTable(tableId("test","t3")).getColumns().size());
        */
        // Confirm all still there
        assertEquals(3, getUserTables().size());
    }

    @Test
    public void boolTypeAliases() throws InvalidOperationException {
        createCheckColumnDrop("c1 bool", Types.TINYINT, null, null);
        createCheckColumnDrop("c1 boolean", Types.TINYINT, null, null);
    }

    @Test
    public void nationalCharTypeAliases() throws InvalidOperationException {
        int tid = createCheckColumn("c1 nchar(2)", Types.CHAR, 2L, null);
        assertEquals("utf8", getUserTable(tid).getColumn("c1").getCharsetAndCollation().charset());
        ddl().dropTable(session(), tableName(tid));

        tid = createCheckColumn("c1 national char(5)", Types.CHAR, 5L, null);
        assertEquals("utf8", getUserTable(tid).getColumn("c1").getCharsetAndCollation().charset());
        ddl().dropTable(session(), tableName(tid));

        tid = createCheckColumn("c1 nvarchar(32)", Types.VARCHAR, 32L, null);
        assertEquals("utf8", getUserTable(tid).getColumn("c1").getCharsetAndCollation().charset());
        ddl().dropTable(session(), tableName(tid));

        tid = createCheckColumn("c1 national varchar(255)", Types.VARCHAR, 255L, null);
        assertEquals("utf8", getUserTable(tid).getColumn("c1").getCharsetAndCollation().charset());
        ddl().dropTable(session(), tableName(tid));

        tid = createCheckColumn("c1 national char varying(255)", Types.VARCHAR, 255L, null);
        assertEquals("utf8", getUserTable(tid).getColumn("c1").getCharsetAndCollation().charset());
        ddl().dropTable(session(), tableName(tid));

        tid = createCheckColumn("c1 national character varying(255)", Types.VARCHAR, 255L, null);
        assertEquals("utf8", getUserTable(tid).getColumn("c1").getCharsetAndCollation().charset());
        ddl().dropTable(session(), tableName(tid));
    }

    @Test
    public void spatialDataTypes() throws InvalidOperationException {
        createExpectException(UnsupportedDataTypeException.class, "test", "t1", "c1 geometry");
        createExpectException(UnsupportedDataTypeException.class, "test", "t2", "c1 geometrycollection");
        createExpectException(UnsupportedDataTypeException.class, "test", "t3", "c1 point");
        createExpectException(UnsupportedDataTypeException.class, "test", "t4", "c1 multipoint ");
        createExpectException(UnsupportedDataTypeException.class, "test", "t5", "c1 linestring");
        createExpectException(UnsupportedDataTypeException.class, "test", "t6", "c1 multilinestring");
        createExpectException(UnsupportedDataTypeException.class, "test", "t7", "c1 polygon");
        createExpectException(UnsupportedDataTypeException.class, "test", "t8", "c1 multipolygon");
    }

    // DOUBLE PRECISION alias
    @Test
    public void bug724021() throws InvalidOperationException {
        createCheckColumnDrop("c1 DOUBLE PRECISION", Types.DOUBLE, null, null);
        createCheckColumnDrop("c1 DOUBLE PRECISION(10,5)", Types.DOUBLE, 10L, 5L);
        createCheckColumnDrop("c1 DOUBLE PRECISION(1,0) NOT NULL", Types.DOUBLE, 1L, 0L);
    }

    // default charset on table results in invalid DDL regeneration
    @Test
    public void bug725100() throws InvalidOperationException {
        ddl().createTable(session(), "test", "create table t(id int key) default charset=utf8");
        final int tid = tableId("test", "t");
        assertEquals("create table `test`.`t`(`id` int, PRIMARY KEY(`id`)) engine=akibandb DEFAULT CHARSET=utf8",
                     new DDLGenerator().createTable(getUserTable(tid)));
    }

    // Akiban foreign key to non-primary key column is allowed
    @Test(expected=JoinToWrongColumnsException.class)
    public void bug727749() throws InvalidOperationException {
        createTable("test", "p", "id int key, wrongInt int");
        createTable("test", "c", "id int key, pid int, constraint __akiban foreign key(pid) references p(wrongInt)");
    }

    @Test
    public void joinMustMatchParentPK() throws InvalidOperationException {
        createTable("test", "p1", "id1 int, id2 int, primary key(id1,id2)");
        // subset of pk
        createExpectException(JoinToWrongColumnsException.class, "test", "c",
                              "id int key, pid1 int, pid2 int, constraint __akiban foreign key(pid1) references p1(id1)");
        // join key missing column
        createExpectException(JoinToWrongColumnsException.class, "test", "c",
                              "id int key, pid1 int, pid2 int, constraint __akiban foreign key(pid1) references p1(id1,id2)");
        // different order in table reference
        createExpectException(JoinToWrongColumnsException.class, "test", "c",
                              "id int key, pid1 int, pid2 int, constraint __akiban foreign key(pid1,pid2) references p1(id2,id1)");
        // wrong column name in join key
        createExpectException(JoinToWrongColumnsException.class, "test", "c",
                              "id int key, pid1 int, pid2 int, constraint __akiban foreign key(pid,pid2) references p1(id1,id2)");
        // should be case insensitive
        createTable("test", "c",
                    "id int key, pid1 int, pid2 int, constraint __akiban foreign key(pid1,pid2) references p1(ID1,iD2)");
    }

    // Table with two akiban foreign keys is reported poorly
    @Test(expected=JoinToMultipleParentsException.class)
    public void bug727754() throws InvalidOperationException {
        createTable("test", "p1", "id int key");
        createTable("test", "p2", "id int key");
        createTable("test", "c", "id int key, p1id int, constraint __akiban1 foreign key(p1id) references p1(id),"+
                                             "p2id int, constraint __akiban2 foreign key(p2id) references p2(id)");
    }

    // Akiban foreign key with differing parent/child columns
    @Test(expected= JoinToWrongColumnsException.class)
    public void bug728003() throws InvalidOperationException {
        createTable("test", "p", "id varchar(32) key");
        createTable("test", "c", "id int key, pid int, constraint __akiban foreign key(pid) references p(id)");
    }

    @Test
    public void unsupportedIndexTypes() throws InvalidOperationException {
        // bug716126/716126
        createExpectException(UnsupportedIndexDataTypeException.class, "test", "t", "c1 float key");
        createExpectException(UnsupportedIndexDataTypeException.class, "test", "t", "c1 double key");
        // bug737692
        createExpectException(UnsupportedIndexDataTypeException.class, "test", "t", "c1 blob, key(c1(100)))");
        createExpectException(UnsupportedIndexDataTypeException.class, "test", "t", "c1 text, key(c1(100)))");
    }

    @Test
    public void createDuplicateTable() throws InvalidOperationException {
        // bug705543
        createTable("test", "t", "c1 int key");
        createExpectException(DuplicateTableNameException.class, "test", "t", "c1 int key");
        createExpectException(DuplicateTableNameException.class, "test", "t", "c1 bigint key");
        createExpectException(DuplicateTableNameException.class, "test", "t", "c1 varchar(32) key");
        createExpectException(DuplicateTableNameException.class, "test", "t", "c1 int key, c2 varchar(32)");
    }

    @Test
    public void pkeyTooLarge() throws InvalidOperationException {
        // bug713387
        createExpectException(UnsupportedIndexSizeException.class, "test", "t", "id varchar(2050) key");
    }

    @Test
    public void bug760202() throws InvalidOperationException {
        // Prefixes not supported, expect rejection until they are
        createExpectException(UnsupportedIndexSizeException.class, "test", "t", "v varchar(10), unique index(v(3))");
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
        ddl().dropTable(session(), tableName(tid));
    }
}

