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

package com.foundationdb.sql.embedded;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.*;

import java.math.BigDecimal;

public class EmbeddedJDBCIT extends EmbeddedJDBCITBase
{

    @Before
    public void loadDB() throws Exception {
        int cid = createTable(SCHEMA_NAME, "c",
                              "cid int primary key not null",
                              "name varchar(16) not null");
        int oid = createTable(SCHEMA_NAME, "o",
                              "oid int primary key not null",
                              "cid int not null",
                              "grouping foreign key(cid) references c(cid)",
                              "order_date date not null");
        writeRow(cid, 1, "Smith");
        writeRow(oid, 101, 1, 2012 * 512 + 1 * 32 + 31);
        writeRow(oid, 102, 1, 2012 * 512 + 2 * 32 + 1);
        writeRow(cid, 2, "Jones");
        writeRow(oid, 201, 2, 2012 * 512 + 4 * 32 + 1);
    }
    
    @Test
    public void testSimple() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM c WHERE cid = 1")) {
            assertTrue("has first row", rs.next());
            assertEquals("result value", "Smith", rs.getString(1));
            assertFalse("has more rows", rs.next());
        }
    }

    @Test
    public void testPrepared() throws Exception {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT name, order_date FROM c INNER JOIN o USING(cid) WHERE c.cid = ?")) {
            assertEquals("estimated count", 2, ((JDBCPreparedStatement)stmt).getEstimatedRowCount());
            stmt.setInt(1, 2);
            assertTrue("has result set", stmt.execute());
            try (ResultSet rs = stmt.getResultSet()) {
                assertTrue("has first row", rs.next());
                assertEquals("result value", "Jones", rs.getString(1));
                assertEquals("result value", "2012-04-01", rs.getDate(2).toString());
                assertFalse("has more rows", rs.next());
            }
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue("has first row", rs.next());
                assertEquals("result value", "Smith", rs.getString(1));
                assertEquals("result value", "2012-01-31", rs.getString(2));
                assertTrue("has next row", rs.next());
                assertEquals("result value", "Smith", rs.getString(1));
                assertEquals("result value", "2012-02-01 00:00:00.0", rs.getTimestamp(2).toString());
                assertFalse("has more rows", rs.next());
            }
        }
    }

    @Test
    public void testPreparedBinary() throws Exception {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT name FROM c WHERE AES_ENCRYPT(name, 'key') = ?")) {
            stmt.setBytes(1, new byte[] { 1, 2, 3 });
            assertTrue("has result set", stmt.execute());
            try (ResultSet rs = stmt.getResultSet()) {
                assertFalse("doesn't have first row", rs.next());
            }
        }
    }

    @Test
    public void testNested() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name, (SELECT oid,order_date FROM o WHERE o.cid = c.cid) FROM c WHERE cid = 1")) {
            assertTrue("has first row", rs.next());
            assertEquals("result value", "Smith", rs.getString(1));
            try (ResultSet nrs = (ResultSet)rs.getObject(2)) {
                assertTrue("nested first row", nrs.next());
                assertEquals("result value", 101, nrs.getInt(1));
                assertEquals("result value", "2012-01-31", nrs.getString(2).toString());
                assertTrue("nested second row", nrs.next());
                assertEquals("result value", 102, nrs.getInt(1));
                assertEquals("result value", "2012-02-01", nrs.getString(2).toString());
                assertFalse("nested third row", nrs.next());
            }
            assertFalse("has more rows", rs.next());
        }
    }

    @Test
    public void testUpdate() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE t1(id INT PRIMARY KEY NOT NULL GENERATED BY DEFAULT AS IDENTITY, s VARCHAR(16))");
            int count = stmt.executeUpdate("INSERT INTO t1(s) VALUES('foo'), ('bar')", Statement.RETURN_GENERATED_KEYS);
            assertEquals("update count", 2, count);
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                assertTrue("first generated keys", rs.next());
                assertEquals("first generated key", 1, rs.getInt(1));
                assertTrue("second generated keys", rs.next());
                assertEquals("second generated key", 2, rs.getInt(1));
                assertFalse("third generated keys", rs.next());
            }
            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE t1 SET s = 'boo' WHERE id = ?")) {
                pstmt.setInt(1, 1);
                assertFalse("has results", pstmt.execute());
                assertEquals("updated count", 1, pstmt.getUpdateCount());
            }
        }
    }
    @Test 
    public void testJavaProcedure() throws Exception {
        try (Connection conn = getConnection()) {
            try (CallableStatement cstmt0 = conn.prepareCall("CALL sqlj.install_jar(?, 'sql_layer', 0)")) {
                String jarName = getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
                cstmt0.setString(1, jarName);
                cstmt0.execute();
            }
            try (Statement stmt2 = conn.createStatement()) {
                stmt2.execute("CREATE PROCEDURE add_sub(IN x INT, IN y INT, OUT \"sum\" INT, out diff INT) LANGUAGE java PARAMETER STYLE java EXTERNAL NAME 'sql_layer:com.foundationdb.server.test.it.routines.TestJavaBasic.addSub'");
            }
            try (CallableStatement cstmt = conn.prepareCall("CALL add_sub(100,?,?,?)")) {
                cstmt.setInt(1, 23);
                assertFalse("call returned results", cstmt.execute());
                assertEquals("sum result", 123, cstmt.getInt("sum"));
                assertEquals("diff results", 77, cstmt.getObject("diff"));
            }
        }
    }

    @Test
    public void testScriptProcedure() throws Exception {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE PROCEDURE concat_n(IN s VARCHAR(128), IN x INT, OUT ns VARCHAR(128)) LANGUAGE javascript PARAMETER STYLE variables AS 's+x'");
            }
            try (CallableStatement cstmt = conn.prepareCall("CALL concat_n(?,?,?)")) {
                cstmt.setString(1, "Hello ");
                cstmt.setInt(2, 123);
                assertFalse("call returned results", cstmt.execute());
                assertEquals("script results", "Hello 123", cstmt.getString(3));
            }
        }
    }

    @Test
    public void testScriptProcedureTypes() throws Exception {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE PROCEDURE add_sub(IN x DOUBLE, IN y INT, OUT \"sum\" DOUBLE, OUT diff INT) LANGUAGE javascript PARAMETER STYLE variables AS $$ sum = x+y; diff = x-y $$");
            }
            try (CallableStatement cstmt = conn.prepareCall("CALL add_sub(?,?,?,?)")) {
                cstmt.setInt(1, 1);
                cstmt.setInt(2, 2);
                assertFalse("call returned results", cstmt.execute());
                assertFalse("call returned results", cstmt.execute());
                assertEquals("sum result", 3, cstmt.getInt("sum"));
                assertEquals("diff results", -1, cstmt.getInt("diff"));
            }
        }
    }

    @Test
    public void testScriptJDBC() throws Exception {
        String defn = String.format(
    "CREATE PROCEDURE get_co(IN cid int) LANGUAGE javascript PARAMETER STYLE variables RESULT SETS 2 AS $$\n" +
    "var conn = java.sql.DriverManager.getConnection(\"%s\", \"%s\", \"\");\n" +
    "var ps1 = conn.prepareStatement(\"SELECT name FROM c WHERE cid = ?\");\n" +
    "var ps2 = conn.prepareStatement(\"SELECT order_date FROM o WHERE cid = ?\");\n" +
    "ps1.setInt(1, cid); ps2.setInt(1, cid);\n" +
    "[ ps1.executeQuery(), ps2.executeQuery() ]" +
    "$$", CONNECTION_URL, SCHEMA_NAME);
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(defn);
            }
            try (CallableStatement cstmt = conn.prepareCall("CALL get_co(?)")) {
                cstmt.setInt(1, 2);
                assertTrue("call returned results", cstmt.execute());
                try (ResultSet rs = cstmt.getResultSet()) {
                    assertTrue("script results 1", rs.next());
                    assertEquals("script results 1 value", "Jones", rs.getString(1));
                    assertFalse("script results 1 more", rs.next());
                }
                assertTrue("call returned more results", cstmt.getMoreResults());
                try (ResultSet rs = cstmt.getResultSet()) {
                    assertTrue("script results 2", rs.next());
                    assertEquals("script results 2 value", "2012-04-01", rs.getDate(1).toString());
                    assertFalse("script results 2 more", rs.next());
                }
                assertFalse("call returned more results", cstmt.getMoreResults());
            }
        }
    }

    @Test
    public void testMoreTypes() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT CURRENT_DATE, 3.14, 1.0e6")) {
            assertTrue("has first row", rs.next());
            assertEquals("date value", new Date(System.currentTimeMillis()).toString(), rs.getDate(1).toString());
            assertEquals("decimal value", new BigDecimal("3.14"), rs.getBigDecimal(2));
            assertEquals("double value", 1.0e6, rs.getDouble(3), 0);
            assertFalse("has more rows", rs.next());
        }
    }

    @Test
    public void prepareUsingObjects() throws Exception {
        createTable("schm", "t", "i int, j bigint, d double, s varchar(16), b boolean, n decimal(8,3)");

        try (Connection connection = getConnection()) {
            final String insert = "INSERT INTO schm.t(i, j, d, s, b, n) VALUES(?, ?, ?, ?, ?, ?)";
            try (PreparedStatement s = connection.prepareStatement(insert)) {
                s.setInt(1, 111);
                s.setLong(2, 12345);
                s.setDouble(3, 3.14159265);
                s.setString(4, "hello");
                s.setBoolean(5, true);
                s.setBigDecimal(6, new BigDecimal("9876.543"));
                s.execute();
            }
            try (Statement s = connection.createStatement()) {
                ResultSet rs = s.executeQuery("SELECT * FROM schm.t");
                assertTrue("no rs rows", rs.next());
                assertEquals("row[1]", 111, rs.getInt(1));
                assertEquals("row[2]", 12345, rs.getLong(2));
                assertEquals("row[3]", 3.14159265, rs.getDouble(3), 0.01);
                assertEquals("row[4]", "hello", rs.getString(4));
                assertEquals("row[5]", true, rs.getBoolean(5));
                assertEquals("row[6]", new BigDecimal("9876.543"), rs.getBigDecimal(6));
                assertFalse("too many rs rows", rs.next());
            }
        }
    }

}
