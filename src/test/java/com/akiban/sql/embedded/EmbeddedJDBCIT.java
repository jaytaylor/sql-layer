/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.embedded;

import com.akiban.server.test.it.ITBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.sql.*;

import com.akiban.server.service.config.Property;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import java.util.Collection;

public class EmbeddedJDBCIT extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        // JDBC service is not in test-services.
        return super.serviceBindingsProvider()
            .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class);
    }

    @Override
    protected Collection<Property> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    public static final String SCHEMA_NAME = "test";
    public static final String CONNECTION_URL = "jdbc:default:connection";
    
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
        Connection conn = DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT name FROM c WHERE cid = 1");
        assertTrue("has first row", rs.next());
        assertEquals("result value", "Smith", rs.getString(1));
        assertFalse("has more rows", rs.next());
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void testPrepared() throws Exception {
        Connection conn = DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
        PreparedStatement stmt = conn.prepareStatement("SELECT name, order_date FROM c INNER JOIN o USING(cid) WHERE c.cid = ?");
        stmt.setInt(1, 2);
        assertTrue("has result set", stmt.execute());
        ResultSet rs = stmt.getResultSet();
        assertTrue("has first row", rs.next());
        assertEquals("result value", "Jones", rs.getString(1));
        assertEquals("result value", "2012-04-01", rs.getDate(2).toString());
        assertFalse("has more rows", rs.next());
        rs.close();
        stmt.setInt(1, 1);
        rs = stmt.executeQuery();
        assertTrue("has first row", rs.next());
        assertEquals("result value", "Smith", rs.getString(1));
        assertEquals("result value", "2012-01-31", rs.getString(2));
        assertTrue("has next row", rs.next());
        assertEquals("result value", "Smith", rs.getString(1));
        assertEquals("result value", "2012-02-01 00:00:00.0", rs.getTimestamp(2).toString());
        assertFalse("has more rows", rs.next());
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void testNested() throws Exception {
        Connection conn = DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT name, (SELECT oid,order_date FROM o WHERE o.cid = c.cid) FROM c WHERE cid = 1");
        assertTrue("has first row", rs.next());
        assertEquals("result value", "Smith", rs.getString(1));
        ResultSet nrs = (ResultSet)rs.getObject(2);
        assertTrue("nested first row", nrs.next());
        assertEquals("result value", 101, nrs.getInt(1));
        assertEquals("result value", "2012-01-31", nrs.getString(2).toString());
        assertTrue("nested second row", nrs.next());
        assertEquals("result value", 102, nrs.getInt(1));
        assertEquals("result value", "2012-02-01", nrs.getString(2).toString());
        assertFalse("nested third row", nrs.next());
        nrs.close();
        assertFalse("has more rows", rs.next());
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void testUpdate() throws Exception {
        Connection conn = DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1(id INT PRIMARY KEY NOT NULL GENERATED BY DEFAULT AS IDENTITY, s VARCHAR(16))");
        int count = stmt.executeUpdate("INSERT INTO t1(s) VALUES('foo'), ('bar')", Statement.RETURN_GENERATED_KEYS);
        assertEquals("update count", 2, count);
        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue("first generated keys", rs.next());
        assertEquals("first generated key", 1, rs.getInt(1));
        assertTrue("second generated keys", rs.next());
        assertEquals("second generated key", 2, rs.getInt(1));
        assertFalse("third generated keys", rs.next());
        rs.close();
        stmt.close();
        PreparedStatement pstmt = conn.prepareStatement("UPDATE t1 SET s = 'boo' WHERE id = ?");
        pstmt.setInt(1, 1);
        assertFalse("has results", pstmt.execute());
        assertEquals("updated count", 1, pstmt.getUpdateCount());
        pstmt.close();
        conn.close();
    }

    @Test
    public void testJavaProcedure() throws Exception {
        Connection conn = DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE PROCEDURE add_sub(IN x INT, IN y INT, OUT \"sum\" INT, out diff INT) LANGUAGE java PARAMETER STYLE java EXTERNAL NAME 'com.akiban.server.test.it.routines.TestJavaBasic.addSub'");
        stmt.close();
        CallableStatement cstmt = conn.prepareCall("CALL add_sub(100,?,?,?)");
        cstmt.setInt(1, 23);
        assertFalse("call returned results", cstmt.execute());
        assertEquals("sum result", 123, cstmt.getInt("sum"));
        assertEquals("diff results", 77L, cstmt.getObject("diff"));
    }

    @Test
    public void testScriptProcedure() throws Exception {
        Connection conn = DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE PROCEDURE concat_n(IN s VARCHAR(128), IN x INT, OUT ns VARCHAR(128)) LANGUAGE javascript PARAMETER STYLE variables AS 's+x'");
        stmt.close();
        CallableStatement cstmt = conn.prepareCall("CALL concat_n(?,?,?)");
        cstmt.setString(1, "Hello ");
        cstmt.setInt(2, 123);
        assertFalse("call returned results", cstmt.execute());
        assertEquals("script results", "Hello 123", cstmt.getString(3));
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
        Connection conn = DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
        Statement stmt = conn.createStatement();
        stmt.execute(defn);
        stmt.close();
        CallableStatement cstmt = conn.prepareCall("CALL get_co(?)");
        cstmt.setInt(1, 2);
        assertTrue("call returned results", cstmt.execute());
        ResultSet rs = cstmt.getResultSet();
        assertTrue("script results 1", rs.next());
        assertEquals("script results 1 value", "Jones", rs.getString(1));
        assertFalse("script results 1 more", rs.next());
        rs.close();
        assertTrue("call returned more results", cstmt.getMoreResults());
        rs = cstmt.getResultSet();
        assertTrue("script results 2", rs.next());
        assertEquals("script results 2 value", "2012-04-01", rs.getDate(1).toString());
        assertFalse("script results 2 more", rs.next());
        rs.close();
        assertFalse("call returned more results", cstmt.getMoreResults());
    }

}
