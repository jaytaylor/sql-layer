/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.server.test.it.isolation;

import com.foundationdb.sql.embedded.JDBCConnection;
import com.foundationdb.server.error.ErrorCode;

import java.sql.*;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.Map;

public class ReadCommittedIsolationIT extends IsolationITBase
{
    private static final int NROWS = 10;

    @Override
    public Map<String,String> startupConfigProperties() {
        Map<String,String> props = new HashMap<>();
        props.putAll(super.startupConfigProperties());
        props.put("fdbsql.fdb.periodically_commit.scan_limit", "2");
        // With the ordinary amount of lookahead, won't see changes made in
        // the middle because the scans will have already been done.
        props.put("fdbsql.pipeline.groupLookup.lookaheadQuantum", "2");
        return props;
    }

    @Before
    public void populate() {
        int tid = createTable(SCHEMA_NAME, "t1", "id INT PRIMARY KEY, n INT");
        for (int i = 0; i < NROWS; i++) {
            writeRow(tid, i, i);
        }
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT)
    public void scanIdle() throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, n FROM t1")) {
            for (int i = 0; i < NROWS; i++) {
                assertTrue("more rows", rs.next());
                assertEquals("id", i, rs.getInt(1));
                assertEquals("n", i, rs.getInt(2));
            }
        }
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT)
    public void scanWhileUpdate() throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, n FROM t1")) {
            for (int i = 0; i < NROWS; i++) {
                if (i == 3) {
                    try (Connection conn2 = getAutoCommitConnection();
                         Statement stmt2 = conn2.createStatement()) {
                        assertEquals(1, stmt2.executeUpdate("UPDATE t1 SET n = n + 1 WHERE id = " + NROWS / 2));
                    }
                }
                assertTrue("more rows", rs.next());
                assertEquals("id", i, rs.getInt(1));
                assertEquals("n", (i == NROWS / 2) ? i + 1 : i, rs.getInt(2));
            }
        }
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT)
    public void scanWhileDelete() throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             // A plan that will have a nested scan.
             ResultSet rs = stmt.executeQuery("SELECT id, n FROM t1 WHERE id > 0")) {
            for (int i = 1; i < NROWS; i++) {
                if (i == 2) {
                    try (Connection conn2 = getAutoCommitConnection();
                         Statement stmt2 = conn2.createStatement()) {
                        assertEquals(1, stmt2.executeUpdate("DELETE FROM t1 WHERE id = " + NROWS / 2));
                    }
                }
                if (i == NROWS / 2) continue;
                assertTrue("more rows", rs.next());
                assertEquals("id", i, rs.getInt(1));
                assertEquals("n", i, rs.getInt(2));
            }
        }
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT)
    public void scanWhileAlter() throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t1")) {
            for (int i = 0; i < NROWS; i++) {
                if (i == 2) {
                    try (Connection conn2 = getAutoCommitConnection();
                         Statement stmt2 = conn2.createStatement()) {
                        stmt2.execute("ALTER TABLE t1 DROP COLUMN n");
                    }
                }
                if (!rs.next()) break; // Only check that it vanished.
                assertEquals("id", i, rs.getInt(1));
                assertEquals("n", i, rs.getInt(2));
            }
        }
    }

    @Test
    @Isolation(Connection.TRANSACTION_SERIALIZABLE)
    public void updateThenReadOnly() throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            assertEquals(1, stmt.executeUpdate("UPDATE t1 SET n = n + 1 WHERE id = 1"));

            conn.setTransactionIsolation(JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT);
            assumeTrue(conn.getTransactionIsolation() == JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT);

            String sqlState = null;
            try {
                assertEquals(1, stmt.executeUpdate("UPDATE t1 SET n = n + 1 WHERE id = 2"));
            }
            catch (SQLException ex) {
                sqlState = ex.getSQLState();
            }
            assertEquals("Expected read only", ErrorCode.TRANSACTION_READ_ONLY.getFormattedValue(), sqlState);

            // See updated results in new read-only transaction.
            try (ResultSet rs = stmt.executeQuery("SELECT id, n FROM t1 WHERE id IN (1,2) ORDER BY 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(2, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(2, rs.getInt(2));
            }

            conn.rollback();
        }
        // Results committed before rollback.
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, n FROM t1 WHERE id IN (1,2) ORDER BY 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
        }
    }

}
