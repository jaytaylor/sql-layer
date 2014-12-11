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

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.sql.embedded.JDBCConnection;

import java.sql.*;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SnapshotIsolationIT extends IsolationITBase
{
    private static final int NBALLS = 10;

    @Before
    public void populate() {
        int tid = createTable(SCHEMA_NAME, "balls", "id INT PRIMARY KEY, color VARCHAR(8)");
        for (int i = 0; i < NBALLS; i++) {
            writeRow(tid, i, (i & 1) == 0 ? "black" : "white");
        }
    }

    protected void switchColors() throws SQLException {
        try (Connection conn1 = getConnection();
             Statement stmt1 = conn1.createStatement();
             Connection conn2 = getConnection();
             Statement stmt2 = conn2.createStatement()) {

            int n = stmt1.executeUpdate("UPDATE balls SET color = 'white' WHERE color = 'black'");
            assertEquals("black balls updated", NBALLS / 2, n);

            n = stmt2.executeUpdate("UPDATE balls SET color = 'black' WHERE color = 'white'");
            assertEquals("white balls updated", NBALLS / 2, n);

            conn1.commit();
            conn2.commit();
        }
    }

    @Test
    @Isolation(Connection.TRANSACTION_SERIALIZABLE)
    @SQLExceptionExpected(errorCode = ErrorCode.FDB_NOT_COMMITTED)
    public void serializableConflicts() throws SQLException {
        switchColors();
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_SERIALIZABLE_SNAPSHOT)
    public void snapshotSkews() throws SQLException {
        switchColors();
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT color, COUNT(*) FROM balls GROUP BY 1 ORDER BY 1")) {
            assertTrue(rs.next());
            assertEquals("black", rs.getString(1));
            assertEquals("black count", NBALLS / 2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("white", rs.getString(1));
            assertEquals("white count", NBALLS / 2, rs.getInt(2));
        }
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_SERIALIZABLE_SNAPSHOT)
    @Ignore("needs 3.0 semantics")
    public void snapshotRYW() throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            assertEquals(NBALLS / 2, stmt.executeUpdate("UPDATE balls SET color = 'blue' WHERE color = 'black'"));
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM balls WHERE color <> 'black'")) {
                rs.next();
                assertEquals("See combined snapshot", NBALLS, rs.getInt(1));
            }
        }
    }

}
