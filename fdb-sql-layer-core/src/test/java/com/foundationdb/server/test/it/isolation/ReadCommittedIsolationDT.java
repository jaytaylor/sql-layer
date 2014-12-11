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
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadCommittedIsolationDT extends IsolationITBase
{
    private static final int NROWS = 10000;

    @Before
    public void populate() throws SQLException {
        createTable(SCHEMA_NAME, "t1", "id INT PRIMARY KEY, n INT");
        Properties props = new Properties();
        props.put("user", SCHEMA_NAME);
        props.put("constraintCheckTime", "DELAYED_WITH_RANGE_CACHE_ALWAYS_UNTIL_COMMIT");
        try (Connection conn = DriverManager.getConnection(CONNECTION_URL, props);
             PreparedStatement stmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {
            conn.setAutoCommit(false);
            for (int i = 0; i < NROWS; i++) {
                stmt.setInt(1, i);
                stmt.setInt(2, i);
                stmt.executeUpdate();
            }
            conn.commit();
        }
    }

    protected void slowScan() throws SQLException {
        int sum = 0;
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t1")) {
            while (rs.next()) {
                sum += rs.getInt(2);
                try {
                    Thread.sleep(6);
                }
                catch (InterruptedException ex) {
                    break;
                }
            }
        }
        assertEquals((NROWS * (NROWS - 1)) / 2, sum);
    }

    protected void manyScans() throws SQLException {
        int sum = 0;
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT n FROM t1 WHERE id = ?")) {
            for (int i = 0; i < NROWS; i++) {
                stmt.setInt(1, i);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        sum += rs.getInt(1);
                    }
                }
                try {
                    Thread.sleep(6);
                }
                catch (InterruptedException ex) {
                    break;
                }
            }
        }
        assertEquals((NROWS * (NROWS - 1)) / 2, sum);
    }

    @Test
    @Isolation(Connection.TRANSACTION_SERIALIZABLE)
    @SQLExceptionExpected(errorCode = ErrorCode.FDB_PAST_VERSION)
    public void slowScanPastVersion() throws SQLException {
        slowScan();
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT)
    public void slowScanReadCommitted() throws SQLException {
        slowScan();
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_SERIALIZABLE_SNAPSHOT)
    @SQLExceptionExpected(errorCode = ErrorCode.FDB_PAST_VERSION)
    public void slowScanSnapshotAlsoPastVersion() throws SQLException {
        slowScan();
    }

    @Test
    @Isolation(Connection.TRANSACTION_SERIALIZABLE)
    @SQLExceptionExpected(errorCode = ErrorCode.FDB_PAST_VERSION)
    public void manyScansPastVersion() throws SQLException {
        manyScans();
    }

    @Test
    @Isolation(JDBCConnection.TRANSACTION_READ_COMMITTED_NO_SNAPSHOT)
    public void manyScansReadCommitted() throws SQLException {
        manyScans();
    }

}
