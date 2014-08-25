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

package com.foundationdb.sql.pg;

import com.foundationdb.sql.jdbc.core.BaseConnection;
import com.foundationdb.sql.jdbc.core.ProtocolConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionPeriodicallyCommitIT extends PostgresServerITBase {

    private static final int AFTER_BYTES = 200;
    /** The number of commits it took to hit the number of bytes at which it should commit **/
    private static final int NUMBER_OF_INSERTS = 4;
    /** the number of rows per insert **/
    private static final int NUMBER_OF_ROWS = 3;
    /** a 100 byte lorem ipsum **/
    private static final String SAMPLE_STRING =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris auctor enim dui, eget egestas metus.";

    @Before
    public void createSimpleSchema() throws Exception {
        String sqlCreate = "CREATE TABLE fake.T1 (c1 integer not null primary key)";
        getConnection().createStatement().execute(sqlCreate);
    }

    @After
    public void dontLeaveConnection() throws Exception {
        // Tests change transaction periodically commit. Easiest not to reuse.
        forgetConnection();
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("fdbsql.fdb.periodically_commit.after_bytes", Integer.toString(AFTER_BYTES));
        return config;
    }

    @Test
    public void testOn() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().setAutoCommit(false);
        int lastCount = -1;
        int rowIndex = 0;
        for (int i=0; i< NUMBER_OF_INSERTS * 2; i++) {
            rowIndex = insertRows(rowIndex, i);
            getConnection().rollback();
            int count = getCount();
            if (i < NUMBER_OF_INSERTS -1) {
                assertEquals("Should not have committed anything after " + i + " statements", 0, count);
            } else {
                if (lastCount < 0) {
                    if (count > 0) {
                        lastCount = count;
                    }
                }
                else if (count > lastCount) {
                    // Make sure that we're approximately consistent, just to make the tests a little less brittle
                    assertEquals(NUMBER_OF_INSERTS*NUMBER_OF_ROWS,lastCount,1);
                    assertEquals("Should be committing the same amount each time", lastCount*2, count);
                    return; // success, it committed twice during the transaction
                }
            }
        }
        if (lastCount < 0) {
            fail("never committed");
        } else {
            fail("only committed once");
        }
    }

    @Test
    public void testDefaultOff() throws Exception {
        testOffHelper();
    }

    @Test
    public void testExplicitlyOff() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'off'");
        testOffHelper();
    }

    @Test
    public void testUserLevel() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'userLevel'");
        getConnection().setAutoCommit(false);
        int lastCount = -1;
        int rowIndex = 0;
        for (int i=1; i< NUMBER_OF_INSERTS * 2; i++) {
            int transactionState = -1;
            for (int j = 0; j < i; j++) {
                rowIndex = insertRow(rowIndex);
                transactionState = ((BaseConnection) getConnection()).getTransactionState();
                if (transactionState == ProtocolConnection.TRANSACTION_IDLE) {
                    rowIndex = insertRow(rowIndex);
                    break;
                }
            }
            assertNotEquals(-1,transactionState);
            getConnection().rollback();
            int count = getCount();
            if (i < NUMBER_OF_INSERTS -1) {
                assertEquals("Should not have committed anything after " + i + " statements", 0, count);
            } else {
                if (lastCount < 0) {
                    if (count > 0) {
                        lastCount = count;
                        assertEquals("IDLE=0, OPEN=1, FAILED=2",
                                ProtocolConnection.TRANSACTION_IDLE, transactionState);
                    } else {
                        assertEquals("IDLE=0, OPEN=1, FAILED=2",
                                ProtocolConnection.TRANSACTION_OPEN, transactionState);
                    }
                }
                else if (count > lastCount) {
                    // Make sure that we're approximately consistent, just because the test may be broken
                    // if we're off by more than this.
                    assertEquals(count + " rows inserted after " + lastCount + " rows",
                            NUMBER_OF_INSERTS*NUMBER_OF_ROWS,lastCount,1);
                    assertEquals("Should be committing the same amount each time", lastCount*2, count);
                    assertEquals(count + " rows inserted after " + lastCount + " rows, but state not idle (OPEN is 1)",
                            ProtocolConnection.TRANSACTION_IDLE, transactionState);
                    return; // success, it committed twice during the transaction
                } else {
                    assertEquals("IDLE=0, OPEN=1, FAILED=2", ProtocolConnection.TRANSACTION_OPEN, transactionState);
                }
            }
        }
        if (lastCount < 0) {
            fail("never committed");
        } else {
            fail("only committed once");
        }
    }

    @Test
    public void testFailPartWayThroughInsertStatementOn() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(NULL, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) {}
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailPartWayThroughInsertStatementUserLevel() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'userLevel'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(NULL, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) {}
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailWithDeferredConstraintCheckOn() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().createStatement().execute("SET constraintCheckTime TO 'DEFERRED_WITH_RANGE_CACHE'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(0, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) { }
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailWithConstraintCheckOn() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'on'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(0, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) { }
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailWithConstraintCheckUserLevel() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'userLevel'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(0, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) { }
        // Note: if the above code works correctly, we'll be in an idle state, which jdbc uses to turn this into noop
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    @Test
    public void testFailWithDeferredConstraintCheckUserLevel() throws Exception {
        getConnection().createStatement().execute("SET transactionPeriodicallyCommit TO 'userLevel'");
        getConnection().createStatement().execute("SET constraintCheckTime TO 'DEFERRED_WITH_RANGE_CACHE'");
        getConnection().setAutoCommit(false);
        getConnection().createStatement().execute("DROP TABLE fake.T1");
        getConnection().createStatement().execute("CREATE TABLE fake.T1 (c1 integer not null primary key, c2 varchar(100))");
        try {
            getConnection().createStatement().execute(
                    "insert into fake.T1 VALUES (0, '" + SAMPLE_STRING + "'),(0, '" + SAMPLE_STRING + "');");
            fail("Expected exception");
        } catch (SQLException e) { }
        // Note: if the above code works correctly, we'll be in an idle state, which jdbc uses to turn this into noop
        getConnection().rollback();
        assertEquals(0, getCount());
    }

    public int insertRows(int rowIndex, int i) throws Exception {
        for (int j = 0; j < i; j++) {
            rowIndex = insertRow(rowIndex);
        }
        return rowIndex;
    }

    public int insertRow(int rowIndex) throws Exception {
        getConnection().createStatement().execute(
                "insert into fake.T1 VALUES (" + rowIndex++ + "),(" + rowIndex++ + "),(" + rowIndex++ + ")");
        return rowIndex;
    }

    public void testOffHelper() throws Exception {
        getConnection().setAutoCommit(false);
        int rowIndex = 0;
        int commitAt = NUMBER_OF_INSERTS * 2;
        for (int i=0; i <= commitAt; i++) {
            rowIndex = insertRows(rowIndex, i);
            if (i < commitAt) {
                getConnection().rollback();
                assertEquals("Should not have committed anything before committing", 0, getCount());
            } else {
                getConnection().commit();
                assertEquals("Should commit eventually", NUMBER_OF_ROWS * commitAt, getCount());
            }
        }
    }

    public int getCount() throws Exception {
        ResultSet resultSet = getConnection().createStatement().executeQuery("SELECT COUNT(*) FROM fake.T1");
        assertTrue(resultSet.next());
        return resultSet.getInt(1);
    }
}
