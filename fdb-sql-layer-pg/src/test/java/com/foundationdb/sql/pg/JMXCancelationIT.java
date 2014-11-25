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

package com.foundationdb.sql.pg;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.sql.test.JMXInterpreter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class JMXCancelationIT extends PostgresServerITBase
{
    private static final int SERVER_JMX_PORT = 8082;
    private static final String SERVER_ADDRESS = "localhost";
    private static final int N = 1000;

    private final List<Throwable> uncaughtExceptions = Collections.synchronizedList(new ArrayList<Throwable>());

    private final UncaughtExceptionHandler UNCAUGHT_HANDLER = new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            uncaughtExceptions.add(e);
        }
    };


    @Before
    public void loadDB() throws Exception {
        Statement statement = getConnection().createStatement();
        statement.execute("CREATE TABLE t(id INTEGER NOT NULL PRIMARY KEY)");
        getConnection().setAutoCommit(false);
        for (int id = 0; id < N; id++) {
            statement.execute(String.format("INSERT INTO t VALUES(%s)", id));
        }
        getConnection().commit();
        getConnection().setAutoCommit(true);
        statement.close();
        uncaughtExceptions.clear();
    }

    @After
    public void checkUncaught() {
        assertEquals("uncaught exceptions", "[]", uncaughtExceptions.toString());
    }

    @Test
    public void testCancel() throws Exception {
        test("cancelQuery", false);
    }

    @Test
    public void testKillConnection() throws Exception {
        test("killConnection", true);
    }

    private void test(String method, boolean forKill) throws Exception {
        try(JMXInterpreter jmx = new JMXInterpreter()) {
            Integer[] sessions = (Integer[])
                jmx.makeBeanCall(SERVER_ADDRESS, SERVER_JMX_PORT,
                                 "com.foundationdb:type=PostgresServer",
                                 "CurrentSessions", null, "get");
            List<Integer> before = Arrays.asList(sessions);

            CountDownLatch latch = new CountDownLatch(1);
            Thread queryThread = startQueryThread(forKill, latch);
            latch.await();

            // Connection is open, so (unique) session should exist.
            sessions = (Integer[])
                jmx.makeBeanCall(SERVER_ADDRESS, SERVER_JMX_PORT,
                                 "com.foundationdb:type=PostgresServer",
                                 "CurrentSessions", null, "get");
            List<Integer> after = Arrays.asList(sessions);
            after = new ArrayList<>(after);
            after.removeAll(before);

            assertEquals(1, after.size());
            Integer session = after.get(0);

            // Still need to wait for session to have a query in progress.
            while (true) {
                String sql = (String)
                    jmx.makeBeanCall(SERVER_ADDRESS, SERVER_JMX_PORT,
                                     "com.foundationdb:type=PostgresServer",
                                     "getSqlString", new Object[] { session }, "method");
                if (sql != null) 
                    break;
                Thread.sleep(50);
            }

            jmx.makeBeanCall(SERVER_ADDRESS, SERVER_JMX_PORT,
                             "com.foundationdb:type=PostgresServer",
                             method, new Object[] { session }, "method");

            queryThread.join();
        }
    }

    private Thread startQueryThread(final boolean forKill, final CountDownLatch latch) throws Exception {
        Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Connection connection = null;
                    Statement statement = null;
                    try {
                        connection = openConnection();
                        latch.countDown();
                        statement = connection.createStatement();
                        statement.execute("SELECT COUNT(*) FROM t t1, t t2, t t3");
                        fail("Query should not complete.");
                    }
                    catch (SQLException ex) {
                        String sqlState = ex.getSQLState();
                        if (forKill) {
                            // Kill case can also see connection close (PSQLState.CONNECTION_FAILURE).
                            if (!"08006".equals(sqlState))
                                assertEquals(ErrorCode.CONNECTION_TERMINATED.getFormattedValue(), sqlState);
                        }
                        else {
                            assertEquals(ErrorCode.QUERY_CANCELED.getFormattedValue(), sqlState);
                        }
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    finally {
                        try {
                            if (statement != null)
                                statement.close();
                            closeConnection(connection);
                        }
                        catch (Exception ex) {
                            // Ignore
                        }
                    }
                }
            });
        thread.setUncaughtExceptionHandler(UNCAUGHT_HANDLER);
        thread.setDaemon(false);
        thread.start();
        return thread;
    }

}
