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

package com.akiban.sql.pg;

import com.akiban.server.error.ErrorCode;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static junit.framework.Assert.*;

public class QueryCancelationIT extends PostgresServerITBase
{
    private static final int N = 1000;
    private static final int TRIALS = 5;

    @Before
    public void before()
    {
        configService().testing(true); // Suppress warning-level logging about cancelation
    }

    @Test
    public void test() throws Exception
    {
        loadDB();
        queryThread = startQueryThread();
        cancelThread = startCancelThread(queryThread);
        for (int i = 0; i < TRIALS; i++) {
            System.out.println("trial " + i);
            test(false);
            test(true);
        }
        queryThread.terminate();
        cancelThread.terminate();
        queryThread.join();
        cancelThread.join();
    }

    private void loadDB() throws Exception
    {
        Connection connection = openConnection();
        Statement statement = connection.createStatement();
        statement.execute("create table t(id integer not null primary key)");
        for (int id = 0; id < N; id++) {
            statement.execute(String.format("insert into t values(%s)", id));
        }
        statement.execute("select count(*) from t");
        ResultSet resultSet = statement.getResultSet();
        resultSet.next();
        System.out.println(String.format("Loaded %s rows", resultSet.getInt(1)));
        statement.close();
    }

    private void test(boolean withCancelation) throws Exception
    {
        System.out.println("cancelation: " + withCancelation);
        queryThread.resetCounters();
        queryThread.go();
        if (withCancelation) {
            cancelThread.go();
        }
        Thread.sleep(5000);
        // System.out.println("About to pause threads");
        queryThread.pause();
        System.out.println(String.format("queries: %s, canceled: %s",
                                         queryThread.queryCount, queryThread.cancelCount));
        if (withCancelation) {
            cancelThread.pause();
            assertTrue(queryThread.cancelCount > 0);
        } else {
            // Allow for the last cancelation from the (withCancelation=true) test to have arrived too late.
            assertTrue(queryThread.cancelCount <= 1);
        }
        assertEquals(0, queryThread.unexpectedRowsCount);
        assertTrue(queryThread.queryCount > 0);
        assertNull(queryThread.termination);
    }

    private QueryThread startQueryThread() throws Exception
    {
        QueryThread thread = new QueryThread();
        thread.setDaemon(false);
        thread.start();
        return thread;
    }

    private CancelThread startCancelThread(final QueryThread queryThread) throws Exception
    {
        CancelThread thread = new CancelThread(queryThread);
        thread.setDaemon(false);
        thread.start();
        return thread;
    }

    private QueryThread queryThread;
    private CancelThread cancelThread;

    enum TestThreadState { PAUSED, RUNNING, STOP, TERMINATE }

    public abstract class TestThread extends Thread
    {
        @Override
        public void run()
        {
            try {
                while (!done) {
                    synchronized (this) {
                        while (state == TestThreadState.PAUSED) {
                            // System.out.println(String.format("%s: wait ...", this));
                            wait();
                        }
                    }
                    if (!done) {
                        while (state == TestThreadState.RUNNING) {
                            action();
                            synchronized (this) {
                                if (state == TestThreadState.STOP) {
                                    state = TestThreadState.PAUSED;
                                    notify();
                                }
                            }
                        }
                        // System.out.println(String.format("%s: about to wait", this));
                    }
                }
            } catch (Throwable e) {
                termination = e;
            }
        }

        public abstract void action() throws SQLException, InterruptedException;

        public synchronized final void go() throws InterruptedException
        {
            // System.out.println(String.format("%s: go", this));
            state = TestThreadState.RUNNING;
            notify();
        }

        public synchronized final void pause() throws InterruptedException
        {
            // System.out.println(String.format("%s: pausing", this));
            state = TestThreadState.STOP;
            while (state != TestThreadState.PAUSED) {
                wait();
            }
            // System.out.println(String.format("%s: paused", this));
        }

        public synchronized final void terminate()
        {
            // System.out.println(String.format("%s: terminate", this));
            assert state == TestThreadState.PAUSED;
            state = TestThreadState.TERMINATE;
            done = true;
            notify();
        }

        public TestThread()
        {
            state = TestThreadState.PAUSED;
        }

        private volatile TestThreadState state;
        private volatile boolean done = false;
        Throwable termination;
    }

    public class QueryThread extends TestThread
    {
        @Override
        public void action() throws SQLException
        {
            queryCount++;
            int rowCount = 0;
            try {
                statement.execute("select * from t");
                ResultSet resultSet = statement.getResultSet();
                while (resultSet.next()) {
                    rowCount++;
                }
                if (rowCount != N) {
                    unexpectedRowsCount++;
                }
            } catch (SQLException e) {
                if (e.getSQLState().equals(ErrorCode.QUERY_CANCELED.getFormattedValue())) {
                    cancelCount++;
                } else {
                    throw e;
                }
            }
        }

        public void resetCounters()
        {
            queryCount = 0;
            cancelCount = 0;
            unexpectedRowsCount = 0;
        }

        public QueryThread() throws Exception
        {
            setName("QueryThread");
            connection = openConnection();
            statement = connection.createStatement();
        }

        private Connection connection;
        volatile Statement statement;
        int queryCount;
        int cancelCount;
        int unexpectedRowsCount;
    }

    public class CancelThread extends TestThread
    {
        @Override
        public void action() throws InterruptedException, SQLException
        {
            victim.statement.cancel();
            Thread.sleep(5);
        }

        public CancelThread(QueryThread victim) throws Exception
        {
            this.victim = victim;
            setName("CancelThread");
        }

        private QueryThread victim;
    }
}
