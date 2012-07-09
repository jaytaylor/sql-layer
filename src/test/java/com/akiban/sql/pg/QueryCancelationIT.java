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

package com.akiban.sql.pg;

import com.akiban.server.error.ErrorCode;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import static junit.framework.Assert.*;

public class QueryCancelationIT extends PostgresServerITBase
{
    private static final int N = 1000;
    private static final int TRIALS = 3;

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
    
    @Test
    public void testSQLcancel() throws Exception {
        loadDB();
        queryThread = startQueryThread();
        cancelThread = startCancelSQLThread();
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
        Statement statement = getConnection().createStatement();
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
    
    private CancelThread startCancelSQLThread() throws Exception
    {
        CancelSQLThread thread = new CancelSQLThread();
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
            } finally {
                try {
                    cleanup();
                }
                catch (Exception ex) {
                    if (termination == null)
                        termination = ex;
                }
            }
        }

        public abstract void action() throws SQLException, InterruptedException;

        public void cleanup() throws Exception {
        }

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
            interrupt();
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

        @Override
        public void cleanup() throws Exception {
            closeConnection(connection);
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
    
    public class CancelSQLThread extends CancelThread
    {
        @Override
        public void action() throws SQLException, InterruptedException {
            statement.execute(String.format("ALTER SERVER INTERRUPT SESSION %s", sessionID));
            sleep(5);
        }

        @Override
        public void cleanup() throws Exception {
            closeConnection(connection);
        }

        public CancelSQLThread () throws Exception
        {
            super(null);
            
            System.out.println (String.format("CancelSQLThread found %s sessions", server().getCurrentSessions().size()));
            Iterator<Integer> i = server().getCurrentSessions().iterator();
            i.next();
            sessionID = i.next();
            
            connection = openConnection();
            statement = connection.createStatement();
            setName ("CancelSQLThread");
        }
        private int sessionID;
        private Connection connection;
        volatile Statement statement;

    }
}
