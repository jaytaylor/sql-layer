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

import com.akiban.ais.model.TableName;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.session.Session;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.Assert.*;

public class DDLLockTimeoutIT extends PostgresServerITBase
{
    private static final Logger LOG = LoggerFactory.getLogger(DDLLockTimeoutIT.class);

    private static final int QUERY_TIMEOUT_SEC = 1;
    private static final int TEST_TIMEOUT_SEC = 5;
    private static final TableName TABLE_NAME = new TableName(SCHEMA_NAME, "t");

    static enum State {
        SET_TIMEOUT, TIMEOUT_SET, LOCK_DDL, DDL_LOCKED, QUERY, TIMEOUT, UNLOCK_DDL
    };

    volatile State state;
    volatile boolean testDidTimeout = false;

    synchronized void waitForState(State newState) throws InterruptedException {
        while (state != newState && !testDidTimeout) {
            wait();
        }
    }

    synchronized void setState(State state) {
        this.state = state;
        notifyAll();
    }

    private DXLReadWriteLockHook getDXLLock() {
        return DXLReadWriteLockHook.only();
    }

    @Test
    public void test() throws Exception {
        int tableID = createTable(TABLE_NAME, "id int");
        Thread queryThread = new QueryThread();
        Thread ddlThread = new DDLThread(tableID);
        Thread watchdogThread = new WatchdogThread(queryThread, ddlThread);
        watchdogThread.start();
        setState(State.SET_TIMEOUT);
        waitForState(State.TIMEOUT_SET);
        setState(State.LOCK_DDL);
        waitForState(State.DDL_LOCKED);
        setState(State.QUERY);
        waitForState(State.TIMEOUT);
        setState(State.UNLOCK_DDL);
        watchdogThread.join();
        assertFalse("Test timed out", testDidTimeout);
    }

    class QueryThread extends Thread {
        public QueryThread() {
            super("QueryThread");
        }

        @Override
        public void run() {
            try {
                Statement statement = getConnection().createStatement();
                waitForState(DDLLockTimeoutIT.State.SET_TIMEOUT);
                statement.executeUpdate("SET queryTimeoutSec = '"+QUERY_TIMEOUT_SEC+"'");
                setState(DDLLockTimeoutIT.State.TIMEOUT_SET);
                waitForState(DDLLockTimeoutIT.State.QUERY);
                try {
                    if (getDXLLock().isDDLLockEnabled())
                        statement.executeQuery("SELECT 2+2");
                    else
                        statement.executeUpdate("INSERT INTO "+TABLE_NAME+" VALUES(1)");
                    fail("Query did not time out");
                }
                catch (SQLException ex) {
                    if (!ex.getSQLState().equals(ErrorCode.QUERY_TIMEOUT.getFormattedValue())) {
                        throw ex;
                    }
                }
                setState(DDLLockTimeoutIT.State.TIMEOUT);
                forgetConnection();
            }
            catch (Exception ex) {
                fail(ex.getMessage());
            }
        }
    }

    class DDLThread extends Thread {
        final int tableID;

        public DDLThread(int tableID) {
            super("DDLThread");
            this.tableID = tableID;
        }

        private void doLock(Session session) throws InterruptedException {
            if (getDXLLock().isDDLLockEnabled())
                getDXLLock().lock(session, DXLFunction.UNSPECIFIED_DDL_WRITE, -1);
            else
                lockService().claimTableInterruptible(session, LockService.Mode.EXCLUSIVE, tableID);
        }

        private void doUnlock(Session session) throws InterruptedException {
            if (getDXLLock().isDDLLockEnabled())
                getDXLLock().unlock(session, DXLFunction.UNSPECIFIED_DDL_WRITE);
            else
                lockService().releaseTable(session, LockService.Mode.EXCLUSIVE, tableID);
        }

        @Override
        public void run() {
            try {
                Session session = createNewSession();
                waitForState(DDLLockTimeoutIT.State.LOCK_DDL);
                doLock(session);
                try {
                    setState(DDLLockTimeoutIT.State.DDL_LOCKED);
                    waitForState(DDLLockTimeoutIT.State.UNLOCK_DDL);
                } finally {
                    doUnlock(session);
                }
            }
            catch (InterruptedException e) {
                fail("Interrupted");
            }
            catch (Exception ex) {
                fail(ex.getMessage());
            }
        }
    }

    class WatchdogThread extends Thread {
        private final Thread[] threads;

        public WatchdogThread(Thread... threads) {
            super("WatchdogThread");
            this.threads = threads;
        }

        @Override
        public void run() {
            for (Thread t : threads) {
                t.start();
            }

            final long endMillis = System.currentTimeMillis() + (TEST_TIMEOUT_SEC*1000);
            for (Thread t : threads) {
                long remaining =  endMillis - System.currentTimeMillis();
                if (remaining <= 0)
                    break;
                try {
                    LOG.debug("Waiting on {} for {}ms", t, remaining);
                    t.join(remaining);
                } catch(InterruptedException e) {
                    LOG.error("Watchdog interrupted");
                    break;
                }
            }

            for (Thread t : threads) {
                if (t.isAlive()) {
                    LOG.error("Interrupting {}", t);
                    testDidTimeout = true;
                    t.interrupt();
                }
            }
            synchronized (DDLLockTimeoutIT.this) {
                DDLLockTimeoutIT.this.notifyAll();
            }
        }
    }
}
