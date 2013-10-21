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

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.lock.LockService;
import com.foundationdb.server.service.session.Session;
import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

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
            lockService().claimTableInterruptible(session, LockService.Mode.EXCLUSIVE, tableID);
        }

        private void doUnlock(Session session) throws InterruptedException {
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
