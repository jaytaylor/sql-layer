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

package com.foundationdb.server.test.it.dxl;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.foundationdb.util.Exceptions;
import org.junit.Test;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.ITBase;

// Inspired by bug 1078331

public class WriteSkewIT extends ITBase
{
    // Test case from bug 1078331, comment 2
    @Test
    public void testHKeyMaintenance() throws InterruptedException
    {
        createDatabase();
        dml().writeRow(session(), ITBase.createNewRow(parentRowDef, 1, 100));
        dml().writeRow(session(), ITBase.createNewRow(parentRowDef, 2, 200));
        dml().writeRow(session(), ITBase.createNewRow(childRowDef, 1, 1, 1100));
        dml().writeRow(session(), ITBase.createNewRow(grandchildRowDef, 1, 1, 11100));
        TestThread threadA = new TestThread("a")
        {
            @Override
            public void doAction()
            {
                dml().writeRow(threadPrivateSession, ITBase.createNewRow(childRowDef, 2, 2, 2200));
            }
        };
        TestThread threadB = new TestThread("b")
        {
            @Override
            public void doAction()
            {
                dml().writeRow(threadPrivateSession, ITBase.createNewRow(grandchildRowDef, 2, 2, 22200));
            }
        };
        runTest(threadA, threadB);
    }

    // Test case from description of bug 1078331
    @Test
    public void testGroupIndexMaintenance() throws InterruptedException
    {
        createDatabase();
        dml().writeRow(session(), ITBase.createNewRow(parentRowDef, 1, 100));
        dml().writeRow(session(), ITBase.createNewRow(childRowDef, 11, 1, 1100));
        TestThread threadA = new TestThread("a")
        {
            @Override
            public void doAction()
            {
                dml().writeRow(threadPrivateSession, ITBase.createNewRow(parentRowDef, 2, 2200));
            }
        };
        TestThread threadB = new TestThread("b")
        {
            @Override
            public void doAction()
            {
                dml().updateRow(threadPrivateSession, ITBase.createNewRow(childRowDef, 11, 1, 1100), 
                        ITBase.createNewRow(childRowDef, 11, 2, 1100), null);
            }
        };
        runTest(threadA, threadB);
    }

    private void runTest(TestThread sessionA, TestThread sessionB) throws InterruptedException
    {
        sessionA.start();
        sessionB.start();

        sessionA.semA.release();
        sessionA.semB.tryAcquire(5, TimeUnit.SECONDS);
        
        sessionB.semA.release();
        /**
         * Give session B time to conflict before A
         * commits.
         */
        sessionB.semB.tryAcquire(1, TimeUnit.SECONDS);
        
        sessionA.semA.release();
        sessionA.semB.tryAcquire(5, TimeUnit.SECONDS);
                
        sessionA.join(10000);
        sessionB.join(10000);
        assertNull(sessionA.termination());
        assertTrue("sessionB termination was rollback", Exceptions.isRollbackException(sessionB.termination()));
    }

    private void createDatabase() throws InvalidOperationException
    {
        final String SCHEMA = "schema";
        parent = createTable(SCHEMA, "parent",
                             "pid int not null",
                             "x int",
                             "primary key(pid)");
        child = createTable(SCHEMA, "child",
                            "cid int not null",
                            "pid int",
                            "y int",
                            "primary key(cid)",
                            "grouping foreign key(pid) references parent(pid)");
        grandchild = createTable(SCHEMA, "grandchild",
                                 "gid int not null",
                                 "cid int",
                                 "z int",
                                 "primary key(gid)",
                                 "grouping foreign key(cid) references child(cid)");
        parentRowDef = getRowDef(parent);
        childRowDef = getRowDef(child);
        grandchildRowDef = getRowDef(grandchild);
        createLeftGroupIndex(TableName.create(SCHEMA, "parent"), "idx_pxcy", "parent.x", "child.y");
    }

    private int parent;
    private int child;
    private int grandchild;
    private RowDef parentRowDef;
    private RowDef childRowDef;
    private RowDef grandchildRowDef;
    
    private final AtomicBoolean exceptionInAnyThread = new AtomicBoolean(false);
    
    abstract private class TestThread extends Thread
    {
        
        TestThread(final String name) {
            super(name);
        }
        
        @Override
        public void run()
        {
            txnService().beginTransaction(threadPrivateSession);
            boolean committed = false;
            try {
                semA.tryAcquire(5, TimeUnit.SECONDS);
                doAction();
                semB.release();
                semA.tryAcquire(5, TimeUnit.SECONDS);
                txnService().commitTransaction(threadPrivateSession);
                semB.release();
                committed = true;
            } catch (Exception e) {
                termination = e;
                exceptionInAnyThread.set(true);
            } finally {
                if (!committed) {
                    txnService().rollbackTransactionIfOpen(threadPrivateSession);
                    semB.release();
                }
            }
        }
        
        abstract protected void doAction();

        public Exception termination()
        {
            return termination;
        }

        protected final Session threadPrivateSession = serviceManager().getSessionService().createSession();
        private Exception termination;
        private final Semaphore semA = new Semaphore(0);
        private final Semaphore semB = new Semaphore(0);
    }

}
