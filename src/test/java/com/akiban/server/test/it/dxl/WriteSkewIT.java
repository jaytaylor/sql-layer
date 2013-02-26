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

package com.akiban.server.test.it.dxl;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;

import com.akiban.ais.model.TableName;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.test.it.ITBase;
import com.persistit.exception.RollbackException;

// Inspired by bug 1078331

public class WriteSkewIT extends ITBase
{
    // Test case from bug 1078331, comment 2
    @Ignore ("broken until #1118871 is fixed") @Test
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
    @Ignore ("broken until #1118871 is fixed") @Test
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
        assertTrue(sessionB.termination() instanceof RollbackException);
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
        createGroupIndex(TableName.create(SCHEMA, "parent"), "idx_pxcy", "parent.x, child.y");
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
            } catch (RollbackException e) {
                termination = e;
                exceptionInAnyThread.set(true);
            } catch (Exception e) {
                System.out.printf("Thread %s threw unexpected Exception %s\n", Thread.currentThread().getName(), e);
                e.printStackTrace();
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
