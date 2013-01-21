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

import com.akiban.ais.model.TableName;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.session.Session;
import com.akiban.server.test.it.ITBase;
import com.persistit.exception.RollbackException;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

// Inspired by bug 1078331

public class WriteSkewIT extends ITBase
{
    // Test case from bug 1078331, comment 2
    @Test
    public void testHKeyMaintenance() throws InterruptedException
    {
        createDatabase();
        dml().writeRow(session(), createNewRow(parent, 1, 100));
        dml().writeRow(session(), createNewRow(parent, 2, 200));
        dml().writeRow(session(), createNewRow(child, 1, 1, 1100));
        dml().writeRow(session(), createNewRow(grandchild, 1, 1, 11100));
        Action actionA = new Action()
        {
            @Override
            public void run()
            {
                dml().writeRow(session, createNewRow(child, 2, 2, 2200));
            }
        };
        Action actionB = new Action()
        {
            @Override
            public void run()
            {
                dml().writeRow(session, createNewRow(grandchild, 2, 2, 22200));
            }
        };
        runTest(actionA, actionB);
    }

    // Test case from description of bug 1078331
    @Test
    public void testGroupIndexMaintenance() throws InterruptedException
    {
        createDatabase();
        dml().writeRow(session(), createNewRow(parent, 1, 100));
        dml().writeRow(session(), createNewRow(child, 11, 1, 1100));
        Action actionA = new Action()
        {
            @Override
            public void run()
            {
                dml().writeRow(session, createNewRow(parent, 2, 2200));
            }
        };
        Action actionB = new Action()
        {
            @Override
            public void run()
            {
                dml().updateRow(session, createNewRow(child, 11, 1, 1100), createNewRow(child, 11, 2, 1100), null);
            }
        };
        runTest(actionA, actionB);
    }

    private void runTest(Action actionA, Action actionB) throws InterruptedException
    {
        TestThread sessionA = new TestThread(actionA);
        TestThread sessionB = new TestThread(actionB);
        sessionA.start();
        sessionB.start();
        sessionA.proceed();
        sessionB.proceed();
        sessionA.proceed();
        sessionB.proceed();
        sessionA.join();
        sessionB.join();
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
        createGroupIndex(TableName.create(SCHEMA, "parent"), "idx_pxcy", "parent.x, child.y");
    }

    private int parent;
    private int child;
    private int grandchild;
    private final AtomicBoolean exceptionInAnyThread = new AtomicBoolean(false);

    private abstract class Action
    {
        public abstract void run();

        public final Session session()
        {
            return session;
        }

        protected Action()
        {
            this.session = serviceManager().getSessionService().createSession();
        }

        protected final Session session;
    }

    private class TestThread extends Thread
    {
        @Override
        public void run()
        {
            txnService().beginTransaction(session);
            try {
                waitForPermissionToProceed();
                // System.out.println(String.format("%s: Before run", session));
                action.run();
                // System.out.println(String.format("%s: After run", session));
                waitForPermissionToProceed();
                // System.out.println(String.format("%s: Before commit", session));
                txnService().commitTransaction(session);
                // System.out.println(String.format("%s: After commit", session));
            } catch (OtherThreadTerminatedException e) {
/*
                System.out.println("Caught OtherThreadTerminatedException");
*/
            } catch (RollbackException e) {
/*
                System.out.println(String.format("%s: Rollback due to %s: %s",
                                                 session, e.getClass(), e.getMessage()));
                e.printStackTrace();
*/
                termination = e;
                exceptionInAnyThread.set(true);
                txnService().rollbackTransaction(session);
            } catch (Exception e) {
/*
                System.out.println(String.format("%s: Unexpected exception: %s: %s",
                                                 session, e.getClass(), e.getMessage()));
                e.printStackTrace();
*/
            }
        }

        public Exception termination()
        {
            return termination;
        }

        public TestThread(Action action)
        {
            this.action = action;
            this.session = action.session();
        }

        public synchronized void proceed() throws InterruptedException
        {
            okToProceed = false;
            notifyAll();
            while (!exceptionInAnyThread.get() && !okToProceed) {
                wait();
            }
        }

        private synchronized void waitForPermissionToProceed() throws InterruptedException, OtherThreadTerminatedException
        {
            while (!exceptionInAnyThread.get() && okToProceed) {
                wait();
            }
            if (exceptionInAnyThread.get()) {
                throw new OtherThreadTerminatedException();
            }
            okToProceed = true;
            notifyAll();
        }

        private final Session session;
        private final Action action;
        private volatile boolean okToProceed = true;
        private Exception termination;
    }

    private static class OtherThreadTerminatedException extends Exception
    {}
}
