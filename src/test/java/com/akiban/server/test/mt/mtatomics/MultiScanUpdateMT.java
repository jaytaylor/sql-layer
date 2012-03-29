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

package com.akiban.server.test.mt.mtatomics;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.service.session.Session;
import com.akiban.server.test.it.multiscan_update.MultiScanUpdateIT;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.*;

public final class MultiScanUpdateMT extends MultiScanUpdateIT {

    // test setup/teardown

    @Before
    public void setUpUpdater() {
        assertEquals("updateQueue.size()", 0, updateQueue.size());
        assertEquals("updateExceptions.size()", 0, updateExceptions.size());
        Session updateSession = serviceManager().getSessionService().createSession();
        updateThread = new Thread(new UpdateRunnable(updateSession, updateQueue, updateExceptions, dml()));
        updateThread.start();
    }

    @After
    public void tearDownUpdater() {
        assertEquals("updateQueue.size()", 0, updateQueue.size());
        assertEquals("updateExceptions.size()", 0, updateExceptions.size());
        updateThread.interrupt();
    }

    // MultiScanUpdateIT interface

    @Override
    protected void doUpdate(NewRow oldRow, NewRow newRow) {
        UpdateTuple tuple = new UpdateTuple(oldRow, newRow);
        final Throwable result;
        try {
            updateQueue.put(tuple);
            result = updateExceptions.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (! (result instanceof PlaceholderException) ) {
            throw new RuntimeException(result);
        }
    }

    // MultiScanUpdateMT interface

    public MultiScanUpdateMT(TestMode testMode, WhichIndex scanIndex, WhichIndex updateColumn) {
        super(testMode, scanIndex, updateColumn);
    }

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        return params(TestMode.MT);
    }

    // inner classes

    private static class UpdateTuple {
        private final NewRow oldRow;
        private final NewRow newRow;

        private UpdateTuple(NewRow oldRow, NewRow newRow) {
            this.oldRow = oldRow;
            this.newRow = newRow;
        }

        public NewRow oldRow() {
            return oldRow;
        }

        public NewRow newRow() {
            return newRow;
        }
    }

    private static class UpdateRunnable implements Runnable {
        private final Session session;
        private final BlockingQueue<UpdateTuple> updateQueue;
        private final BlockingQueue<Throwable> updateResults;
        private final DMLFunctions dml;

        private UpdateRunnable(Session session,
                               BlockingQueue<UpdateTuple> updateQueue,
                               BlockingQueue<Throwable> updateExceptions,
                               DMLFunctions dml)
        {
            this.session = session;
            this.updateQueue = updateQueue;
            this.dml = dml;
            this.updateResults = updateExceptions;
        }

        @Override
        public void run() {
            while(!Thread.interrupted()) {
                final UpdateTuple updateTuple;
                try {
                    updateTuple = updateQueue.take();
                } catch (InterruptedException e) {
                    return;
                    // that's fine, it just means @After was invoked
                }
                Throwable result;
                try {
                    dml.updateRow(session, updateTuple.oldRow(), updateTuple.newRow(), ConstantColumnSelector.ALL_ON);
                    result = new PlaceholderException();
                } catch (Throwable t) {
                    result = t;
                }
                try {
                    updateResults.put(result);
                } catch (InterruptedException e) {
                    throw new RuntimeException("unexpected interrupt!", e);
                }
            }
        }
    }

    private static class PlaceholderException extends Exception {
        // nothing
    }

    // object state

    private final BlockingQueue<UpdateTuple> updateQueue = new ArrayBlockingQueue<UpdateTuple>(1);
    private final BlockingQueue<Throwable> updateExceptions = new ArrayBlockingQueue<Throwable>(1);
    private Thread updateThread;
}
