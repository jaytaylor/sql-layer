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

package com.foundationdb.server.test.mt;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.api.dml.ConstantColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.multiscan_update.MultiScanUpdateIT;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.*;

@Ignore("DMLFunctions cursor spanning multiple transactions")
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

    private final BlockingQueue<UpdateTuple> updateQueue = new ArrayBlockingQueue<>(1);
    private final BlockingQueue<Throwable> updateExceptions = new ArrayBlockingQueue<>(1);
    private Thread updateThread;
}
