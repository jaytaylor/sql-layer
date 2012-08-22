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

package com.akiban.server.test.it.qp;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.*;
import com.akiban.qp.row.Row;
import com.akiban.server.AkServer;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.error.QueryTimedOutException;
import com.akiban.server.explain.Explainer;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.akiban.qp.operator.API.cursor;
import java.util.Map;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryTimeoutIT extends OperatorITBase
{
    @Before
    public void before()
    {
        super.before();
        session().cancelCurrentQuery(false);
    }

    @Test
    public void noExitWithDefaultTimeout() throws InterruptedException
    {
        final Operator plan = new DoNothingForever();
        final Cursor cursor = cursor(plan, queryContext);
        final AtomicBoolean exited = new AtomicBoolean(false);
        Thread queryThread = new Thread(
            new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        while (true) {
                            cursor.next();
                        }
                    } catch (QueryCanceledException e) {
                        exited.set(true);
                    }
                }
            });
        queryThread.start();
        Thread.sleep(5 * 1000); // 5 sec
        session().cancelCurrentQuery(true);
        queryThread.join();
        assertTrue(exited.get());
    }

    @Test
    public void exitWithTimeout() throws InterruptedException
    {
        int timeoutSec = 3;
        configService().queryTimeoutSec(timeoutSec);
        final Operator plan = new DoNothingForever();
        final Cursor cursor = cursor(plan, queryContext);
        final AtomicBoolean exited = new AtomicBoolean(false);
        Thread queryThread = new Thread(
            new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        while (true) {
                            cursor.next();
                        }
                    } catch (QueryTimedOutException e) {
                        exited.set(true);
                    }
                }
            });
        long start = System.currentTimeMillis();
        queryThread.start();
        queryThread.join();
        long stop = System.currentTimeMillis();
        assertTrue(exited.get());
        long elapsedSec = (stop - start) / 1000;
        assertTrue(closeEnough(timeoutSec, elapsedSec));
    }

    @Test
    public void shortenedTimeout() throws InterruptedException
    {
        int INITIAL_TIMEOUT_SEC = 10000;
        int MODIFIED_TIMEOUT_SEC = 5;
        AkServer akServer = (AkServer) akServer();
        configService().queryTimeoutSec(INITIAL_TIMEOUT_SEC);
        final Operator plan = new DoNothingForever();
        final Cursor cursor = cursor(plan, queryContext);
        final AtomicBoolean exited = new AtomicBoolean(false);
        Thread queryThread = new Thread(
            new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        while (true) {
                            cursor.next();
                        }
                    } catch (QueryTimedOutException e) {
                        exited.set(true);
                    }
                }
            });
        long start = System.currentTimeMillis();
        queryThread.start();
        // Shorten timeout
        Thread.sleep(1000); // 1 sec
        configService().queryTimeoutSec(MODIFIED_TIMEOUT_SEC);
        queryThread.join();
        long stop = System.currentTimeMillis();
        assertTrue(exited.get());
        long elapsedSec = (stop - start) / 1000;
        assertTrue(closeEnough(MODIFIED_TIMEOUT_SEC, elapsedSec));
    }

    @Test
    public void removedTimeout() throws InterruptedException
    {
        int INITIAL_TIMEOUT_SEC = 5;
        int MODIFIED_TIMEOUT_SEC = -1; // No timeout
        AkServer akServer = (AkServer) akServer();
        configService().queryTimeoutSec(INITIAL_TIMEOUT_SEC);
        final Operator plan = new DoNothingForever();
        final Cursor cursor = cursor(plan, queryContext);
        final AtomicBoolean exited = new AtomicBoolean(false);
        Thread queryThread = new Thread(
            new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        while (true) {
                            cursor.next();
                        }
                    } catch (QueryCanceledException e) { // catches timeout too
                        exited.set(true);
                    }
                }
            });
        long start = System.currentTimeMillis();
        queryThread.start();
        // Remove timeout
        Thread.sleep(1000); // 1 sec
        configService().queryTimeoutSec(MODIFIED_TIMEOUT_SEC);
        Thread.sleep(INITIAL_TIMEOUT_SEC * 1000);
        session().cancelCurrentQuery(true);
        queryThread.join();
        long stop = System.currentTimeMillis();
        assertTrue(exited.get());
        long elapsedSec = (stop - start) / 1000;
        long expectedSec = INITIAL_TIMEOUT_SEC + 1;
        assertTrue(closeEnough(expectedSec, elapsedSec));
    }

    private boolean closeEnough(long expectedSec, long actualSec)
    {
        // A lower fudge factor is needed because the timeout starts from the time the QueryContext is created,
        // which is in the @Before method, slightly before the test's timer starts.
        return
            actualSec <= expectedSec * (1 + UPPER_FUDGE_FACTOR) &&
            actualSec >= expectedSec * (1 - LOWER_FUDGE_FACTOR);
    }
    
    private static final double UPPER_FUDGE_FACTOR = 1.0; // 100%
    private static final double LOWER_FUDGE_FACTOR = 0.2; // 20%

    private static class DoNothingForever extends Operator
    {
        // Operator interface

        @Override
        protected Cursor cursor(QueryContext context)
        {
            return new Execution(context);
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        private class Execution extends OperatorExecutionBase implements Cursor
        {
            // Cursor interface

            @Override
            public void open()
            {
            }

            @Override
            public Row next()
            {
                checkQueryCancelation();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    fail();
                }
                return null;
            }

            @Override
            public void close()
            {
            }

            // Execution interface

            Execution(QueryContext context)
            {
                super(context);
            }
        }
    }
}
