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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.operator.*;
import com.foundationdb.qp.row.Row;
import com.foundationdb.sql.Main;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.error.QueryTimedOutException;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.foundationdb.qp.operator.API.cursor;
import java.util.Map;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryTimeoutIT extends OperatorITBase
{
    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
        session().cancelCurrentQuery(false);
    }

    @Test
    public void noExitWithDefaultTimeout() throws InterruptedException
    {
        final Operator plan = new DoNothingForever();
        final Cursor cursor = cursor(plan, queryContext, queryBindings);
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
        int timeoutMilli = 3000;
        configService().queryTimeoutMilli(timeoutMilli);
        final Operator plan = new DoNothingForever();
        final Cursor cursor = cursor(plan, queryContext, queryBindings);
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
        long elapsedMilli = (stop - start);
        assertTrue("Time: " + timeoutMilli + " Not equal to " + elapsedMilli, closeEnough(timeoutMilli, elapsedMilli));
    }

    @Test
    @Ignore
    public void shortenedTimeout() throws InterruptedException
    {
        int INITIAL_TIMEOUT_MILLI = 10000000;
        int MODIFIED_TIMEOUT_MILLI = 5000;
        Main layerInfo = (Main) layerInfo();
        configService().queryTimeoutMilli(INITIAL_TIMEOUT_MILLI);
        final Operator plan = new DoNothingForever();
        final Cursor cursor = cursor(plan, queryContext, queryBindings);
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
        configService().queryTimeoutMilli(MODIFIED_TIMEOUT_MILLI);
        queryThread.join();
        long stop = System.currentTimeMillis();
        assertTrue(exited.get());
        long elapsedMilli = (stop - start);
        assertTrue(closeEnough(MODIFIED_TIMEOUT_MILLI, elapsedMilli));
    }

    @Test
    @Ignore
    public void removedTimeout() throws InterruptedException
    {
        int INITIAL_TIMEOUT_MILLI = 5000;
        int MODIFIED_TIMEOUT_MILLI = -1; // No timeout
        Main layerInfo = (Main) layerInfo();
        configService().queryTimeoutMilli(INITIAL_TIMEOUT_MILLI);
        final Operator plan = new DoNothingForever();
        final Cursor cursor = cursor(plan, queryContext, queryBindings);
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
        configService().queryTimeoutMilli(MODIFIED_TIMEOUT_MILLI);
        Thread.sleep(INITIAL_TIMEOUT_MILLI);
        session().cancelCurrentQuery(true);
        queryThread.join();
        long stop = System.currentTimeMillis();
        assertTrue(exited.get());
        long elapsedMilli = (stop - start);
        long expectedMilli = INITIAL_TIMEOUT_MILLI + 1000;
        assertTrue(closeEnough(expectedMilli, elapsedMilli));
    }

    private boolean closeEnough(long expectedMilli, long actualMilli)
    {
        // A lower fudge factor is needed because the timeout starts from the time the QueryContext is created,
        // which is in the @Before method, slightly before the test's timer starts.
        return
            actualMilli <= expectedMilli * (1 + UPPER_FUDGE_FACTOR) &&
            actualMilli >= expectedMilli * (1 - LOWER_FUDGE_FACTOR);
    }
    
    private static final double UPPER_FUDGE_FACTOR = 1.0; // 100%
    private static final double LOWER_FUDGE_FACTOR = 0.2; // 20%

    private static class DoNothingForever extends Operator
    {
        // Operator interface

        @Override
        protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            return new Execution(context, bindingsCursor);
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        private class Execution extends LeafCursor
        {
            // Cursor interface

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

            // Execution interface

            Execution(QueryContext context,QueryBindingsCursor bindingsCursor)
            {
                super(context, bindingsCursor);
            }
        }
    }
}
