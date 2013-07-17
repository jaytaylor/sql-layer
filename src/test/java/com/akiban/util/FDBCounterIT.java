/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.util;

import com.akiban.server.store.FDBHolder;
import com.akiban.server.test.it.ITBase;
import com.foundationdb.Database;
import com.foundationdb.FDBError;
import com.foundationdb.Retryable;
import com.foundationdb.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;

public class FDBCounterIT extends ITBase {
    private static final String KEY_PREFIX = "test";

    private FDBHolder holder;
    private FDBCounter counter;

    @Before
    public void createCounter() {
        holder = serviceManager().getServiceByClass(FDBHolder.class);
        counter = new FDBCounter(holder.getDatabase(), KEY_PREFIX);
    }

    @After
    public void clearCounter() throws Throwable {
        holder.getDatabase().run(new Retryable() {
            @Override
            public void attempt(Transaction tr) {
                counter.clearState(tr);
            }
        });
    }

    @Test
    public void getTransactionalEmpty() throws Throwable {
        long value = getTransactional();
        assertEquals("value of empty", 0, value);
    }

    @Test
    public void getSnapshotEmpty() throws Throwable {
        long value = getSnapshot();
        assertEquals("value of empty", 0, value);
    }

    @Test
    public void addOneAndGet() throws Throwable {
        addSome(1);
        long value = getTransactional();
        assertEquals("value after add", 1, value);
    }

    @Test
    public void addOneTenTimesAndGet() throws Throwable {
        for(int i = 0; i < 10; ++i) {
            addSome(1);
        }
        long value = getTransactional();
        assertEquals("value after add", 10, value);
    }

    @Test
    public void addTenAndGet() throws Throwable {
        addSome(10);
        long value = getTransactional();
        assertEquals("value after add", 10, value);
    }

    @Test
    public void addSubtractSetAndGet() throws Throwable {
        for(int i = 0; i <= 100; ++i) {
            addSome(i);
        }
        assertEquals("value after add 0..100", 5050, getTransactional());

        for(int i = 0; i <= 50; ++i) {
            addSome(-i);
        }
        assertEquals("value after sub 0..50", 3775, getTransactional());

        setTo(42);
        assertEquals("value after set 42", 42, getTransactional());
    }

    // Manual only
    @Ignore
    @Test
    public void manyThreads() throws Throwable {
        // 50 threads adding 1 or -1, 1000 times in a row
        final int THREADS = 50;
        final int TXN_COUNT = 1000;

        boolean inc = true;
        WorkerThread[] threads = new WorkerThread[THREADS];
        for(int i = 0; i < threads.length; ++i) {
            threads[i] = new WorkerThread(holder.getDatabase(), counter, TXN_COUNT, inc ? 1 : -1);
            inc = !inc;
        }

        long startMillis = System.currentTimeMillis();
        for(WorkerThread t : threads) {
            t.start();
        }
        int retries;
        int failures;
        for(;;) {
            retries = 0;
            failures = 0;
            try {
                for(WorkerThread t : threads) {
                    t.join();
                    retries += t.retryCount;
                    failures += t.failureCount;
                }
                break;
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
        long endMillis = System.currentTimeMillis();

        long elapsed = endMillis - startMillis;
        long total = getTransactional();
        System.out.println(" elapsed: " + elapsed);
        System.out.println(" retries: " + retries);
        System.out.println("failures: " + failures);
        System.out.println("   total: " + total);
        assertEquals("final total", 0, total);
    }


    private void addSome(final int x) throws Throwable {
        holder.getDatabase().run(new Retryable() {
            @Override
            public void attempt(Transaction tr) throws Exception {
                counter.add(tr, x);
            }
        });
    }

    private void setTo(final int x) throws Throwable {
        holder.getDatabase().run(new Retryable() {
            @Override
            public void attempt(Transaction tr) throws Exception {
                counter.set(tr, x);
            }
        });
    }

    private long getTransactional() throws Throwable {
        return get(true);
    }

    private long getSnapshot() throws Throwable {
        return get(false);
    }

    private long get(final boolean transactional) throws Throwable {
        final long[] value = { 0 };
        holder.getDatabase().run(new Retryable() {
            @Override
            public void attempt(Transaction tr) throws Exception {
                value[0] = transactional ? counter.getTransactional(tr) : counter.getSnapshot(tr);
            }
        });
        return value[0];
    }


    private static class WorkerThread extends Thread {
        private final Database db;
        private final FDBCounter counter;
        private final int txnCount;
        private final int inc;
        private volatile int retryCount;
        private volatile int failureCount;

        public WorkerThread(Database db, FDBCounter counter, int txnCount, int inc) {
            this.db = db;
            this.counter = counter;
            this.txnCount = txnCount;
            this.inc = inc;
        }

        @Override
        public void run() {
            int retries = 0;
            int failures = 0;
            for(int t = 0; t < txnCount; ++t) {
                for(int retry = 0; ; ++retry) {
                    if(retry == 10) {
                        ++failures;
                        break;
                    }
                    Transaction tr = db.createTransaction();
                    counter.add(tr, inc);
                    try {
                        tr.commit().get();
                        break;
                    } catch(FDBError e) {
                        ++retries;
                    }
                }
            }
            this.retryCount = retries;
            this.failureCount = failures;
        }
    }
}
