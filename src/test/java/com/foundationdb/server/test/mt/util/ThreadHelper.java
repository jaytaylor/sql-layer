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

package com.foundationdb.server.test.mt.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class ThreadHelper
{
    private static final long DEFAULT_TIMEOUT_MILLIS = 5 * 1000;

    public static class UncaughtHandler implements Thread.UncaughtExceptionHandler {
        public final Map<Thread, Throwable> thrown = Collections.synchronizedMap(new HashMap<Thread, Throwable>());

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            thrown.put(t, e);
        }

        @Override
        public String toString() {
            Map<String,String> nameToError = new TreeMap<>();
            for(Entry<Thread, Throwable> entry : thrown.entrySet()) {
                nameToError.put(entry.getKey().getName(), entry.getValue().getMessage());
            }
            return nameToError.toString();
        }
    }


    public static UncaughtHandler start(Thread... threads) {
        return start(Arrays.asList(threads));
    }

    public static void join(UncaughtHandler handler, long timeoutMillis, Thread... threads) {
        join(handler, timeoutMillis, Arrays.asList(threads));
    }

    public static UncaughtHandler startAndJoin(long timeoutMillis, Thread... threads) {
        return startAndJoin(timeoutMillis, Arrays.asList(threads));
    }
    public static void runAndCheck(Thread... threads) {
        runAndCheck(Arrays.asList(threads));
    }

    public static void runAndCheck(long timeoutMillis, Thread... threads) {
        runAndCheck(timeoutMillis, Arrays.asList(threads));
    }


    public static UncaughtHandler start(Collection<? extends Thread> threads) {
        UncaughtHandler handler = new UncaughtHandler();
        for(Thread t : threads) {
            t.setUncaughtExceptionHandler(handler);
            t.start();
        }
        return handler;
    }

    public static void join(UncaughtHandler handler, long timeoutMillis, Collection<? extends Thread> threads) {
        for(Thread t : threads) {
            Throwable error = null;
            try {
                t.join(timeoutMillis);
                if(t.isAlive()) {
                    error = new AssertionError("Thread did not complete in timeout: " + timeoutMillis);
                    t.interrupt();
                }
            } catch(InterruptedException e) {
                error = e;
            }
            if(error != null) {
                handler.uncaughtException(t, error);
            }
        }
    }

    public static UncaughtHandler startAndJoin(Collection<? extends Thread> threads) {
        return startAndJoin(DEFAULT_TIMEOUT_MILLIS, threads);
    }

    public static UncaughtHandler startAndJoin(long timeoutMillis, Collection<? extends Thread> threads) {
        UncaughtHandler handler = start(threads);
        join(handler, timeoutMillis, threads);
        return handler;
    }

    public static void runAndCheck(Collection<? extends Thread> threads) {
        runAndCheck(DEFAULT_TIMEOUT_MILLIS, threads);
    }

    public static void runAndCheck(long timeoutMillis, Collection<? extends Thread> threads) {
        UncaughtHandler handler = startAndJoin(timeoutMillis, threads);
        assertEquals("Thread errors", new UncaughtHandler().toString(), handler.toString());
    }
}
