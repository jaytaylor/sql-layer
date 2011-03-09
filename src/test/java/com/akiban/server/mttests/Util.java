/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.mttests;

import java.util.concurrent.Callable;

public final class Util {
    /**
     * Sleeps in a way that gets around code instrumentation which may end up shortening Thread.sleep.
     * If Thread.sleep throws an exception, this will invoke {@code Thread.currentThread().interrupt()} to propagate it.
     * @param millis
     */
    public static void sleep(long millis) {
        final long start = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } while (System.currentTimeMillis() - start < millis);
    }

    public static class TimedResult<T> {
        private final long time;
        private final T item;

        private TimedResult(long time, T item) {
            this.time = time;
            this.item = item;
        }

        public long getTime() {
            return time;
        }

        public T getItem() {
            return item;
        }
    }

    public abstract static class TimedCallable<T> implements Callable<TimedResult<T>> {
        protected abstract T doCall() throws Exception;

        @Override
        public final TimedResult<T> call() throws Exception {
            final long start = System.currentTimeMillis();
            T result = doCall();
            return new TimedResult<T>(System.currentTimeMillis() - start, result);
        }
    }
}
