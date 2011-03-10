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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class Util {
    /**
     * Sleeps in a way that gets around code instrumentation which may end up shortening Thread.sleep.
     * If Thread.sleep throws an exception, this will invoke {@code Thread.currentThread().interrupt()} to propagate it.
     * @param millis how long to sleep
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
        private final T item;
        private final SortedMap<Long,String> timePoints;

        private TimedResult(T item, TimePoints timePoints) {
            this.item = item;
            this.timePoints = new TreeMap<Long, String>(timePoints.getMarks());
        }

        public long getTime() {
            assertFalse("can't time; no marks set", timePoints.isEmpty());
            final long start = this.timePoints.firstKey();
            final long end = this.timePoints.lastKey();
            return end - start;
        }

        public T getItem() {
            return item;
        }
    }

    public static class TimePoints {
        private final Map<Long,String> marks;

        public TimePoints() {
            this.marks = new HashMap<Long, String>();
        }

        public void mark(String message) {
            long time = System.currentTimeMillis();
            marks.put(time, message);
        }

        Map<Long,String> getMarks() {
            return marks;
        }
    }

    public static class TimePointsComparison {
        private final SortedMap<Long,String> marks;

        public TimePointsComparison(TimePoints... timePointsArray) {
            marks = new TreeMap<Long, String>();
            for (TimePoints timePoints : timePointsArray) {
                marks.putAll(timePoints.getMarks());
            }
        }

        public TimePointsComparison(TimedResult<?>... timedResults) {
            marks = new TreeMap<Long, String>();
            for (TimedResult<?> timePoints : timedResults) {
                marks.putAll(timePoints.timePoints);
            }
        }

        public void verify(String... expectedMessages) {
            List<String> expected = Arrays.asList(expectedMessages);
            List<String> actual = new ArrayList<String>( marks.values() );
            assertEquals("timepoint messages (in order)", expected, actual);
        }
    }

    public abstract static class TimedCallable<T> implements Callable<TimedResult<T>> {
        protected abstract T doCall(TimePoints timePoints) throws Exception;

        @Override
        public final TimedResult<T> call() throws Exception {
            TimePoints timePoints = new TimePoints();
            T result = doCall(timePoints);
            return new TimedResult<T>(result, timePoints);
        }
    }
}
