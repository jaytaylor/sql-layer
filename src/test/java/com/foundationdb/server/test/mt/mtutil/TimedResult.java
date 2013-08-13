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

package com.foundationdb.server.test.mt.mtutil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class TimedResult<T> {

    static class TimeMarks  {
        private final Map<Long,List<String>> marks;

        TimeMarks(Map<Long, List<String>> marks) {
            this.marks = marks;
        }

        Map<Long,List<String>> getMarks() {
            return marks;
        }

        @Override
        public String toString() {
            return getMarks().toString();
        }
    }

    private final T item;
    private final TimeMarks timePoints;

    TimedResult(T item, TimePoints timePoints) {
        this.item = item;
        this.timePoints = new TimeMarks(timePoints.getMarks());
    }

    @SuppressWarnings("unused") // useful for debugging
    public static TimedResult<Void> ofNull(TimePoints timePoints) {
        return new TimedResult<>(null, timePoints);
    }

    public T getItem() {
        return item;
    }

    TimeMarks timePoints() {
        return timePoints;
    }

    @Override
    public String toString() {
        return toString( getLowestTimeStamp() );
    }

    private Long getLowestTimeStamp() {
        return Collections.min(timePoints().getMarks().keySet());
    }

    private String toString(long baseTimestamp) {
        TreeMap<Long,List<String>> marksSorted = new TreeMap<>();
        for (Map.Entry<Long,List<String>> entry :  timePoints().getMarks().entrySet()) {
            marksSorted.put(entry.getKey() - baseTimestamp, entry.getValue());
        }

        return String.format("TimedResult<base=%d {%s} => %s>", baseTimestamp, marksSorted, getItem());
    }

    public static String toString(TimedResult... timedResults) {
        TreeMap<Long,TimedResult> resultsByLowest = new TreeMap<>();

        for (TimedResult timedResult : timedResults) {
            resultsByLowest.put(timedResult.getLowestTimeStamp(), timedResult);
        }

        long lowest = resultsByLowest.firstKey();
        List<String> strings = new ArrayList<>();
        for (TimedResult timedResult : resultsByLowest.values()) {
            strings.add( timedResult.toString(lowest) );
        }
        return strings.toString();
    }
}
