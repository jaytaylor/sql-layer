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

package com.akiban.server.test.mt.mtutil;

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
        return new TimedResult<Void>(null, timePoints);
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
        TreeMap<Long,List<String>> marksSorted = new TreeMap<Long, List<String>>();
        for (Map.Entry<Long,List<String>> entry :  timePoints().getMarks().entrySet()) {
            marksSorted.put(entry.getKey() - baseTimestamp, entry.getValue());
        }

        return String.format("TimedResult<base=%d {%s} => %s>", baseTimestamp, marksSorted, getItem());
    }

    public static String toString(TimedResult... timedResults) {
        TreeMap<Long,TimedResult> resultsByLowest = new TreeMap<Long, TimedResult>();

        for (TimedResult timedResult : timedResults) {
            resultsByLowest.put(timedResult.getLowestTimeStamp(), timedResult);
        }

        long lowest = resultsByLowest.firstKey();
        List<String> strings = new ArrayList<String>();
        for (TimedResult timedResult : resultsByLowest.values()) {
            strings.add( timedResult.toString(lowest) );
        }
        return strings.toString();
    }
}
