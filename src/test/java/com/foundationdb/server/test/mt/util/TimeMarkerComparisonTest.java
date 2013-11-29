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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class TimeMarkerComparisonTest
{
    @Test
    public void noTimestampConflicts() {
        ListMultimap<Long,String> alpha = newMultimap();
        alpha.put(1L, "one");
        alpha.put(3L, "three");
        ListMultimap<Long,String> beta = newMultimap();
        beta.put(2L, "two");
        test("[[one], [two], [three]]", alpha, beta);
    }

    @Test
    public void conflictsWithinOne() {
        ListMultimap<Long,String> alpha = newMultimap();
        alpha.put(1L, "oneA");
        alpha.put(1L, "oneB");
        ListMultimap<Long,String> beta = newMultimap();
        beta.put(2L, "two");
        test("[[oneA], [oneB], [two]]", alpha, beta);
    }

    @Test
    public void conflictsAcrossTwo() {
        ListMultimap<Long,String> alpha = newMultimap();
        alpha.put(1L, "one");
        alpha.put(2L, "twoA");
        ListMultimap<Long,String> beta = newMultimap();
        beta.put(2L, "twoB");
        test("[[one], [twoA, twoB]]", alpha, beta);
    }

    @Test
    public void multipleConflicts() {
        ListMultimap<Long,String> alpha = newMultimap();
        alpha.put(1L, "oneXA");
        alpha.put(1L, "oneYA");
        ListMultimap<Long,String> beta = newMultimap();
        beta.put(1L, "oneYB");
        beta.put(1L, "oneXB");
        test("[[oneXA, oneYB], [oneXB, oneYA]]", alpha, beta);
    }

    private static <K,V> ListMultimap<K,V> newMultimap() {
        return ArrayListMultimap.create();
    }

    private static List<List<String>> combine(ListMultimap<Long, String> a, ListMultimap<Long, String> b) {
        return TimeMarkerComparison.combineTimeMarkers(Arrays.asList(new TimeMarker(a), new TimeMarker(b)));
    }

    private static void test(String expected, ListMultimap<Long,String> a, ListMultimap<Long,String> b) {
        assertEquals("flattened list", expected, combine(a, b).toString());
    }
}
