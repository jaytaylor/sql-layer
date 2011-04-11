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

package com.akiban.server.test.mt.mtutil;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public final class TimePointsComparisonTest {
    @Test
    public void noTimestampConflicts() {
        Map<Long,List<String>> alpha = new HashMap<Long, List<String>>();
        alpha.put(1L, Arrays.asList("one"));
        alpha.put(3L, Arrays.asList("three"));

        Map<Long,List<String>> beta = new HashMap<Long, List<String>>();
        beta.put(2L, Arrays.asList("two"));

        test("[[one], [two], [three]]", alpha, beta);
    }

    @Test
    public void conflictsWithinOne() {
        Map<Long,List<String>> alpha = new HashMap<Long, List<String>>();
        alpha.put(1L, Arrays.asList("oneA", "oneB"));

        Map<Long,List<String>> beta = new HashMap<Long, List<String>>();
        beta.put(2L, Arrays.asList("two"));

        test("[[oneA], [oneB], [two]]", alpha, beta);
    }

    @Test
    public void conflictsAcrossTwo() {
        Map<Long,List<String>> alpha = new HashMap<Long, List<String>>();
        alpha.put(1L, Arrays.asList("one"));
        alpha.put(2L, Arrays.asList("twoA"));

        Map<Long,List<String>> beta = new HashMap<Long, List<String>>();
        beta.put(2L, Arrays.asList("twoB"));

        List<Collection<String>> marks = marks(alpha, beta);
        assertEquals("marks.size", 2, marks.size());
        assertEquals("marks[0]", Arrays.asList("one"), marks.get(0));
        assertEquals("marks[1]", set("twoA", "twoB"), set(marks.get(1)));
    }

    @Test
    public void multipleConflicts() {
        Map<Long,List<String>> alpha = new HashMap<Long, List<String>>();
        alpha.put(1L, Arrays.asList("oneA", "oneB"));

        Map<Long,List<String>> beta = new HashMap<Long, List<String>>();
        beta.put(1L, Arrays.asList("twoA", "twoB"));

        List<Collection<String>> marks = marks(alpha, beta);
        assertEquals("marks.size", 2, marks.size());
        assertEquals("marks[0]", set("oneA", "twoA"), set(marks.get(0)));
        assertEquals("marks[1]", set("oneB", "twoB"), set(marks.get(1)));
    }

    private static List<Collection<String>> marks(Map<Long,List<String>>... markMaps) {
        List<TimedResult.TimeMarks> timeMarks = new ArrayList<TimedResult.TimeMarks>();
        for (Map<Long,List<String>> marksMap : markMaps) {
            timeMarks.add(new TimedResult.TimeMarks(marksMap));
        }
        return TimePointsComparison.compileMarks(timeMarks);

    }

    private static Set<String> set(String... strings) {
        return set(Arrays.asList(strings));
    }

    private static Set<String> set(Collection<String> strings) {
        Set<String> set = new HashSet<String>(strings);
        assertEquals("set size", strings.size(), set.size());
        return set;
    }

    private static String marksToString(Map<Long,List<String>>[] markMaps) {
        return marks(markMaps).toString();
    }

    private static void test(String expected, Map<Long,List<String>>... markMaps) {
        assertEquals("flattened list", expected, marksToString(markMaps));
    }
}
