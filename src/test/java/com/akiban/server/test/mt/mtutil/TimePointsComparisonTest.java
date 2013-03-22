
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
        Map<Long,List<String>> alpha = new HashMap<>();
        alpha.put(1L, Arrays.asList("one"));
        alpha.put(3L, Arrays.asList("three"));

        Map<Long,List<String>> beta = new HashMap<>();
        beta.put(2L, Arrays.asList("two"));

        test("[[one], [two], [three]]", alpha, beta);
    }

    @Test
    public void conflictsWithinOne() {
        Map<Long,List<String>> alpha = new HashMap<>();
        alpha.put(1L, Arrays.asList("oneA", "oneB"));

        Map<Long,List<String>> beta = new HashMap<>();
        beta.put(2L, Arrays.asList("two"));

        test("[[oneA], [oneB], [two]]", alpha, beta);
    }

    @Test
    public void conflictsAcrossTwo() {
        Map<Long,List<String>> alpha = new HashMap<>();
        alpha.put(1L, Arrays.asList("one"));
        alpha.put(2L, Arrays.asList("twoA"));

        Map<Long,List<String>> beta = new HashMap<>();
        beta.put(2L, Arrays.asList("twoB"));

        List<Collection<String>> marks = marks(alpha, beta);
        assertEquals("marks.size", 2, marks.size());
        assertEquals("marks[0]", Arrays.asList("one"), marks.get(0));
        assertEquals("marks[1]", set("twoA", "twoB"), set(marks.get(1)));
    }

    @Test
    public void multipleConflicts() {
        Map<Long,List<String>> alpha = new HashMap<>();
        alpha.put(1L, Arrays.asList("oneA", "oneB"));

        Map<Long,List<String>> beta = new HashMap<>();
        beta.put(1L, Arrays.asList("twoA", "twoB"));

        List<Collection<String>> marks = marks(alpha, beta);
        assertEquals("marks.size", 2, marks.size());
        assertEquals("marks[0]", set("oneA", "twoA"), set(marks.get(0)));
        assertEquals("marks[1]", set("oneB", "twoB"), set(marks.get(1)));
    }

    private static List<Collection<String>> marks(Map<Long,List<String>>... markMaps) {
        List<TimedResult.TimeMarks> timeMarks = new ArrayList<>();
        for (Map<Long,List<String>> marksMap : markMaps) {
            timeMarks.add(new TimedResult.TimeMarks(marksMap));
        }
        return TimePointsComparison.compileMarks(timeMarks);

    }

    private static Set<String> set(String... strings) {
        return set(Arrays.asList(strings));
    }

    private static Set<String> set(Collection<String> strings) {
        Set<String> set = new HashSet<>(strings);
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
