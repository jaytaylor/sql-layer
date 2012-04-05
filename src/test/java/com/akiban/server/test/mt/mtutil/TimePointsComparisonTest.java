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
