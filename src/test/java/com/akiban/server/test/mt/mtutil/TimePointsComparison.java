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

import org.junit.ComparisonFailure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public final class TimePointsComparison {
    private final List<Collection<String>> marks;

    public TimePointsComparison(TimedResult<?>... timedResults) {
        List<TimedResult.TimeMarks> timeMarks = new ArrayList<TimedResult.TimeMarks>();
        for (TimedResult<?> timedResult : timedResults) {
            timeMarks.add(timedResult.timePoints());
        }
        marks = compileMarks(timeMarks);
    }

    /**
     * <p>Creates a list which describes the time points described across a collection of TimeMarks.</p>
     *
     * <p>Each TimeMark contains a map from timestamp to a list of messages. It is assumed that each TimeMark is generated
     * from a single thread, so 1 -> [A, B] means that A and B actually came at different times, and in that
     * order. On the other hand, if TimeMark Alpha contains 1 -> [A] and TimeMark Beta contains 1 -> [B], we
     * can't establish an order and assume they happened at the same time.</p>
     *
     * <p>With that in mind, each collection of the returned list contains, roughly speaking, the message(s) for a
     * unique time. In the case of a single TimeMark {@code 1 -> [A, B]}, this would be: {@code [ {A}, {B} ]}.
     * In the case of our Alpha and Bravo TimeMarks, this would be: {@code [ {A, B} ]}.</p>
     *
     * <p>For the resulting list to be useful, each {@code Collection<String>} in the resulting List must have exactly
     * one element. If any collections have multiple elements, we can't assess the order in which things happened.</p>
     *
     * <p>The "roughly speaking" caveat above is that if two TimeMarks both contain the same timestamp with multiple
     * messages, we'll assume each pair happened together:
     * <pre>
     *     Alpha: 1 -> [A, B]
     *     Beta:  1 -> [C, D]
     *     Compiled: [ {A, C}, {B, D} ]
     * </pre>
     * This pairing may be wrong, of course, but the essential information -- that we can't uniquely place each
     * message relative to all other messages -- is still maintained.</p>
     * @param timeMarkses the collection of TimeMarks to compile into one list
     * @return see description
     */
    static List<Collection<String>> compileMarks(Collection<TimedResult.TimeMarks> timeMarkses) {
        Map<Long,List<List<String>>> all = new TreeMap<Long, List<List<String>>>();

        for (TimedResult.TimeMarks timeMarks : timeMarkses) {
            for (Map.Entry<Long,List<String>> marksEntry : timeMarks.getMarks().entrySet()) {
                Long timestamp = marksEntry.getKey();
                List<String> marks = marksEntry.getValue();
                if (!all.containsKey(timestamp)) {
                    all.put(timestamp, new ArrayList<List<String>>());
                }
                List<List<String>> listsList = all.get(timestamp);
                int i = 0;
                for (String mark : marks) {
                    if (listsList.size() <= i) {
                        List<String> single = new ArrayList<String>();
                        single.add(mark);
                        listsList.add(single);
                    }
                    else {
                        List<String> conflicted = listsList.get(i);
                        conflicted.add(mark);
                    }
                    ++i;
                }
            }
        }

        List<Collection<String>> compiled = new ArrayList<Collection<String>>();
        for (List<List<String>> segment : all.values()) {
            compiled.addAll(segment);
        }
        return compiled;
    }

    public void verify(String... expectedMessages) {
        List<Collection<String>> expected = new ArrayList<Collection<String>>();
        for (String expectedMessage : expectedMessages) {
            expected.add(Collections.singletonList(expectedMessage));
        }
        List<Collection<String>> actual = marks;
        // For pretty print
        if(!expected.equals(actual)) {
            throw new ComparisonFailure("timepoint messages (in order)", expected.toString(), actual.toString());
        }
    }

    public List<String> getMarkNames() {
        List<String> markNames = new ArrayList<String>();
        for (Collection<String> marksList : marks) {
            assertEquals("individual marks lists must be singletons; size ", 1, marksList.size());
            markNames.addAll(marksList);
        }
        return markNames;
    }

    public boolean startsWith(String... expectedMessages) {
        List<String> expected = Arrays.asList(expectedMessages);
        List<String> actual = getMarkNames();

        if (actual.size() < expected.size()) {
            return false;
        }
        actual = actual.subList(0, expected.size());

        return expected.equals(actual);
    }

    public boolean matches(String... expectedMessages) {
        List<String> expected = Arrays.asList(expectedMessages);
        List<String> actual = getMarkNames();
        return expected.equals(actual);
    }

    @Override
    public String toString() {
        return marks.toString();
    }
}
