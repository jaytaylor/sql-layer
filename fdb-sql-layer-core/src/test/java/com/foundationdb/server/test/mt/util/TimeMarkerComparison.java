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

import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import org.junit.ComparisonFailure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public final class TimeMarkerComparison
{
    private final List<List<String>> combinedMarks;

    public TimeMarkerComparison(Collection<? extends HasTimeMarker> hasTimeMarkers) {
        List<TimeMarker> timeMarkers = new ArrayList<>();
        for(HasTimeMarker htm : hasTimeMarkers) {
            timeMarkers.add(htm.getTimeMarker());
        }
        combinedMarks = combineTimeMarkers(timeMarkers);
    }

    /**
     * Combine multiple TimeMarkers into a single, ordered list of marks.
     *
     * <p>
     * Final output is a list of messages in the order they occurred. If the
     * order is unknown (i.e. overlapping timestamps from multiple markers)
     * then multiple messages will be in a single position and sorted by name.
     * </p>
     *
     * <pre>
     * Input:
     *     A: 1 -> [baz,hop], 2 -> [cap], 3 -> [dig]
     *     B: 1 -> [gap,foo],             3 -> [jog]  4 -> [zap]
     * Output:
     *     [ [baz,gap], [foo,hop], [cap], [dig,jog], [zap] ]
     * </pre>
     */
    static List<List<String>> combineTimeMarkers(TimeMarker... timeMarkers) {
        return combineTimeMarkers(Arrays.asList(timeMarkers));
    }

    /** See {@link #combineTimeMarkers(TimeMarker...)}. */
    static List<List<String>> combineTimeMarkers(Collection<TimeMarker> timeMarkers) {
        // Sort by time, list of lists of messages from all markers
        ListMultimap<Long,List<String>> timeToMarks = Multimaps.newListMultimap(
            new TreeMap<Long, Collection<List<String>>>(),
            new Supplier<List<List<String>>>() {
                @Override
                public List<List<String>> get() {
                    return new ArrayList<>();
                }
            }
        );
        for(TimeMarker tm : timeMarkers) {
            for(Entry<Long, List<String>> entry : Multimaps.asMap(tm.getMarks()).entrySet()) {
                timeToMarks.put(entry.getKey(), entry.getValue());
            }
        }
        // Concatenate into final ordering
        List<List<String>> output = new ArrayList<>();
        for(List<List<String>> singleTimeMarks : Multimaps.asMap(timeToMarks).values()) {
            // Split a given Marker's messages apart, coalesce overlapping timestamps from multiple Marker's
            ListMultimap<Integer,String> split = ArrayListMultimap.create();
            for(List<String> marks : singleTimeMarks) {
                for(int i = 0; i < marks.size(); ++i) {
                    split.put(i, marks.get(i));
                }
            }
            // Sort any coalesced and output
            for(List<String> marks : Multimaps.asMap(split).values()) {
                Collections.sort(marks);
                output.add(marks);
            }
        }
        return output;
    }

    public void verify(String... expectedMessages) {
        List<Collection<String>> expected = new ArrayList<>();
        for (String expectedMessage : expectedMessages) {
            if(expectedMessage != null) {
                expected.add(Collections.singletonList(expectedMessage));
            }
        }
        // For pretty print
        if(!expected.equals(combinedMarks)) {
            throw new ComparisonFailure("TimePoint messages (in order)", expected.toString(), combinedMarks.toString());
        }
    }

    public List<String> getMarkNames() {
        List<String> markNames = new ArrayList<>();
        for (Collection<String> marksList : combinedMarks) {
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
        return combinedMarks.toString();
    }
}
