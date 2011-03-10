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

package com.akiban.server.mttests.mtutil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public final class TimePointsComparison {
    private final SortedMap<Long,String> marks;

    public TimePointsComparison(TimedResult<?>... timedResults) {
        marks = new TreeMap<Long, String>();
        for (TimedResult<?> timePoints : timedResults) {
            marks.putAll(timePoints.timePoints());
        }
    }

    public void verify(String... expectedMessages) {
        List<String> expected = Arrays.asList(expectedMessages);
        List<String> actual = new ArrayList<String>( marks.values() );
        assertEquals("timepoint messages (in order)", expected, actual);
    }
}
