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
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertFalse;

public final class TimedResult<T> {
    private final T item;
    private final SortedMap<Long,List<String>> timePoints;

    TimedResult(T item, TimePoints timePoints) {
        this.item = item;
        this.timePoints = new TreeMap<Long, List<String>>(timePoints.getMarks());
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

    SortedMap<Long,List<String>> timePoints() {
        return timePoints;
    }
}
