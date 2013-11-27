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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TimeMarker
{
    private final ListMultimap<Long,String> marks;

    public TimeMarker() {
        this(ArrayListMultimap.<Long,String>create());
    }

    public TimeMarker(ListMultimap<Long,String> marks) {
        this.marks = marks;
    }

    public void mark(String message) {
        long time = System.nanoTime();
        marks.put(time, message);
    }

    public ListMultimap<Long,String> getMarks() {
        return ImmutableListMultimap.copyOf(marks);
    }

    @Override
    public String toString() {
        return getMarks().toString();
    }
}
