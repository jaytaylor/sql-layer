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

import java.util.List;
import java.util.Map;

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

    public T getItem() {
        return item;
    }

    TimeMarks timePoints() {
        return timePoints;
    }
}
