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

import java.util.HashMap;
import java.util.Map;

public final class TimePoints {
    private final Map<Long,String> marks;

    public TimePoints() {
        this.marks = new HashMap<Long, String>();
    }

    public void mark(String message) {
        long time = System.currentTimeMillis();
        marks.put(time, message);
    }

    Map<Long,String> getMarks() {
        return marks;
    }
}
