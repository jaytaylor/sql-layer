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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TimePoints {
    private final Map<Long,List<String>> marks = new HashMap<Long, List<String>>();

    public void mark(String message) {
        long time = System.currentTimeMillis();
        List<String> nowMarks = marks.get(time);
        if (nowMarks == null) {
            nowMarks = new ArrayList<String>();
            marks.put(time, nowMarks);
        }
        nowMarks.add(message);
    }

    Map<Long,List<String>> getMarks() {
        return marks;
    }
}
