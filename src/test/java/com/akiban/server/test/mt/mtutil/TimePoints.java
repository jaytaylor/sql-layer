
package com.akiban.server.test.mt.mtutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TimePoints {
    private final Map<Long,List<String>> marks = new HashMap<>();

    public void mark(String message) {
        long time = System.currentTimeMillis();
        List<String> nowMarks = marks.get(time);
        if (nowMarks == null) {
            nowMarks = new ArrayList<>();
            marks.put(time, nowMarks);
        }
        nowMarks.add(message);
    }

    Map<Long,List<String>> getMarks() {
        return marks;
    }
}
