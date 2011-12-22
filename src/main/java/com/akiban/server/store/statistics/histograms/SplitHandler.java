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

package com.akiban.server.store.statistics.histograms;

import com.akiban.util.Equality;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

abstract class SplitHandler<T> implements SampleVisitor<T> {

    protected abstract void handle(int segmentIndex, T input, int count);

    @Override
    public void init() {
        buffers = new ArrayList<SegmentBuffer<T>>(segments);
        for (int i = 0; i < segments; ++i) {
            buffers.add(new SegmentBuffer<T>());
        }
    }

    @Override
    public void finish() {
        checkInit();
        for (int i = 0; i < segments; ++i) {
            SegmentBuffer<T> buffer = buffers.get(i);
            T segment = buffer.last();
            int count = buffer.lastCount();
            if (count > 0)
                handle(i, segment, count);
        }
    }

    @Override
    public void visit(T input) {
        checkInit();
        List<? extends T> split = splitter.split(input);
        if (split.size() != segments)
            throw new IllegalStateException("required " + segments + ", found " + split.size() + ": " + split);
        for (int i = 0; i < segments; ++i) {
            T segment = split.get(i);
            SegmentBuffer<T> buffer = buffers.get(i);
            T prev = buffer.last();
            int count = buffer.put(segment);
            if (count > 0) {
                handle(i, prev, count);
            }
        }
    }

    public SplitHandler(Splitter<T> splitter) {
        this.splitter = splitter;
        this.segments = splitter.segments();
        if (segments < 1)
            throw new IllegalArgumentException("splitter must provide at least 1 segment: " + segments);
    }

    private void checkInit() {
        if (buffers == null)
            throw new IllegalStateException("not initialized");
    }

    private final Splitter<T> splitter;
    private final int segments;
    private List<SegmentBuffer<T>> buffers;

    private static class SegmentBuffer<T> {
        int put(T segment) {
            int count;
            if (lastCount == 0) {
                // first element
                count = 0;
                lastCount = 1;
                last = segment;
            }
            else if (Equality.areEqual(last, segment)) {
                // same segment, just update lastCount
                ++lastCount;
                count = 0;
            } else {
                // new segment. Return and reset lastCount, and reset last
                count = lastCount;
                lastCount = 1;
                last = segment;
            }
            return count;
        }

        T last() {

            return last;
        }
        
        int lastCount() {
            return lastCount;
        }

        @Override
        public String toString() {
            return (lastCount == 0)
                    ? "SegmentBuffer(last=" + last + ')'
                    : "SegmentBuffer(FRESH)";
        }

        T last;
        int lastCount = 0;
    }
}
