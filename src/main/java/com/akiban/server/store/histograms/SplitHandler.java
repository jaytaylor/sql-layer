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

package com.akiban.server.store.histograms;

import com.akiban.util.Equality;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

abstract class SplitHandler<T> {

    protected abstract void handle(int segmentIndex, T input, int count);

    public void run(Iterator<? extends T> inputs) {
        int segments = splitter.segments();

        // init the buffers
        if (segments < 1)
            throw new IllegalStateException("splitter must provide at least 1 segment: " + segments);
        List<SegmentBuffer<T>> buffers = new ArrayList<SegmentBuffer<T>>(segments);
        for (int i = 0; i < segments; ++i) {
            buffers.add(new SegmentBuffer<T>());
        }

        while (inputs.hasNext()) {
            T input = inputs.next();
            List<? extends T> split = splitter.split(input);
            if (split.size() != segments)
                throw new IllegalStateException("required " + segments + ", found " + split.size() + ": " + split);
            for (int i = 0; i < segments; ++i) {
                T segment = split.get(i);
                SegmentBuffer<T> buffer = buffers.get(i);
                int count = buffer.put(segment);
                if (count > 0) {
                    handle(i, segment, count);
                }
            }
        }
        for (int i = 0; i < segments; ++i) {
            SegmentBuffer<T> buffer = buffers.get(i);
            T segment = buffer.last();
            int count = buffer.lastCount();
            handle(i, segment, count);
        }
    }

    public SplitHandler(Splitter<T> splitter) {
        this.splitter = splitter;
    }

    private final Splitter<T> splitter;

    private static class SegmentBuffer<T> {
        int put(T segment) {
            int count;
            if (lastCount == 0) {
                count = 0;
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
