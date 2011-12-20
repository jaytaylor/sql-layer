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

import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class FairRandomTieBreakerTest {
    @Test
    public void testDistribution() {
        int iterations = 10000;
        int max = 100;
        Map<Integer,Long> distributions = new TreeMap<Integer, Long>();
        for (int i = 0; i < max; ++i) {
            distributions.put(i, 0L);
        }
        for (int iteration = 0; iteration < iterations; ++iteration) {
            List<Bucket<Integer>> buckets = Buckets.compile(new Range(max), 31);
//            assertEquals("buckets size", 31, buckets.size());
            for (Bucket<Integer> bucket : buckets) {
                put(distributions, bucket.value());
            }
        }
        for (long hits : distributions.values()) {
            System.out.println(hits);
        }
        fail(distributions.toString());
    }

    private void put(Map<Integer,Long> map, Integer key) {
        Long previous = map.get(key);
        previous = previous == null ? 1 : previous + 1;
        map.put(key, previous);
    }

    private static class Range implements Iterable<Integer> {

        @Override
        public Iterator<Integer> iterator() {
            return new InternalIterator(max);
        }

        private Range(int max) {
            this.max = max;
        }

        private final int max;

        private static class InternalIterator implements Iterator<Integer> {

            @Override
            public boolean hasNext() {
                return value < max;
            }

            @Override
            public Integer next() {
                return value++;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private InternalIterator(int max) {
                this.max = max;
            }

            private final int max;
            private int value;
        }
    }
}
