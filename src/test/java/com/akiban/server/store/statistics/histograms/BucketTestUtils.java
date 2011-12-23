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

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

final class BucketTestUtils {
    public static <T> Bucket<T> bucket(T value, long equals, long lessThans, long lessThanDistincts) {
        Bucket<T> result = new Bucket<T>();
        result.init(value, 1);
        for (int i = 1; i < equals; ++i) { // starting count from 1 since a new Bucket has equals of 1
            result.addEquals();
        }
        result.addLessThans(lessThans);
        result.addLessThanDistincts(lessThanDistincts);
        return result;
    }
    public static <T> List<Bucket<T>> compileSingleStream(Iterable<? extends T> inputs, int maxBuckets, long seed) {
        return compileSingleStream(inputs, maxBuckets, seed, new SingletonSplitter<T>());
    }
    
    public static <T> List<Bucket<T>> compileSingleStream(Iterable<? extends T> inputs, int maxBuckets, long seed,
                                                          Splitter<T> splitter
    ) {
        Sampler<T> sampler = new Sampler<T>(splitter, maxBuckets, seed);
        sampler.init();
        for (T input : inputs) {
            sampler.visit(input);
        }
        sampler.finish();
        List<List<Bucket<T>>> result = sampler.toBuckets();
        assertEquals("result length: " + result, 1, result.size());
        return result.get(0);
    }

    public static <T> List<Bucket<T>> compileSingleStream(Iterable<? extends T> inputs, int maxBuckets) {
        return compileSingleStream(inputs, maxBuckets, 37L);
    }

    public static class SingletonSplitter<T> implements Splitter<T> {
        @Override
        public int segments() {
            return 1;
        }

        @Override
        public List<? extends T> split(T input) {
            oneElementList.set(0, input);
            return oneElementList;
        }

        @Override
        public void recycle(T element) {
        }

        @SuppressWarnings("unchecked")
        private List<T> oneElementList = Arrays.asList((T[])new Object[1]);
    }
}
