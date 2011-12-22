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

import com.akiban.util.ArgumentValidation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class FairRandomTieBreakerTest {
    @Test
    public void evenDistribution() {
        new DistributionChecker<Integer>() {

            @Override
            protected TestDescription<Integer> inputs() {
                TestDescription<Integer> test = new TestDescription<Integer>(31);

                for (int i = 0; i < 100; ++i) {
                    test.input(i, 1, 30d/99); // 30 buckets; value 99 is always there
                }

                return test;
            }
        }.run();
    }

    @Test
    public void unevenDistrib() {
        new DistributionChecker<Integer>() {
            @Override
            protected TestDescription<Integer> inputs() {
                TestDescription<Integer> test = new TestDescription<Integer>(31);
                for (int i = 0; i < 100; ++i) {
                    final int count;
                    final double distribution;
                    if (i % 4 == 0) {
                        count = 3;
                        distribution = Double.POSITIVE_INFINITY;
                    }
                    else if (i % 7 == 0) {
                        count = 2;
                        // 28, 56 and 84 are divisible by 7, but their divisiblity by 4 takes precedence
                        // this leaves 11 values in this category.
                        // 31 buckets - 25 (for the %4s) - 1 (for the endpoint) = 5 buckets split by 11 values
                        distribution = 5d / 11;
                    }
                    else {
                        count = 1;
                        distribution = Double.NEGATIVE_INFINITY;
                    }
                    test.input(i, count, distribution);
                }
                return test;
            }
        }.run();
    }

    @Test
    public void unevenDistribWithTies() {
        new DistributionChecker<Integer>() {
            @Override
            protected TestDescription<Integer> inputs() {
                TestDescription<Integer> test = new TestDescription<Integer>(31);
                for (int i = 0; i < 100; ++i) {
                    final int count;
                    final double distribution;
                    if (i % 4 == 0) {
                        count = 3;
                        distribution = Double.POSITIVE_INFINITY;
                    }
                    else if (i % 7 == 0 || i % 13 == 0) {
                        count = 2;
                        // for the %7:
                        // 28, 56 and 84 are divisible by 7, but their divisiblity by 4 takes precedence
                        // for the %13, there are 7 values, of which one (52) is also divisible by 4,
                        // and onen of which (91) is divisible by 7, so we shouldn't count it twice
                        // So we have 11 + 5 = 16 values
                        // 31 buckets - 25 (for the %4s) - 1 (for the endpoint) = 5 buckets split by 17 values
                        distribution = 5d / 16;
                    }
                    else {
                        count = 1;
                        distribution = Double.NEGATIVE_INFINITY;
                    }
                    test.input(i, count, distribution);
                }
                return test;
            }
        }.run();
    }
    
    private static abstract class DistributionChecker<T extends Comparable<? super T>> {

        protected abstract TestDescription<T> inputs();

        public void run() {
            TestDescription<T> testDescription = inputs();
            testDescription.fixLastInput();
            // initialize distributions
            Map<T,Long> distributions = new TreeMap<T, Long>();
            for (T input : testDescription.list()) {
                distributions.put(input, 0L);
            }

            // run the iterations
            // Each iteration runs with a different seed number, but these seeds are generated Random with a constant
            // seed. This means the iterations themselves get well-distributed seeds, but these seeds are the same
            // for each run of this test. That way, we won't get intermittent failures.
            Random seedGenerator = new Random(seedSeed);
            for (int iteration = 0; iteration < iterations; ++iteration) {
                long seed = seedGenerator.nextLong();
                List<Bucket<T>> buckets = Buckets.compile(testDescription.list(), testDescription.maxBuckets(), seed);
                assertEquals("buckets size", testDescription.maxBuckets(), buckets.size());
                for (Bucket<T> bucket : buckets) {
                    T bucketValue = bucket.value();
                    long old = distributions.get(bucketValue);
                    distributions.put(bucketValue, old+1);
                }
            }

            // do the checking
            for (Map.Entry<Double,T> entry : testDescription.distributionsPerValue()) {
                double distribution = entry.getKey();
                T key = entry.getValue();
                if (distribution == Double.POSITIVE_INFINITY) {
                    long hits = distributions.remove(key);
                    assertEquals("hits for " + key + " (should be ALL)", iterations, hits);
                }
                else if (distribution == Double.NEGATIVE_INFINITY) {
                    long hits = distributions.remove(key);
                    assertEquals("hits for " + key + " (should be ALL)", 0, hits);
                }
                else {
                    double hits = distributions.remove(key);
                    double expectedHits = distribution * iterations;
                    double error = 0.05 * expectedHits;
                    assertFalse(key + ": hit " + hits + ", but shouldn't have: " + iterations, hits >= iterations);
                    assertEquals(
                            String.format("hits for %s (err=%.3f)", key, error),
                            expectedHits,
                            hits,
                            error);
                }
            }
            assertTrue("untested distributions: " + distributions, distributions.isEmpty());
        }
        
        private int iterations = 10000;
    }
    
    private static class TestDescription<T> {

        void input(T input, int count, double distribution) {
            if (Double.isNaN(distribution))
                throw new IllegalArgumentException("NaN not allowed");
            ArgumentValidation.isGT("count", count, 0);
            for (int i = 0; i < count; ++i)
                inputs.add(input);
            boolean success = distributions.put(distribution, input);
            assertTrue("duplicate of " + input + ", " + distribution, success);
            lastItemDistribution = distribution;
        }

        public void fixLastInput() {
            T last = inputs.get(inputs.size() - 1);
            boolean success = distributions.remove(lastItemDistribution, last);
            assertTrue("couldn't remove " + last + " -> " + lastItemDistribution, success);
            success = distributions.put(Double.POSITIVE_INFINITY, last);
            assertTrue("couldn't put " + last + " -> INFINITY", success);
            
        }
        
        public int maxBuckets() {
            return buckets;
        }
        
        public List<? extends T> list() {
            return inputs;
        }

        public Collection<Map.Entry<Double,T>> distributionsPerValue() {
            return distributions.entries();
        }
        
        TestDescription(int buckets) {
            this.buckets = buckets;
        }

        private int buckets;
        private List<T> inputs = Lists.newArrayList();
        private Multimap<Double,T> distributions = HashMultimap.create();
        private double lastItemDistribution = Double.NaN;
    }
    
    private static final long seedSeed = 31L;
}
