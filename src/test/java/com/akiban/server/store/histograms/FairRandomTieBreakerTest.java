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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class FairRandomTieBreakerTest {
    @Test
    public void evenDistribution() {
        new DistributionChecker<Integer>() {

            @Override
            protected TestDescription<Integer> inputs() {
                TestDescription<Integer> test = new TestDescription<Integer>(31);

                for (int i = 0; i < 100; ++i) {
                    test.input(i, 1, Distribution.SOME);
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
                    final Distribution distribution;
                    if (i % 4 == 0) {
                        count = 3;
                        distribution = Distribution.ALL;
                    }
                    else if (i % 11 == 0) {
                        count = 2;
                        distribution = Distribution.SOME;
                    }
                    else {
                        count = 1;
                        distribution = Distribution.NONE;
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
            for (int iteration = 0; iteration < iterations; ++iteration) {
                List<Bucket<T>> buckets = Buckets.compile(testDescription.list(), testDescription.maxBuckets());
                assertEquals("buckets size", testDescription.maxBuckets(), buckets.size());
                for (Bucket<T> bucket : buckets) {
                    long old = distributions.get(bucket.value());
                    distributions.put(bucket.value(), old+1);
                }
            }

            // do the checking
            int bucketsRemaining = testDescription.maxBuckets();
            for (T key : testDescription.forDistribution(Distribution.ALL)) {
                long hits = distributions.remove(key);
                assertEquals("hits for " + key + " (should be ALL)", iterations, hits);
                bucketsRemaining -= 1;
            }
            Collection<? extends T> somes = testDescription.forDistribution(Distribution.SOME);
            assertTrue("not enough buckets: " + bucketsRemaining, bucketsRemaining >= 0);
            double bucketsUsed = bucketsRemaining > somes.size() ? somes.size() : bucketsRemaining;
            double expectedHits = bucketsUsed / somes.size() * iterations;
            double error = 0.1 * expectedHits;
            for (T key : somes) {
                double hits = distributions.remove(key);
                double difference = Math.abs(expectedHits - hits);
                assertFalse("hit exactly " + iterations + ", but shouldn't have", hits == iterations);
                assertEquals(
                        "hits for " + key + " (should be SOME): expected=" + expectedHits + ", was=" + hits,
                        0,
                        difference,
                        error);
                bucketsRemaining -= 1;
            }
            for (T key : testDescription.forDistribution(Distribution.NONE)) {
                long hits = distributions.remove(key);
                assertEquals("hits for " + key + " (should be NONE)", 0, hits);
            }
            assertTrue("untested distributions: " + distributions, distributions.isEmpty());
        }
        
        private int iterations = 50000;
    }
    
    private static class TestDescription<T> {

        void input(T input, int count, Distribution distribution) {
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
            success = distributions.put(Distribution.ALL, last);
            assertTrue("couldn't put " + last + " -> " + Distribution.ALL, success);
            
        }
        
        public int maxBuckets() {
            return buckets;
        }
        
        public List<? extends T> list() {
            return inputs;
        }
        
        public Collection<? extends T> forDistribution(Distribution distribution) {
            return distributions.get(distribution);
        }
        
        TestDescription(int buckets) {
            this.buckets = buckets;
        }

        private int buckets;
        private List<T> inputs = Lists.newArrayList();
        private Multimap<Distribution,T> distributions = HashMultimap.create();
        private Distribution lastItemDistribution = null;
    } 

    enum Distribution {
        ALL,
        SOME,
        NONE
    }
}
