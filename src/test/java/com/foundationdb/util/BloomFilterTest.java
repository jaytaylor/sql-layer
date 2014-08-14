/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class BloomFilterTest
{
    @Test
    public void test()
    {
        for (double errorRate : ERROR_RATES) {
            for (int count : COUNTS) {
                test("dense longs", errorRate, count, denseLongs(0, count), denseLongs(count, 2 * count));
                test("sparse longs", errorRate, count, sparseLongs(0, count), sparseLongs(count, 2 * count));
                test("patterned strings", errorRate, count, patternedStrings(0, count), patternedStrings(count, 2 * count));
                test("random strings", errorRate, count, randomStrings(0, count), randomStrings(count, 2 * count));
            }
        }
    }

    private void test(String label, double errorRate, int count, List keys, List missingKeys)
    {
        BloomFilter filter = new BloomFilter(count, errorRate);
        for (Object key : keys) {
            filter.add(key.hashCode());
        }
        // Check that all keys are found
        for (Object key : keys) {
            assertTrue(filter.maybePresent(key.hashCode()));
        }
        // Check false positives for missing keys
        int falsePositives = 0;
        for (Object missingKey : missingKeys) {
            if (filter.maybePresent(missingKey.hashCode())) {
                falsePositives++;
            }
        }
        double actualErrorRate = ((double) falsePositives) / count;
        double maxAcceptableErrorRate = errorRate * 10;
        assertTrue(actualErrorRate <= maxAcceptableErrorRate);
    }

    @SuppressWarnings("unchecked")
    private List denseLongs(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            keys.add(i);
        }
        return keys;
    }

    @SuppressWarnings("unchecked")
    private List sparseLongs(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            long key = (((long) random.nextInt()) << 32) | i;
            keys.add(key);
        }
        return keys;
    }

    @SuppressWarnings("unchecked")
    private List patternedStrings(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            String key = String.format("img%09d.jpg", i);
            keys.add(key);
        }
        return keys;
    }

    @SuppressWarnings("unchecked")
    private List randomStrings(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            long key = (((long) random.nextInt()) << 32) | i;
            keys.add(Long.toString(key));
        }
        return keys;
    }

    private static final Random random = new Random(419);
    private static final double[] ERROR_RATES = { 0.10d, 0.1d, 0.01d, 0.001d, 0.0001d };
    private static final int[] COUNTS = { 100, 1000, 10000, 100000 };
}
