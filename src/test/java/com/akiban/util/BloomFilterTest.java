/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.util;

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

    private List denseLongs(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            keys.add(i);
        }
        return keys;
    }

    private List sparseLongs(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            long key = (((long) random.nextInt()) << 32) | i;
            keys.add(key);
        }
        return keys;
    }

    private List patternedStrings(int start, int end)
    {
        List keys = new ArrayList(end - start);
        for (long i = start; i < end; i++) {
            String key = String.format("img%09d.jpg", i);
            keys.add(key);
        }
        return keys;
    }

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
