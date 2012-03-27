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

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.fail;

public final class WeightedRandomTest {
    @Test
    public void testRandomly() {
        int[] weights = { 2, 4, 0, 3, 1 };
        final int LOOPS = 10000000;
        final double acceptableError = 0.01;
        int[] results = new int[weights.length];

        Random random = new Random();
        for (int i = 0; i < LOOPS; ++i) {
            int index = WeightedRandom.randomWeighted(random.nextInt(), weights);
            results[index] ++;
        }
        double[] ratios = new double[weights.length];
        for (int i = 0; i < results.length; ++i) {
            ratios[i] = 10. * ((double)results[i]) / LOOPS;
        }
        for (int i = 0; i < ratios.length; ++i) {
            if (weights[i] == 0 && results[i] != 0) {
                fail(Arrays.toString(ratios));
            }
            double expected = weights[i];
            double error = Math.abs(ratios[i] - expected) / expected;
            if (error > acceptableError) {
                fail(Arrays.toString(ratios));
            }
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void negativeWeight() {
        WeightedRandom.randomWeighted(0, new int[]{-1});
    }

    @Test
    public void maxWeight() {
        WeightedRandom.randomWeighted(0, new int[]{Integer.MAX_VALUE - 2, 2});
    }

    @Test(expected=IllegalArgumentException.class)
    public void tooMuchWeight() {
        WeightedRandom.randomWeighted(0, new int[]{Integer.MAX_VALUE - 2, 3});
    }
}
