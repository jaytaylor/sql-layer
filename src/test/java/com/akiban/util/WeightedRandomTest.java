
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
