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

import java.util.NoSuchElementException;

public final class WeightedRandom<T> {
    public interface Randomizer {
        int nextInt();
    }

    private static class ThreadlessRandomizer implements Randomizer {
        private int rand = (int)System.currentTimeMillis();

        @Override
        public int nextInt() {
            return ( rand = ThreadlessRandom.rand(rand) );
        }
    }

    private final Object[] elements;
    private final int[] weights;
    private final Randomizer randomizer;

    public WeightedRandom(T... elements) {
        this(new ThreadlessRandomizer(), elements);
    }

    public WeightedRandom(Randomizer randomizer, T... elements) {
        if (randomizer == null) {
            throw new IllegalArgumentException("Randomizer was null");
        }
        this.randomizer = randomizer;
        this.elements = new Object[elements.length];
        System.arraycopy(elements, 0, this.elements, 0, this.elements.length);
        weights = new int[elements.length];
        for (int i=0; i < weights.length; ++i) {
            weights[i] = 1;
        }
    }

    public void setWeight(T element, int weight) {
        if (weight < 0) {
            throw new IllegalArgumentException("weight can't be negative; was " + weight);
        }
        for (int i=0; i < elements.length; ++i) {
            if (element == elements[i]) {
                weights[i] = weight;
                return;
            }
        }
        throw new NoSuchElementException(element == null ? "null" : element.toString());
    }

    public boolean hasWeights() {
        for (int weight : weights) {
            if (weight > 0) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public T get(int weightDelta) {
        int rand = randomizer.nextInt();
        int index = randomWeighted(rand, weights);
        weights[index] += weightDelta;
        return (T)elements[index];
    }

    /**
     * Based on the pseudorandom provided (which should have a full int's range), returns the index of one of the
     * given ints. The likelihood of any given index being chosen is proportional to that index's int, which must
     * be {@code > 0}. For instance, if you passed in {@code {4, 3, 0, 3}} you would have a 40% chance of getting back
     * 0, a 30% chance of getting back 1, a 0% chance of getting back a 2, and a 30% chance of getting back 3.
     * @param pseudoRandom the random number to use
     * @param weights a non-null, non-empty array of positive ints
     * @return a number N such that {@code 0 <= N < weights.length}
     * @throws IllegalArgumentException if weights is empty, or if any weight is @{code < 0}, or the sum of
     * all weights is {@code >= Integer.MAX_VALUE}, or if the sum of all weights is 0.
     * @throws NullPointerException if weights is null
     */
    static int randomWeighted(int pseudoRandom, int[] weights) {
        int totalWeight = Integer.MIN_VALUE;
        if (weights.length == 0) {
            throw new IllegalArgumentException("weights can't be empty");
        }
        for (int i = 0; i < weights.length; ++i) {
            final int weight = weights[i];
            if (weight < 0) {
                throw new IllegalArgumentException(String.format("weights[%d] <= 0: %d", i, weight));
            }
            totalWeight += weight;
            if (totalWeight >= 0) {
                throw new IllegalArgumentException("sum of weights is too high");
            }
        }
        assert totalWeight < 0 : totalWeight;
        totalWeight -= Integer.MIN_VALUE;
        if (totalWeight == 0) {
            throw new IllegalArgumentException("no weights were > 0");
        }

        pseudoRandom = Math.abs(pseudoRandom % totalWeight);
        int sum = 0;
        for (int i = 0; i < weights.length; ++i) {
            sum += weights[i];
            if (pseudoRandom < sum) {
                return i;
            }
        }
        throw new AssertionError();
    }
}
