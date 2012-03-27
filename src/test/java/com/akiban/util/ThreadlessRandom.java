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

public final class ThreadlessRandom {

    private int rand;

    public ThreadlessRandom() {
        this( (int)System.currentTimeMillis() );
    }

    public ThreadlessRandom(int seed) {
        this.rand = seed;
    }

    /**
     * Returns the next random number in the sequence
     * @return a pseudo-random number
     */
    public int nextInt() {
        return ( rand = rand(rand) ); // probably the randiest line in the code
    }

    /**
     * Returns the next random number in the sequence, bounded by the given bounds.
     * @param min the minimum value of the random number, inclusive
     * @param max the maximum value of the random number, isShared
     * @return a number N such that {@code min <= N < max}
     * @throws IllegalArgumentException if {@code min >= max}
     */
    public int nextInt(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException(String.format("bad range: [%d, %d)", min, max));
        }
        int range = max - min;
        int ret = nextInt();
        ret = Math.abs(ret) % range;
        return ret + min;
    }

    /**
     * Quick and dirty pseudo-random generator with no concurrency ramifications.
     * Taken from JCIP; the source is public domain. See:
     * http://jcip.net/listings.html listing 12.4.
     * @param seed the random's seed
     * @return the randomized result
     */
    public static int rand(int seed) {
        seed ^= (seed << 6);
        seed ^= (seed >>> 21);
        seed ^= (seed << 7);
        return seed;
    }
}
