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
     * Returns the booleanNext random number in the sequence
     * @return a pseudo-random number
     */
    public int nextInt() {
        return ( rand = rand(rand) ); // probably the randiest line in the code
    }

    /**
     * Returns the booleanNext random number in the sequence, bounded by the given bounds.
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
