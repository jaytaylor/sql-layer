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

package com.foundationdb.sql.optimizer.rule.join_enum;

import com.foundationdb.sql.optimizer.plan.Joinable;
import com.foundationdb.sql.optimizer.plan.PlanToString;

import java.util.List;

/**
 * A set of {@link Joinable}s is represented as a bit vector,
 * implemented directly as a <code>long</code> and not even wrapped in
 * a small class. Hence this class is all static methods.
 */
public class JoinableBitSet
{
    /** The empty set. */
    public static long empty() {
        return 0;
    }

    public static boolean isEmpty(long s) {
        return (s == 0);
    }

    /** A set containing a single element with the given index. */
    public static long of(int i) {
        return (1L << i);
    }

    public static int count(long s) {
        return Long.bitCount(s);
    }

    /** Are these two sets equal (same members)? */
    public static boolean equals(long s1, long s2) {
        return (s1 == s2);
    }

    /** Union of the two sets. */
    public static long union(long s1, long s2) {
        return s1 | s2;
    }

    /** Intersection of the two sets. */
    public static long intersection(long s1, long s2) {
        return s1 & s2;
    }

    /** Set difference: elements of <code>s1</code> not in <code>s2</code>. */
    public static long difference(long s1, long s2) {
        return s1 & ~s2;
    }

    /** Do the two sets overlap? */
    public static boolean overlaps(long s1, long s2) {
        return ((s1 & s2) != 0);
    }

    /** Is <code>sub</code> a subset (not necessarily proper) of <code>sup</code>? */
    public static boolean isSubset(long sub, long sup) {
        return ((sub & sup) == sub);
    }

    /** Given a subset <code>sub</code> of <code>sup</code>, return the next subset.
     * Repeated application of this method, starting with {@link empty} 
     * will enumerate all subsets of the given set, ending with <code>sup</code> itself.
     */
    public static long nextSubset(long sub, long sup) {
        return (sup & (sub - sup));
    }

    /** The index of the smallest element of the given set. */
    public static int min(long s) {
        return Long.numberOfTrailingZeros(s);
    }

    /** The single element subset of the given set containing just the
     * element with the samllest index. */
    public static long minSubset(long s) {
        return Long.lowestOneBit(s);
    }

    /** A set of all elements with index up to and including the given index. */
    public static long through(int i) {
        return ((1 << (i+1)) - 1);
    }

    /** Printed representation of the given set for debugging. */
    public static String toString(long s, List<Joinable> items) {
        StringBuilder str = new StringBuilder("[");
        boolean first = true;
        for (int i = 0; i < 64; i++) {
            if (overlaps(s, of(i))) {
                if (first)
                    first = false;
                else
                    str.append(", ");
                str.append(items.get(i).summaryString(PlanToString.Configuration.DEFAULT));
            }
        }
        str.append("]");
        return str.toString();
    }

    private JoinableBitSet() {
    }
}
