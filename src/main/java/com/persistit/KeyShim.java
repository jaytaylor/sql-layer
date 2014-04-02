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
package com.persistit;

/**
 * Promote visibility of package private Key methods.
 */
public class KeyShim
{
    /** Delegates to {@link Key#nextElementIndex(int)} */
    public static int nextElementIndex(Key key, int index) {
        return key.nextElementIndex(index);
    }

    /** Delegates to {@link Key#previousElementIndex(int)} */
    public static int previousElementIndex(Key key, int index) {
        return key.previousElementIndex(index);
    }

    /** Delegates to {@link Key#isSpecial()} */
    public static boolean isSpecial(Key key) {
        return key.isSpecial();
    }

    /** Delegates to {@link Key#isBefore()} */
    public static boolean isBefore(Key key) {
        return key.isBefore();
    }

    /** Delegates to {@link Key#isAfter()} */
    public static boolean isAfter(Key key) {
        return key.isAfter();
    }

    /** Delegates to {@link Key#nudgeDeeper()} */
    public static void nudgeDeeper(Key key) {
        key.nudgeDeeper();
    }

    /** Delegates to {@link Key#nudgeRight()} */
    public static void nudgeRight(Key key) {
        key.nudgeRight();
    }

    /** Delegates to {@link Key#nudgeLeft()} */
    public static void nudgeLeft(Key key) {
        key.nudgeLeft();
    }
}

