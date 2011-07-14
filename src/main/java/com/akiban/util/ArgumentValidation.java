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

import java.util.Collection;

public final class ArgumentValidation {
    public static void isNull(String argName, Object arg) {
        if (arg != null) {
            throw new IllegalArgumentException(String.format("%s must be null", argName));
        }
    }

    public static void notNull(String argName, Object arg) {
        if (arg == null) {
            throw new IllegalArgumentException(String.format("%s may not be null", argName));
        }
    }

    public static void arrayLength(String argName, Object[] array, int length) {
        notNull(argName, array);
        if (array.length != length) {
            throw new IllegalArgumentException(
                    String.format("%s.length must be %d, was %d", argName, length, array.length)
            );
        }
    }

    public static void notEmpty(String argName, Collection<?> collection) {
        notNull(argName, collection);
        if (collection.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s may not be empty", argName));
        }
    }

    public static void isTrue(String predicateDescription, boolean predicate) {
        if (!predicate) {
            throw new IllegalArgumentException(String.format("%s does not hold", predicateDescription));
        }
    }

    public static void isSame(String oneName, Object one, String twoName, Object two) {
        if (one != two) {
            throw new IllegalArgumentException(String.format("%s(%d) != %s(%d)", oneName, one, twoName, two));
        }
    }

    public static void isNotSame(String oneName, Object one, String twoName, Object two) {
        if (one == two) {
            throw new IllegalArgumentException(String.format("%s(%d) == %s(%d)", oneName, one, twoName, two));
        }
    }

    /**
     * Makes sure the given number is greater than or equal to the given minimum.
     * @param i the number to test
     * @param min the minimum value that i may be (inclusive)
     */
    public static void isGTE(String argName, long i, long min) {
        if (i < min) {
            throw new IllegalArgumentException(String.format("%s must be >= %d; was %d", argName, min, i));
        }
    }

    public static void isGT(String argName, long i, long min) {
        if (i <= min) {
            throw new IllegalArgumentException(String.format("%s must be > %d; was %d", argName, min, i));
        }
    }

    public static void isNotNegative(String argName, int i) {
        isGTE(argName, i, 0);
    }

    public static void isLTE(String argName, int i, int max) {
        if (i > max) {
            throw new IllegalArgumentException(String.format("%s must be <= %d; was %d", argName, max, i));
        }
    }

    public static void isLT(String argName, int i, int max) {
        if (i >= max) {
            throw new IllegalArgumentException(String.format("%s must be < %d; was %d", argName, max, i));
        }
    }

    public static void isEQ(String oneName, int one, String twoName, int two) {
        if (one != two) {
            throw new IllegalArgumentException(String.format("%s(%d) != %s(%d)", oneName, one, twoName, two));
        }
    }
}
