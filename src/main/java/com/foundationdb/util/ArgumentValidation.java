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

    public static void isBetween(String argName, long min, long actual, long max) {
        if (actual < min || actual >= max) {
            throw new IllegalArgumentException(String.format("required %d <= %s < %d, but found %s=%d",
                    min, argName, max, argName, actual
            ));
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

    public static void isEQ(String oneName, Object one, String twoName, Object two) {
        if (!one.equals(two)) {
            throw new IllegalArgumentException(String.format("%s(%s) != %s(%s)", oneName, one, twoName, two));
        }
    }

    public static void isEQ(String message, int i, int requiredValue) {
        if (i != requiredValue) {
            throw new IllegalArgumentException(message + " required " + requiredValue + " but got " + i);
        }
    }

    public static void withinArray(String arrayDescription, byte[] array, String offsetDescription, int offset) {
        if (offset < 0 || offset >= array.length) {
            throw new IllegalArgumentException(
                    offsetDescription + " (" + offset + ") not within bounds of array "
                            + arrayDescription + " (length=" + array.length+')'
            );
        }
    }
}
