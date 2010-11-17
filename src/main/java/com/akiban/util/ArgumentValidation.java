package com.akiban.util;

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

    /**
     * Makes sure the given number is greater than or equal to the given minimum.
     * @param i the number to test
     * @param min the minimum value that i may be (inclusive)
     */
    public static void isGTE(String argName, int i, int min) {
        if (i < min) {
            throw new IllegalArgumentException(String.format("%s must be >= %d; was %d", argName, min, i));
        }
    }

    public static void isNotNegative(String argName, int i) {
        isGTE(argName, i, 0);
    }

    public static void isLT(String argName, int i, int max) {
        if (i >= max) {
            throw new IllegalArgumentException(String.format("%s must be < %d; was %d", argName, max, i));
        }
    }
}
