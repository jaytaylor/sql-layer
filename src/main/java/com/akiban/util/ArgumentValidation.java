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
