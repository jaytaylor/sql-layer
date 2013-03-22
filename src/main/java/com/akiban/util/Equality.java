
package com.akiban.util;

import java.util.Arrays;

public final class Equality {

    public static boolean areEqual(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a == null) {
            return false;
        }
        Class<?> aClass = a.getClass();
        if (aClass.isArray()) {
            return equalArrays(a, b);
        }
        return a.equals(b);
    }

    private static boolean equalArrays(Object a, Object b) {
        Class<?> aClass = a.getClass();
        if (!aClass.equals(b.getClass())) {
            return false;
        }
        if (boolean[].class.equals(aClass)) return Arrays.equals( (boolean[])a, (boolean[])b );
        if (byte[].class.equals(aClass)) return Arrays.equals( (byte[])a, (byte[])b );
        if (char[].class.equals(aClass)) return Arrays.equals( (char[])a, (char[])b );
        if (double[].class.equals(aClass)) return Arrays.equals( (double[])a, (double[])b );
        if (float[].class.equals(aClass)) return Arrays.equals( (float[])a, (float[])b );
        if (int[].class.equals(aClass)) return Arrays.equals( (int[])a, (int[])b );
        if (long[].class.equals(aClass)) return Arrays.equals( (long[])a, (long[])b );
        if (short[].class.equals(aClass)) return Arrays.equals( (short[])a, (short[])b );
        if (Object[].class.equals(aClass)) return Arrays.deepEquals( (Object[])a, (Object[])b );
        throw new AssertionError(aClass.getName());
    }

    private Equality() {}
}
