
package com.akiban.util;

public final class Undef {

    public static Object only() {
        return INSTANCE;
    }

    public static boolean isUndefined(Object possiblyUndef) {
        return INSTANCE == possiblyUndef;
    }

    private static final Undef INSTANCE = new Undef();

    @Override
    public String toString() {
        return "?";
    }
}
