package com.akiban.util;

public abstract class Debugging {
    public static boolean assertsAreOn() {
        boolean usingAsserts = false;
        assert usingAsserts = true;
        return usingAsserts;
    }
}
