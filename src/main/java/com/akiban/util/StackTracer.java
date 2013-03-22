
package com.akiban.util;

import java.util.AbstractList;

/**
 * Utility class for seeing stack traces. Basically a thin shim around <tt>Thread.currentThread().getStackTrace()</tt>
 * that looks like a {@code List&lt;StackTraceElement&gt;}. Put one of these as a field in a class, and you'll be able
 * to see where its instances come from.
 */
public final class StackTracer extends AbstractList<StackTraceElement> {

    public StackTracer() {
        this.trace = Thread.currentThread().getStackTrace();
    }

    // AbstractList methods

    @Override
    public StackTraceElement get(int index) {
        return trace[index + TRIM];
    }

    @Override
    public int size() {
        return trace.length - TRIM;
    }

    private final StackTraceElement[] trace;
    private static final int TRIM = 2; // trim off the top two of the stack, which are getStackTrace and constructor
}
