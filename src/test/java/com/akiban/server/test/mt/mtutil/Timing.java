
package com.akiban.server.test.mt.mtutil;

public class Timing {
    private final static Timing instance = new Timing();

    Timing() {
        // nothing
    }

    /**
     * Sleeps in a way that gets around code instrumentation which may end up shortening Thread.sleep.
     * If Thread.sleep throws an exception, this will invoke {@code Thread.currentThread().interrupt()} to propagate it.
     * @param millis how long to sleep
     */
    public static void sleep(long millis) {
        instance.doSleepLoop(millis);
    }

    final void doSleepLoop(long millis) {
        if (millis <= 0) {
            return;
        }
        final long start = System.currentTimeMillis();
        final long end = start + millis;
        long remaining = millis;
        do {
            doSleep(remaining);
            remaining = end - System.currentTimeMillis();
        } while (remaining > 0);
    }

    protected void doSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
