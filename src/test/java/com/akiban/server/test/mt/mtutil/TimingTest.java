
package com.akiban.server.test.mt.mtutil;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public final class TimingTest {
    @Test
    public void sleepIsRandom() {
        Timing timing = new Timing() {
            private boolean firstRun = true;
            @Override
            protected void doSleep(long millis) {
                if (firstRun) {
                    millis *= .80;
                    firstRun = false;
                }
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        long start = System.currentTimeMillis();
        timing.doSleepLoop(1500);
        long timeTook = System.currentTimeMillis() - start;

        assertTrue(String.format("%d", timeTook), timeTook >= 1500);
        assertTrue(String.format("%d", timeTook), timeTook - 250 < 1500);
    }
}
