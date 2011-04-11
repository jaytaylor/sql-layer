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
