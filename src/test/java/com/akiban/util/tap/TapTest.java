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

package com.akiban.util.tap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TapTest {
    private static final int THREADS = 20;
    private static final int CYCLES = 10000;

    @Test
    public void testPerThreadTap() throws Exception {
        final InOutTap tap = Tap.createTimer("tap");
        Tap.setEnabled(".*", true);
        final Thread[] threads = new Thread[THREADS];
        for (int i = 0; i < THREADS; i++) {
            final Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    for (int j = 0; j < CYCLES; j++) {
                        tap.in();
                        tap.out();
                    }
                }

            }, "thread_" + i);
            threads[i] = thread;
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        final TapReport report = tap.getReport();
        assertEquals(THREADS * CYCLES, report.getInCount());
        assertEquals(THREADS * CYCLES, report.getOutCount());
    }

    // Inspired by bug 869554
    @Test
    public void testConcurrentTapControl() throws InterruptedException
    {
        Thread[] threads = new Thread[THREADS];
        final boolean[] ok = new boolean[THREADS];
        for (int t = 0; t < THREADS; t++) {
            final int threadId = t;
            threads[t] = new Thread()
            {
                @Override
                public void run()
                {
                    try {
                        Tap.createTimer(String.format("tap %s", threadId));
                        for (int i = 0; i < CYCLES; i++) {
                            Tap.setEnabled(".*", (i % 2) == 0);
                        }
                        ok[threadId] = true;
                    } catch (Exception e) {
                        ok[threadId] = false;
                    }
                }
            };
        }
        for (Thread thread : threads) {
            thread.start();
            Thread.sleep(1);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        for (boolean b : ok) {
            assertTrue(b);
        }
    }
}
