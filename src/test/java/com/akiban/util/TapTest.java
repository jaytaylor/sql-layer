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

package com.akiban.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Random;

import org.junit.Test;

public class TapTest {

    @Test
    public void testPerThreadTap() throws Exception {
        final Tap.InOutTap tap = Tap.createTimeStampLog("ttap");
        Tap.setEnabled(".*", true);
        final Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            final Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    final Random random = new Random();
                    for (int j = 0; j < 20; j++) {
                        tap.in();
                        sleep(random.nextInt(200) + 10);
                        tap.out();
                        sleep(10);
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
        System.out.println(Tap.report());
        final TapReport report = tap.getReport();
        final Tap.PerThread.PerThreadTapReport pttr = (Tap.PerThread.PerThreadTapReport) report;
        final Map<String, TapReport> map = pttr.getTapReportMap();
        assertEquals(10, map.size());
        final Tap.TimeStampLog.TimeStampTapReport tstp = (Tap.TimeStampLog.TimeStampTapReport) map
                .get("thread_6");
        final long[] log = tstp.getLog();
        assertEquals(40, log.length);
        assertEquals(0, log[0]);
        long value = -1;
        for (int i = 0; i < log.length; i++) {
            assertTrue(log[i] > value);
            value = log[i];
        }
    }

    private void sleep(final int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }
}
