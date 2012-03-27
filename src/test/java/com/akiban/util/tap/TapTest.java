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

package com.akiban.util.tap;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TapTest {
    private static final int THREADS = 20;
    private static final int CYCLES = 10000;
    
    @Before
    public void before() {
        Tap.DISPATCHES.clear();
    }

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
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        TapReport[] reports = tap.getReports();
        assertEquals(1, reports.length);
        TapReport report = reports[0];
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

    @Test
    public void testInitiallyEnabled()
    {
        Tap.setInitiallyEnabled("^c|d$");
        InOutTap a = Tap.createTimer("a");
        PointTap b = Tap.createCount("b");
        InOutTap c = Tap.createTimer("c");
        PointTap d = Tap.createCount("d");
        TapReport[] reports = Tap.getReport(".*");
        assertEquals(2, reports.length);
        int mask = 0;
        for (TapReport report : reports) {
            if (report.getName().equals("c")) {
                mask |= 0x1;
            } else if (report.getName().equals("d")) {
                mask |= 0x2;
            } else {
                fail();
            }
        }
        assertEquals(0x3, mask);
    }

    @Test
    public void testDisableAll()
    {
        Tap.setInitiallyEnabled("^c|d$");
        InOutTap a = Tap.createTimer("a");
        PointTap b = Tap.createCount("b");
        InOutTap c = Tap.createTimer("c");
        PointTap d = Tap.createCount("d");
        Tap.setEnabled(".*", false);
        TapReport[] reports = Tap.getReport(".*");
        assertEquals(0, reports.length);
    }

    @Test
    public void testEnableAll()
    {
        Tap.setInitiallyEnabled("^c|d$");
        InOutTap a = Tap.createTimer("a");
        PointTap b = Tap.createCount("b");
        InOutTap c = Tap.createTimer("c");
        PointTap d = Tap.createCount("d");
        Tap.setEnabled(".*", false);
        TapReport[] reports = Tap.getReport(".*");
        assertEquals(0, reports.length);
        Tap.setEnabled(".*", true);
        reports = Tap.getReport(".*");
        assertEquals(4, reports.length);
        int mask = 0;
        for (TapReport report : reports) {
            if (report.getName().equals("a")) {
                mask |= 0x1;
            } else if (report.getName().equals("b")) {
                mask |= 0x2;
            } else if (report.getName().equals("c")) {
                mask |= 0x4;
            } else if (report.getName().equals("d")) {
                mask |= 0x8;
            } else {
                fail();
            }
        }
        assertEquals(0xf, mask);
    }

    @Test
    public void testEnableInitial()
    {
        Tap.setInitiallyEnabled("^c|d$");
        InOutTap a = Tap.createTimer("a");
        PointTap b = Tap.createCount("b");
        InOutTap c = Tap.createTimer("c");
        PointTap d = Tap.createCount("d");
        Tap.setEnabled(".*", false);
        TapReport[] reports = Tap.getReport(".*");
        assertEquals(0, reports.length);
        Tap.enableInitial();
        reports = Tap.getReport(".*");
        assertEquals(2, reports.length);
        int mask = 0;
        for (TapReport report : reports) {
            if (report.getName().equals("c")) {
                mask |= 0x1;
            } else if (report.getName().equals("d")) {
                mask |= 0x2;
            } else {
                fail();
            }
        }
        assertEquals(0x3, mask);
    }
}
