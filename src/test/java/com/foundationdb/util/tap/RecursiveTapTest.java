/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.util.tap;

import org.junit.Before;
import org.junit.Test;

import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecursiveTapTest
{
    @Before
    public void before()
    {
        Tap.DISPATCHES.clear();
        createTaps();
    }

    @Test
    public void testTapStructure()
    {
        assertTrue(a.internal() instanceof Dispatch);
        assertTrue(b.internal() instanceof Dispatch);
    }
    
    @Test
    public void testEnableRoot() throws InterruptedException
    {
        aba();
    }

    @Test
    public void testEnableSubsidiary() throws InterruptedException
    {
        disableTaps();
        Tap.setEnabled(SUBSIDIARY_TAP, true);
        a.in();
        sleep();
        b.in();
        sleep();
        a.in();
        sleep();
        a.out();
        sleep();
        b.out();
        sleep();
        a.out();
        expectNoReports();
    }

    @Test
    public void testDisableRoot() throws InterruptedException
    {
        disableTaps();
        a.in();
        sleep();
        b.in();
        sleep();
        a.in();
        sleep();
        a.out();
        sleep();
        b.out();
        sleep();
        a.out();
        expectNoReports();
    }
    
    @Test
    public void testSiblings() throws InterruptedException
    {
        Tap.DISPATCHES.clear(); // Different set of taps from other tests
        // Tap structure:
        // r
        //    i
        //    j
        //        x
        //        y
        //    k
        InOutTap r = Tap.createRecursiveTimer("r");
        InOutTap i = r.createSubsidiaryTap("i");
        InOutTap j = r.createSubsidiaryTap("j");
        InOutTap k = r.createSubsidiaryTap("k");
        InOutTap x = r.createSubsidiaryTap("x");
        InOutTap y = r.createSubsidiaryTap("y");
        Tap.setEnabled("r", true);
        r.in();
        sleep();
            i.in();
            sleep();
                j.in();
                sleep();
                    x.in();
                    sleep();
                    x.out();
                sleep();
                    y.in();
                    sleep();
                    y.out();
                sleep();
                j.out();
            sleep();
                k.in();
                sleep();
                k.out();
            sleep();
            i.out();
        sleep();
        r.out();
        TapReport[] tapReports = Tap.getReport("r");
        assertEquals(6, tapReports.length);
        for (TapReport report : tapReports) {
            if (report.getName().equals("r")) {
                assertEquals(1, report.getInCount());
//                assertTrue(checkTicks(report, 2));
            } else if (report.getName().equals("i")) {
                assertEquals(1, report.getInCount());
//                assertTrue(checkTicks(report, 3));
            } else if (report.getName().equals("j")) {
                assertEquals(1, report.getInCount());
//                assertTrue(checkTicks(report, 3));
            } else if (report.getName().equals("k")) {
                assertEquals(1, report.getInCount());
//                assertTrue(checkTicks(report, 1));
            } else if (report.getName().equals("x")) {
                assertEquals(1, report.getInCount());
//                assertTrue(checkTicks(report, 1));
            } else if (report.getName().equals("y")) {
                assertEquals(1, report.getInCount());
//                assertTrue(checkTicks(report, 1));
            } else {
                fail();
            }
            assertEquals(report.getOutCount(), report.getInCount());
        }
    }
    
    // The following tests enable/disable taps at all combinations of points with respect to the ABA 
    // tap pattern:
    //
    // 1
    //       in A
    // 2
    //           in B
    // 3
    //               in A
    // 4
    //               out A
    // 5
    //           out B
    // 6
    //       out A
    // 7
    // 
    // testEnableDisable_XY starts with taps enabled. The root tap is disabled at position X and enabled it at Y,
    // X = 1..7, Y = 1..7, X <= Y. Then, after making sure that results are correct, the standard aba test is run
    // to see that that still works after the disabling/enabling.

    @Test
    public void testEnableDisable_11() throws InterruptedException
    {
        /* 1 */ disableTaps(); enableTaps();
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 2, 3, 1, 2);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_12() throws InterruptedException
    {
        /* 1 */ disableTaps();
        a.in();
        /* 2 */ enableTaps();
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 1, 1, 1, 2);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_13() throws InterruptedException
    {
        /* 1 */ disableTaps();
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */ enableTaps();
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 1, 1, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_14() throws InterruptedException
    {
        /* 1 */ disableTaps();
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */ enableTaps();
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_15() throws InterruptedException
    {
        /* 1 */ disableTaps();
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */ enableTaps();
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_16() throws InterruptedException
    {
        /* 1 */ disableTaps();
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */ enableTaps();
        sleep();
        a.out();
        /* 7 */
        expect(1, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_17() throws InterruptedException
    {
        /* 1 */ disableTaps();
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */ enableTaps();
        expect(1, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_22() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */ disableTaps(); enableTaps();
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 1, 1, 1, 2);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_23() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */ disableTaps();
        sleep();
        b.in();
        /* 3 */ enableTaps();
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 1, 1, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_24() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */ disableTaps();
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */ enableTaps();
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_25() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */ disableTaps();
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */ enableTaps();
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_26() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */ disableTaps();
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */ enableTaps();
        sleep();
        a.out();
        /* 7 */
        expect(1, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_27() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */ disableTaps();
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */ enableTaps();
        expect(1, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_33() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */ disableTaps(); enableTaps();
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 1, 1, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_34() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */ disableTaps();
        sleep();
        a.in();
        /* 4 */ enableTaps();
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_35() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */ disableTaps();
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */ enableTaps();
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_36() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */ disableTaps();
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */ enableTaps();
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_37() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */ disableTaps();
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */ enableTaps();
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_44() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */ disableTaps(); enableTaps();
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_45() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */ disableTaps();
        sleep();
        a.out();
        /* 5 */ enableTaps();
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_46() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */ disableTaps();
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */ enableTaps();
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_47() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */ disableTaps();
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */ enableTaps();
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_55() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */ disableTaps(); enableTaps();
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_56() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */ disableTaps();
        sleep();
        b.out();
        /* 6 */ enableTaps();
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_57() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */ disableTaps();
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */ enableTaps();
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_66() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */ disableTaps(); enableTaps();
        sleep();
        a.out();
        /* 7 */
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_67() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */ disableTaps();
        sleep();
        a.out();
        /* 7 */ enableTaps();
        expect(2, 0, 0, 0, 0);
        enableTaps();
        aba();
    }

    @Test
    public void testEnableDisable_77() throws InterruptedException
    {
        /* 1 */
        a.in();
        /* 2 */
        sleep();
        b.in();
        /* 3 */
        sleep();
        a.in();
        /* 4 */
        sleep();
        a.out();
        /* 5 */
        sleep();
        b.out();
        /* 6 */
        sleep();
        a.out();
        /* 7 */ disableTaps(); enableTaps();
        expect(2, 0, 0, 0, 0);
    }

    private void aba() throws InterruptedException
    {
        a.in();
        sleep();
            b.in();
            sleep();
                a.in();
                sleep();
                a.out();
            sleep();
            b.out();
        sleep();
        a.out();
        expect(2, 2, 3, 1, 2);
    }

    private void createTaps()
    {
        a = Tap.createRecursiveTimer("a");
        b = a.createSubsidiaryTap("b");
        enableTaps();
    }

    // nTaps: number of taps touched (via in or out) while taps are enabled. The root tap (a) is enabled even if
    // not touched.
    private void expect(int nTaps, int aCount, int aTicks, int bCount, int bTicks)
    {
        TapReport[] tapReports = Tap.getReport(ROOT_TAP);
        assertEquals(nTaps, tapReports.length);
        for (TapReport report : tapReports) {
            if (report.getName().equals("a")) {
                assertEquals(
                    String.format("aCount = %s", report.getInCount()),
                    aCount,
                    report.getInCount());
/*
                assertTrue(
                    String.format("aCount = %s, aTime = %s",
                                  report.getInCount(),
                                  report.getCumulativeTime() / MILLION),
                    checkTicks(report, aTicks));
*/
            } else if (report.getName().equals("b")) {
                assertEquals(
                    String.format("bCount = %s", report.getInCount()),
                    bCount,
                    report.getInCount());
/*
                assertTrue(
                    String.format("bCount = %s, bTime = %s",
                                  report.getInCount(),
                                  report.getCumulativeTime() / MILLION),
                    checkTicks(report, bTicks));
*/
            } else {
                fail();
            }
            assertEquals(report.getOutCount(), report.getInCount());
        }
    }

    private void expectNoReports()
    {
        assertEquals(0, Tap.getReport(ROOT_TAP).length);
    }

    private void enableTaps()
    {
        Tap.setEnabled(ROOT_TAP, true);
    }

    private void disableTaps()
    {
        Tap.setEnabled(ROOT_TAP, false);
    }

    private void sleep() throws InterruptedException
    {
        Thread.sleep(TICK_LENGTH_MSEC);
    }

    private boolean checkTicks(TapReport tapReport, int ticks)
    {
        return abs(tapReport.getCumulativeTime() / MILLION - ticks * TICK_LENGTH_MSEC) < CLOCK_IMPRECISION_MSEC;
    }

    private static final String ROOT_TAP = "a";
    private static final String SUBSIDIARY_TAP = "b";
    private static final int MILLION = 1000000;
    private static final int TICK_LENGTH_MSEC = 20;
    private static final int CLOCK_IMPRECISION_MSEC = 10;

    private InOutTap a;
    private InOutTap b;
}
