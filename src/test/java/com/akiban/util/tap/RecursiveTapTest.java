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

public class RecursiveTapTest
{
    @Test
    public void test() throws InterruptedException
    {
        InOutTap a = Tap.createRecursiveTimer("a");
        InOutTap b = a.createSubsidiaryTap("b", a);
        Tap.setEnabled(ALL_TAPS, true);
        a.in();
        Thread.sleep(100);
        b.in();
        Thread.sleep(100);
        a.in();
        Thread.sleep(100);
        a.out();
        Thread.sleep(100);
        b.out();
        Thread.sleep(100);
        a.out();
        for (TapReport report : Tap.getReport(ALL_TAPS)) {
            System.out.println(report);
        }
    }
    
    private static final String ALL_TAPS = ".*";
}
