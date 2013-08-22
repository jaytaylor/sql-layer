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

import java.util.Random;

public class TapNestingTest
{
    @Before
    public void before()
    {
        Tap.DISPATCHES.clear();
        Tap.registerBadNestingHandler(
            new Tap.BadNestingHandler()
            {
                @Override
                public void handleBadNesting(Tap tap)
                {
                    throw new BadNestingException();
                }
            });
    }

    @Test
    public void testOK()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        tap.out();
    }

    @Test(expected = BadNestingException.class)
    public void testReenter()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        tap.in();
    }

    @Test(expected = BadNestingException.class)
    public void testUnexpectedExit()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        tap.out();
        tap.out();
    }

    @Test(expected = BadNestingException.class)
    public void testUnexpectedExitSomeMore()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        tap.out();
        tap.out();
    }
    
    @Test
    public void testInEnableOut()
    {
        InOutTap tap = Tap.createTimer("test");
        disable();
        tap.in();
        enable();
        tap.out();
    }

    @Test
    public void testEnableOutInOut()
    {
        InOutTap tap = Tap.createTimer("test");
        disable();
        enable();
        tap.out();
        tap.in();
        tap.out();
    }

    @Test
    public void testTemporaryDisable()
    {
        InOutTap tap = Tap.createTimer("test");
        enable();
        tap.in();
        disable();
        tap.out();
        enable();
        tap.in();
    }

    @Test
    public void testRandomTemporaryDisable()
    {
        final int N = 100000;
        Random random = new Random();
        random.setSeed(419);
        InOutTap tap = Tap.createTimer("test");
        enable();
        boolean enabled = true;
        for (int i = 0; i < N; i++) {
            if ((i % 2) == 0) {
                tap.in();
            } else {
                tap.out();
            }
            if ((random.nextInt() % 3) == 0) {
                if (enabled) {
                    disable();
                    enabled = false;
                } else {
                    enable();
                    enabled = true;
                }
            }
        }
    }

    private void disable()
    {
        Tap.setEnabled(ALL_TAPS, false);
    }

    private void enable()
    {
        Tap.setEnabled(ALL_TAPS, true);
    }

    private static final String ALL_TAPS = ".*";
    private static class BadNestingException extends RuntimeException
    {
        public BadNestingException()
        {
        }
    }
}
