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

import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class TapNestingTest
{
    @Before
    public void before()
    {
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
        InOutTap tap = Tap.createTimer("test", true);
        tap.in();
        tap.out();
    }

    @Test(expected = BadNestingException.class)
    public void testReenter()
    {
        InOutTap tap = Tap.createTimer("test", true);
        tap.in();
        tap.in();
    }

    @Test(expected = BadNestingException.class)
    public void testUnexpectedExit()
    {
        InOutTap tap = Tap.createTimer("test", true);
        tap.in();
        tap.out();
        tap.out();
    }

    @Test(expected = BadNestingException.class)
    public void testUnexpectedExitSomeMore()
    {
        InOutTap tap = Tap.createTimer("test", true);
        tap.in();
        tap.out();
        tap.out();
    }
    
    @Test
    public void testInEnableOut()
    {
        InOutTap tap = Tap.createTimer("test", false);
        tap.in();
        enable();
        tap.out();
    }

    @Test
    public void testEnableOutInOut()
    {
        InOutTap tap = Tap.createTimer("test", false);
        enable();
        tap.out();
        tap.in();
        tap.out();
    }

    @Test
    public void testTemporaryDisable()
    {
        InOutTap tap = Tap.createTimer("test", true);
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
        InOutTap tap = Tap.createTimer("test", true);
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
        tap.in();
        disable();
        tap.out();
        enable();
        tap.in();
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
