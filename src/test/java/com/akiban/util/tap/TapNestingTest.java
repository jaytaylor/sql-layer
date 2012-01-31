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

    private static class BadNestingException extends RuntimeException
    {
        public BadNestingException()
        {
        }
    }
}
