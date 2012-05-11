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
