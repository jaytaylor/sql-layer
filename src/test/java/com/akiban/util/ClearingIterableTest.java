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

package com.akiban.util;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public final class ClearingIterableTest
{
    @Test
    public void mainTest()
    {
        List<Integer> list = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4, 5));

        Iterator<Integer> iter = ClearingIterable.from(list).iterator();

        assertEquals(iter.next().intValue(), 1);
        assertArray(list, 1, 2, 3, 4, 5);

        {
            int count = 0, sum = 0;
            for (Integer i : list)
            {
                ++count;
                sum += i;
            }
            assertEquals(count, 5);
            assertEquals(sum, 1 + 2 + 3 + 4 + 5);
            assertArray(list, 1, 2, 3, 4, 5);
        }

        while(iter.hasNext())
        {
            iter.next();
        }

        assertArray(list);
    }

    @Test
    public void removeTest()
    {
        List<Integer> list = new ArrayList<Integer>(Arrays.asList(1, 2, 3));
        Iterator<Integer> iter = ClearingIterable.from(list).iterator();

        assertEquals(iter.next().intValue(), 1);
        assertArray(list, 1, 2, 3);
        iter.remove();
        assertArray(list, 2, 3);

        assertEquals(iter.next().intValue(), 2);
        assertArray(list, 2, 3);
        iter.remove();
        assertArray(list, 3);

        assertEquals(iter.next().intValue(), 3);
        assertArray(list);
        iter.remove();
        assertArray(list);
    }

    @Test
    public void foreachTest()
    {
        List<Integer> list = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4, 5));

        {
            int count=0, sum = 0;
            for(Integer i : list)
            {
                ++count;
                sum += i;
            }
            assertEquals(count, 5);
            assertEquals(sum, 1 + 2 + 3 + 4 + 5);
            assertArray(list, 1, 2, 3, 4, 5);
        }

        {
            int count=0, sum = 0;
            for(Integer i : ClearingIterable.from(list))
            {
                ++count;
                sum += i;
            }
            assertEquals(count, 5);
            assertEquals(sum, 1 + 2 + 3 + 4 + 5);
            assertArray(list);
        }
    }

    private static void assertArray(List<Integer> list, int... expected)
    {
        assertEquals("list size", expected.length, list.size());
        for (int i=0; i<expected.length; ++i)
        {
            assertEquals("at index " + expected, expected[i], list.get(i).intValue());
        }
    }
}
