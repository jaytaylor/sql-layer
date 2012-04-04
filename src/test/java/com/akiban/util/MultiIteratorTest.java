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

import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

public class MultiIteratorTest extends TestCase
{
    @Test
    public void testEmptyIterators()
    {
        checkIteration(new MultiIterator<Integer>());
        checkIteration(new MultiIterator<Integer>(iterator()));
        checkIteration(new MultiIterator<Integer>(iterator(), iterator()));
        checkIteration(new MultiIterator<Integer>(iterator(), iterator(), iterator()));
    }

    @Test
    public void testNonEmptyIterators()
    {
        checkIteration(new MultiIterator<Integer>(iterator(1)),
                       1);
        checkIteration(new MultiIterator<Integer>(iterator(1), iterator(2, 3)),
                       1, 2, 3);
        checkIteration(new MultiIterator<Integer>(iterator(1), iterator(2, 3), iterator(4, 5, 6)),
                       1, 2, 3, 4, 5, 6);
        checkIteration(new MultiIterator<Integer>(iterator(1), iterator(2, 3), iterator(4, 5, 6), iterator()),
                       1, 2, 3, 4, 5, 6);
        checkIteration(new MultiIterator<Integer>(iterator(1), iterator(2, 3), iterator(4, 5, 6), iterator(), iterator()),
                       1, 2, 3, 4, 5, 6);
        checkIteration(new MultiIterator<Integer>(iterator(), iterator(1), iterator(2, 3), iterator(4, 5, 6)),
                       1, 2, 3, 4, 5, 6);
        checkIteration(new MultiIterator<Integer>(iterator(), iterator(), iterator(1), iterator(2, 3), iterator(4, 5, 6)),
                       1, 2, 3, 4, 5, 6);
    }

    private void checkIteration(Iterator<Integer> iterator, int ... expected)
    {
        int n = 0;
        while (iterator.hasNext()) {
            Integer actual = iterator.next();
            assertEquals(expected[n++], actual.intValue());
        }
        assertEquals(expected.length, n);
    }

    private Iterator<Integer> iterator(Integer ... elements)
    {
        return Arrays.asList(elements).iterator();
    }
}
