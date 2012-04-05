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

package com.akiban.server.service.memcache.outputter.jsonoutputter;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class UnOrphaningIteratorTest
{
    @Test
    public void testEmpty()
    {
        check(fillIn());
    }

    @Test
    public void testNoOrphans()
    {
        check(fillIn("a"),
              "a");
        check(fillIn("a", "b"),
              "a", "b");
        check(fillIn("a", "b", "c"),
              "a", "b", "c");
    }

    @Test
    public void testParent()
    {
        check(fillIn("a", "ab"),
              "a", "ab");
        check(fillIn("a", "ab", "abc"),
              "a", "ab", "abc");
        check(fillIn("a", "ab", "abc", "abcd"),
              "a", "ab", "abc", "abcd");
    }

    @Test
    public void testAncestor()
    {
        check(fillIn("a", "abc"),
              "a", "ab", "abc");
        check(fillIn("a", "abcd"),
              "a", "ab", "abc", "abcd");
        check(fillIn("a", "abcde"),
              "a", "ab", "abc", "abcd", "abcde");
    }

    @Test
    public void testMultipleRoots()
    {
        check(fillIn("a", "abcde", "abm", "abmno", "p", "pqrst"),
              "a", "ab", "abc", "abcd", "abcde",
              "abm", "abmn", "abmno",
              "p", "pq", "pqr", "pqrs", "pqrst");
    }

    @Test
    public void testMissingRoot()
    {
        check(fillIn("ab"), "a", "ab");
        check(fillIn("abc"), "a", "ab", "abc");
        check(fillIn("abcd"), "a", "ab", "abc", "abcd");
    }

    private Iterator<String> fillIn(String... input)
    {
        return new UnOrphaningIterator<String>(Arrays.asList(input).iterator(), new TestGenealogist());
    }

    private void check(Iterator<String> actual, String... expected)
    {
        int i = 0;
        while (actual.hasNext()) {
            assertTrue(i < expected.length);
            assertEquals(expected[i], actual.next());
            i++;
        }
        assertEquals(expected.length, i);
    }

    private static class TestGenealogist implements Genealogist<String>
    {
        @Override
        public void fillInMissing(String x, String y, Queue<String> missing)
        {
            if (x == null) {
                // y is first element of input iterator
                x = "";
            }
            if (y.startsWith(x)) {
                for (int n = x.length() + 1; n < y.length(); n++) {
                    missing.add(y.substring(0, n));
                }
            }
        }
    }
}
