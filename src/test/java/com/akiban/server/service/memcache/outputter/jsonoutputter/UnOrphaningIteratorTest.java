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
        public void fillInDescendents(String x, String y, Queue<String> missing)
        {
            if (y.startsWith(x)) {
                for (int n = x.length() + 1; n < y.length(); n++) {
                    missing.add(y.substring(0, n));
                }
            }
        }
    }
}
