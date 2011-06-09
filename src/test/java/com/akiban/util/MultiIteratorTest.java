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
