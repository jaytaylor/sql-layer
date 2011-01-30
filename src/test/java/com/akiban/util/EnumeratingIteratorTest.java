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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public final class EnumeratingIteratorTest
{
    @Test
    public void testOf()
    {
        List<Character> list = Arrays.asList('a', 'b', 'c', 'd');

        int localCount = 0;
        for (Enumerated<Character> countingChar : EnumeratingIterator.of(list))
        {
            int counted = countingChar.count();
            assertEquals("count", localCount, counted);
            assertEquals("value", list.get(localCount), countingChar.get());
            ++localCount;
        }
    }

    @Test(expected=UnsupportedOperationException.class)
    public void cannotRemove()
    {
        List<Character> list = Arrays.asList('e', 'f', 'g', 'h');
        Iterator<Enumerated<Character>> iterator = EnumeratingIterator.of(list).iterator();

        assertTrue("has next", iterator.hasNext());
        assertEquals("value", Character.valueOf('e'), iterator.next().get());
        iterator.remove();
    }
}
