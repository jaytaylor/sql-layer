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
