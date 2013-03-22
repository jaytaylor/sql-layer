
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
        checkIteration(new MultiIterator<>(iterator()));
        checkIteration(new MultiIterator<>(iterator(), iterator()));
        checkIteration(new MultiIterator<>(iterator(), iterator(), iterator()));
    }

    @Test
    public void testNonEmptyIterators()
    {
        checkIteration(new MultiIterator<>(iterator(1)),
                       1);
        checkIteration(new MultiIterator<>(iterator(1), iterator(2, 3)),
                       1, 2, 3);
        checkIteration(new MultiIterator<>(iterator(1), iterator(2, 3), iterator(4, 5, 6)),
                       1, 2, 3, 4, 5, 6);
        checkIteration(new MultiIterator<>(iterator(1), iterator(2, 3), iterator(4, 5, 6), iterator()),
                       1, 2, 3, 4, 5, 6);
        checkIteration(new MultiIterator<>(iterator(1), iterator(2, 3), iterator(4, 5, 6), iterator(), iterator()),
                       1, 2, 3, 4, 5, 6);
        checkIteration(new MultiIterator<>(iterator(), iterator(1), iterator(2, 3), iterator(4, 5, 6)),
                       1, 2, 3, 4, 5, 6);
        checkIteration(new MultiIterator<>(iterator(), iterator(), iterator(1), iterator(2, 3), iterator(4, 5, 6)),
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
