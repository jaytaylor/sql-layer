/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public final class FilteringIteratorTest {

    @Test
    public void correctUsageBasic() {
        ArrayList<Integer> list = list(1, 2, 3, 4, 5, 6);
        List<Integer> filtered = dump( onlyEvens(list) );
        assertEquals("filtered list", list(2, 4, 6), filtered);
    }

    // have to filter out more than one in a row
    @Test
    public void correctUsageBasicSparse() {
        ArrayList<Integer> list = list(1, 27, 31, 11, 2, 3, 4, 5, 6);
        List<Integer> filtered = dump( onlyEvens(list) );
        assertEquals("filtered list", list(2, 4, 6), filtered);
    }

    @Test
    public void correctUsageEmptyList() {
        List<Integer> filtered = dump( onlyEvens() );
        assertEquals("filtered list", list(), filtered);
    }

    @Test
    public void correctUsageEmptyAfterFilter() {
        List<Integer> filtered = dump( onlyEvens(1, 3, 7) );
        assertEquals("filtered list", list(), filtered);
    }

    @Test
    public void correctUsageRemove() {
        ArrayList<Integer> list = list(1, 2, 3, 4, 5, 6);
        Iterator<Integer> iterator = onlyEvens(list, true);
        List<Integer> removed = new ArrayList<>();
        while (iterator.hasNext()) {
            removed.add( iterator.next() );
            iterator.remove();
        }
        assertEquals("removed", list(2, 4, 6), removed);
        assertEquals("left", list(1, 3, 5), list);
    }

    @Test(expected=NoSuchElementException.class)
    public void nextOnEmpty() {
        Iterator<Integer> iterator = onlyEvens();
        iterator.next();
    }

    @Test(expected=NoSuchElementException.class)
    public void nextOnEmptyAfterFilter() {
        Iterator<Integer> iterator = onlyEvens(1);
        iterator.next();
    }

    @Test
    public void nextTwice() {
        Iterator<Integer> iterator = onlyEvens(1, 2, 3, 4, 5);
        assertEquals("first", 2, iterator.next().intValue());
        assertEquals("second", 4, iterator.next().intValue());
        assertFalse("hasNext() == true", iterator.hasNext());
    }

    @Test(expected=IllegalStateException.class)
    public void removeTwice() {
        Iterator<Integer> iterator = SafeAction.of(new SafeAction.Get<Iterator<Integer>>() {
            @Override
            public Iterator<Integer> get() {
                Iterator<Integer> iterator = onlyEvens(1, 2);
                assertTrue("hasNext == false", iterator.hasNext());
                assertEquals("next()", 2, iterator.next().intValue());
                iterator.remove();
                return iterator;
            }
        });
        iterator.remove();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void removeDisallowedByIterator() {
        Iterator<Integer> iterator = SafeAction.of(new SafeAction.Get<Iterator<Integer>>() {
            @Override
            public Iterator<Integer> get() {
                Iterator<Integer> iterator = onlyEvens(list(2), false);
                assertTrue("hasNext == false", iterator.hasNext());
                assertEquals("next()", 2, iterator.next().intValue());
                return iterator;
            }
        });
        iterator.remove();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void removeDisallowedByDelegate() {
        Iterator<Integer> iterator = SafeAction.of(new SafeAction.Get<Iterator<Integer>>() {
            @Override
            public Iterator<Integer> get() {
                Iterator<Integer> iterator = onlyEvens(Collections.unmodifiableList(list(2)), false);
                assertTrue("hasNext == false", iterator.hasNext());
                assertEquals("next()", 2, iterator.next().intValue());
                return iterator;
            }
        });
        iterator.remove();
    }

    @Test
    public void hasNextTwice() {
        Iterator<Integer> iterator = onlyEvens(1, 2, 3);
        assertTrue("hasNext 1", iterator.hasNext());
        assertTrue("hasNext 2", iterator.hasNext());
        assertEquals("next", 2, iterator.next().intValue());
        assertFalse("hasNext 3", iterator.hasNext());
    }

    private static Iterator<Integer> onlyEvens(int... integers) {
        return onlyEvens(list(integers));
    }

    private static Iterator<Integer> onlyEvens(Iterable<Integer> iterable) {
        return onlyEvens(iterable, true);
    }

    private static Iterator<Integer> onlyEvens(Iterable<Integer> iterable, boolean mutable) {
        return new FilteringIterator<Integer>(iterable.iterator(), mutable) {
            @Override
            protected boolean allow(Integer item) {
                return (item % 2) == 0;
            }
        };
    }

    private static List<Integer> dump(Iterator<Integer> iterator) {
        ArrayList<Integer> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    private static ArrayList<Integer> list(int... values) {
        ArrayList<Integer> list = new ArrayList<>(values.length);
        for (int value : values) {
            list.add(value);
        }
        return list;
    }
}
