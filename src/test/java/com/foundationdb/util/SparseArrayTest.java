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

import java.util.List;

import static org.junit.Assert.*;

public final class SparseArrayTest {

    @Test
    public void getCreates() {
        CountingSparseArray tester = new CountingSparseArray();

        assertEquals("isDefined", false, tester.isDefined(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("isDefined", true, tester.isDefined(0));

        assertEquals("isDefined", false, tester.isDefined(5));
        assertEquals("second get", "2", tester.get(5));
        assertEquals("isDefined", true, tester.isDefined(5));

        assertEquals("create count", 2, tester.count);
    }
    
    @Test
    public void getGrowsCapacity() {
        CountingSparseArray tester = new CountingSparseArray();
        int oldCapacity = tester.currentCapacity();

        int index = oldCapacity + 10;
        assertEquals("isDefined", false, tester.isDefined(index));
        assertEquals("first get", "1", tester.get(index));
        assertEquals("isDefined", true, tester.isDefined(index));

        int newCapacity = tester.currentCapacity();
        assertTrue("capacity <= index: " + newCapacity+ " <= " + index, newCapacity > index);

        assertEquals("create count", 1, tester.count);
    }

    @Test
    public void getReuses() {
        CountingSparseArray tester = new CountingSparseArray();

        assertEquals("isDefined", false, tester.isDefined(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("isDefined", true, tester.isDefined(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("isDefined", true, tester.isDefined(0));
        assertEquals("create count", 1, tester.count);
    }
    
    @Test
    public void setIndex() {
        CountingSparseArray tester = new CountingSparseArray();

        assertEquals("isDefined", false, tester.isDefined(0));
        tester.set(0, "foo");
        assertEquals("create count", 0, tester.count);
        assertEquals("get", "foo", tester.get(0));
        assertEquals("create count", 0, tester.count);
        assertEquals("isDefined", true, tester.isDefined(0));
        
        String old = tester.set(0, "bar");
        assertEquals("get", "bar", tester.get(0));
        assertEquals("get", "foo", old);
        assertEquals("isDefined", true, tester.isDefined(0));
        assertEquals("create count", 0, tester.count);
    }
    
    @Test
    public void setGrowsCapacity() {
        CountingSparseArray tester = new CountingSparseArray();
        int oldCapacity = tester.currentCapacity();

        int index = oldCapacity + 10;
        assertEquals("isDefined", false, tester.isDefined(index));
        String old = tester.set(index, "foo");
        assertEquals("old value", null, old);
        assertEquals("isDefined", true, tester.isDefined(index));

        int newCapacity = tester.currentCapacity();
        assertTrue("capacity <= index: " + newCapacity+ " <= " + index, newCapacity > index);

        assertEquals("create count", 0, tester.count);
    }

    @Test
    public void toListIsCompact() {
        SparseArray<Integer> array = new SparseArray<>();
        array.set(0, 10);
        array.set(1, 11);
        List<Integer> list = array.toList();
        // Internal size may be bigger, but only get defined elements
        assertEquals("list size", 2, list.size());
        assertEquals("list get(0)", Integer.valueOf(10), list.get(0));
        assertEquals("list get(1)", Integer.valueOf(11), list.get(1));
    }

    @Test(expected=IllegalArgumentException.class)
    public void toListMustBeCompact() {
        SparseArray<Integer> array = new SparseArray<>();
        array.set(5, 50);
        array.toList();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void negativeIndex() {
        new CountingSparseArray().get(-1);
    }

    private static class CountingSparseArray extends SparseArray<String> {
        @Override
        protected String initialValue() {
            return Integer.toString(++count);
        }

        int count = 0;
    }
}
