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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class SparseArrayTest {

    @Test
    public void getCreates() {
        CountingSparseArray tester = new CountingSparseArray();
        assertEquals("first get", "1", tester.get(0));
        assertEquals("first get", "2", tester.get(100));
        assertEquals("create count", 2, tester.count);
    }

    @Test
    public void getReuses() {
        CountingSparseArray tester = new CountingSparseArray();
        assertEquals("first get", "1", tester.get(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("create count", 1, tester.count);
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
