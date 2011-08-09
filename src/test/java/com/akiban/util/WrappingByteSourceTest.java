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

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class WrappingByteSourceTest {

    @Test
    public void totallyFine() {
        byte[] bytes = new byte[10];
        check(wrap(bytes, 3, 5), bytes, 3, 5);
    }

    @Test
    public void offsetAndLengthZero() {
        byte[] bytes = new byte[10];
        check(wrap(bytes, 0, 0), bytes, 0, 0);
    }

    @Test
    public void offsetAtEdge() {
        byte[] bytes = new byte[10];
        check(wrap(bytes, 9, 0), bytes, 9, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void offsetPastEdge() {
        wrap(new byte[10], 10, 0);
    }

    @Test
    public void lengthAtEdge() {
        byte[] bytes = new byte[10];
        check(wrap(bytes, 0, 10), bytes, 0, 10);
        check(wrap(bytes, 1, 9), bytes, 1, 9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void lengthPastEdge() {
        wrap(new byte[10], 1, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullBytes() {
        wrap(null, 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeOffset() {
        wrap(new byte[10], -1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeLength() {
        wrap(new byte[10], 0, -1);
    }

    private static void check(ByteSource byteSource, byte[] expectedBytes, int expectedOffset, int expectedLength) {
        byte[] actualBytes = byteSource.byteArray();
        if (actualBytes != expectedBytes) {
            fail("Not same instance: " + stringify(actualBytes) + " but expected " + stringify(expectedBytes));
        }
        assertEquals("offset", expectedOffset, byteSource.byteArrayOffset());
        assertEquals("length", expectedLength, byteSource.byteArrayLength());
    }

    private static String stringify(byte[] bytes) {
        return Arrays.toString(bytes);
    }

    private static WrappingByteSource wrap(byte[] bytes, int offset, int length) {
        return new WrappingByteSource().wrap(bytes, offset, length);
    }
}
