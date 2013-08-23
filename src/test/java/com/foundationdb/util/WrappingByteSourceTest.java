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

import java.nio.ByteBuffer;
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
    public void simpleWrap() {
        byte[] bytes = new byte[10];
        check(wrap(bytes), bytes, 0, 10);
    }

    @Test
    public void offsetAndLengthZero() {
        byte[] bytes = new byte[10];
        check(wrap(bytes, 0, 0), bytes, 0, 0);
    }

    @Test
    public void offsetSizeAndLengthZero() {
        byte[] bytes = new byte[0];
        check(wrap(bytes, 0, 0), bytes, 0, 0);
    }

    @Test
    public void offsetAtEdge() {
        byte[] bytes = new byte[10];
        check(wrap(bytes, 9, 0), bytes, 9, 0);
        check(wrap(bytes, 10, 0), bytes, 10, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void offsetPastEdge() {
        wrap(new byte[10], 11, 0);
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

    @Test
    public void byteBufferConversion() {
        byte[] bytes = new byte[10];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 3, 4);
        WrappingByteSource converted = WrappingByteSource.fromByteBuffer(byteBuffer);
        WrappingByteSource manual = new WrappingByteSource().wrap(bytes, 3, 4);
        assertEquals("converted WrappingByteSource", manual, converted);
    }

    @Test
    public void equality() {
        byte[] bytes = new byte[4*11];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        for(int i=0; i < 5; ++i) {
            buffer.putInt(i);
        }
        buffer.putInt(-1); // space
        for(int i=0; i < 5; ++i) {
            buffer.putInt(i);
        }
        WrappingByteSource one = new WrappingByteSource().wrap(bytes, 0, 4*5);
        WrappingByteSource two = new WrappingByteSource().wrap(bytes, 6*4, 4*5);
        assertEquals("equality", one, two);
        assertEquals("hash codes", one.hashCode(), two.hashCode());
    }

    @Test
    public void equalityShort() {
        WrappingByteSource one = new WrappingByteSource().wrap(new byte[]{(byte)0xAB});
        WrappingByteSource two = new WrappingByteSource().wrap(new byte[]{(byte)0xAB});
        assertEquals("equality", one, two);
        assertEquals("hash codes", one.hashCode(), two.hashCode());
    }

    @Test
    public void equalityEmpty() {
        WrappingByteSource one = new WrappingByteSource().wrap(new byte[0]);
        WrappingByteSource two = new WrappingByteSource().wrap(new byte[0]);
        assertEquals("equality", one, two);
        assertEquals("hash codes", one.hashCode(), two.hashCode());
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

    private static WrappingByteSource wrap(byte[] bytes) {
        return new WrappingByteSource().wrap(bytes);
    }

    private static WrappingByteSource wrap(byte[] bytes, int offset, int length) {
        return new WrappingByteSource().wrap(bytes, offset, length);
    }
}
