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

import static junit.framework.Assert.assertEquals;

public final class StringsTest {
    @Test
    public void testStringToBytes() {
        ByteSource actual = Strings.parseHex("0xBEEFCAFE");
        int[] expectedInts = { 190, 239, 202, 254 };
        byte[] expected = new byte[expectedInts.length];
        for (int i=0; i < expected.length; ++i) {
            expected[i] = (byte)expectedInts[i];
        }

        assertEquals("bytes", new WrappingByteSource(expected), actual);
    }
    
    @Test
    public void withSpace() {
        ByteSource actual = Strings.parseHex("0x BE EFCA FE");
        int[] expectedInts = { 190, 239, 202, 254 };
        byte[] expected = new byte[expectedInts.length];
        for (int i=0; i < expected.length; ++i) {
            expected[i] = (byte)expectedInts[i];
        }

        assertEquals("bytes", new WrappingByteSource(expected), actual);
    }
}
