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

package com.akiban.server;

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.junit.Assert.*;

public final class AkServerUtilTest {
    private static class BadException extends RuntimeException {
        public BadException(Throwable cause) {
            super(cause);
        }
    }

    @Test
    public void decodeUTF8() throws Exception {
        testDecoding("hello snowman: ☃", "UTF-8");
    }

    @Test
    public void decodeACII() throws Exception {
        testDecoding("hello ascii", "US-ASCII");
    }

    @Test
    public void decodeLatin1() throws Exception {
        testDecoding("360 °, plus or minus ±... hopefully better than ½ at least", "latin1");
    }

    @Test
    public void decodeUTF16() throws Exception {
        testDecoding("utf 16 says this is broken: ☃", "UTF-16");
    }

    @Test
    public void decodeNullByteBuffer() {
        String shouldBeNull = AkServerUtil.decodeString(null, null);
        assertNull("null byte buffer", shouldBeNull);
    }

    @Test(expected=IllegalArgumentException.class)
    public void decodeNullCharset() {
        final byte[] someBytes;
        try {
            someBytes = "some bytes".getBytes();
            assertTrue("someBytes was empty!", someBytes.length > 0);
        } catch (NullPointerException e) {
            throw new BadException(e);
        }
        AkServerUtil.decodeString(ByteBuffer.wrap(someBytes), null);
    }

    private static void testDecoding(String testString, String charset) {
        final ByteBuffer buffer;
        try {
            byte[] bytes = testString.getBytes(charset);
            buffer = ByteBuffer.wrap(bytes);
        } catch (UnsupportedEncodingException e) {
            throw new BadException(e);
        }
        String decoded = AkServerUtil.decodeString(buffer, charset);
        assertEquals("test string", testString, decoded);
    }
}
