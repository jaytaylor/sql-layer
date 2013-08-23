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

package com.foundationdb.server;

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public final class AkServerUtilTest {
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
        someBytes = "some bytes".getBytes();
        assertTrue("someBytes was empty!", someBytes.length > 0);
        AkServerUtil.decodeString(ByteBuffer.wrap(someBytes), null);
    }

    private static void testDecoding(String testString, String charset) {
        final ByteBuffer buffer;
        try {
            byte[] bytes = testString.getBytes(charset);
            buffer = ByteBuffer.wrap(bytes);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        String decoded = AkServerUtil.decodeString(buffer, charset);
        assertEquals("test string", testString, decoded);
    }

    private static byte[] byteArray(int... values) {
        byte[] bytes = new byte[values.length];
        for(int i = 0; i < values.length; ++i) {
            bytes[i] = (byte)values[i];
        }
        return bytes;
    }

    @Test
    public void getSignedByte() {
        assertEquals(   0, AkServerUtil.getByte(byteArray(0x00), 0));
        assertEquals(   1, AkServerUtil.getByte(byteArray(0x01), 0));
        assertEquals( 127, AkServerUtil.getByte(byteArray(0x7F), 0));
        assertEquals(-128, AkServerUtil.getByte(byteArray(0x80), 0));
        assertEquals(  -2, AkServerUtil.getByte(byteArray(0xFE), 0));
        assertEquals(  -1, AkServerUtil.getByte(byteArray(0xFF), 0));
    }

    @Test
    public void getUnsignedByte() {
        assertEquals(  0, AkServerUtil.getUByte(byteArray(0x00), 0));
        assertEquals(  1, AkServerUtil.getUByte(byteArray(0x01), 0));
        assertEquals(127, AkServerUtil.getUByte(byteArray(0x7F), 0));
        assertEquals(128, AkServerUtil.getUByte(byteArray(0x80), 0));
        assertEquals(254, AkServerUtil.getUByte(byteArray(0xFE), 0));
        assertEquals(255, AkServerUtil.getUByte(byteArray(0xFF), 0));
    }

    @Test
    public void getSignedShort() {
        assertEquals(     0, AkServerUtil.getShort(byteArray(0x00, 0x00), 0));
        assertEquals(     1, AkServerUtil.getShort(byteArray(0x01, 0x00), 0));
        assertEquals( 32767, AkServerUtil.getShort(byteArray(0xFF, 0x7F), 0));
        assertEquals(-32768, AkServerUtil.getShort(byteArray(0x00, 0x80), 0));
        assertEquals(    -2, AkServerUtil.getShort(byteArray(0xFE, 0xFF), 0));
        assertEquals(    -1, AkServerUtil.getShort(byteArray(0xFF, 0xFF), 0));
    }

    @Test
    public void getUnsignedShort() {
        assertEquals(    0, AkServerUtil.getUShort(byteArray(0x00, 0x00), 0));
        assertEquals(    1, AkServerUtil.getUShort(byteArray(0x01, 0x00), 0));
        assertEquals(32767, AkServerUtil.getUShort(byteArray(0xFF, 0x7F), 0));
        assertEquals(32768, AkServerUtil.getUShort(byteArray(0x00, 0x80), 0));
        assertEquals(65534, AkServerUtil.getUShort(byteArray(0xFE, 0xFF), 0));
        assertEquals(65535, AkServerUtil.getUShort(byteArray(0xFF, 0xFF), 0));
    }

    @Test
    public void getSignedMedium() {
        assertEquals(       0, AkServerUtil.getMediumInt(byteArray(0x00, 0x00, 0x00), 0));
        assertEquals(       1, AkServerUtil.getMediumInt(byteArray(0x01, 0x00, 0x00), 0));
        assertEquals( 8388607, AkServerUtil.getMediumInt(byteArray(0xFF, 0xFF, 0x7F), 0));
        assertEquals(-8388608, AkServerUtil.getMediumInt(byteArray(0x00, 0x00, 0x80), 0));
        assertEquals(      -2, AkServerUtil.getMediumInt(byteArray(0xFE, 0xFF, 0xFF), 0));
        assertEquals(      -1, AkServerUtil.getMediumInt(byteArray(0xFF, 0xFF, 0xFF), 0));
    }

    @Test
    public void getUnsignedMedium() {
        assertEquals(       0, AkServerUtil.getUMediumInt(byteArray(0x00, 0x00, 0x00), 0));
        assertEquals(       1, AkServerUtil.getUMediumInt(byteArray(0x01, 0x00, 0x00), 0));
        assertEquals( 8388607, AkServerUtil.getUMediumInt(byteArray(0xFF, 0xFF, 0x7F), 0));
        assertEquals( 8388608, AkServerUtil.getUMediumInt(byteArray(0x00, 0x00, 0x80), 0));
        assertEquals(16777214, AkServerUtil.getUMediumInt(byteArray(0xFE, 0xFF, 0xFF), 0));
        assertEquals(16777215, AkServerUtil.getUMediumInt(byteArray(0xFF, 0xFF, 0xFF), 0));
    }

    @Test
    public void getSignedInt() {
        assertEquals(          0, AkServerUtil.getInt(byteArray(0x00, 0x00, 0x00, 0x00), 0));
        assertEquals(          1, AkServerUtil.getInt(byteArray(0x01, 0x00, 0x00, 0x00), 0));
        assertEquals( 2147483647, AkServerUtil.getInt(byteArray(0xFF, 0xFF, 0xFF, 0x7F), 0));
        assertEquals(-2147483648, AkServerUtil.getInt(byteArray(0x00, 0x00, 0x00, 0x80), 0));
        assertEquals(         -2, AkServerUtil.getInt(byteArray(0xFE, 0xFF, 0xFF, 0xFF), 0));
        assertEquals(         -1, AkServerUtil.getInt(byteArray(0xFF, 0xFF, 0xFF, 0xFF), 0));
    }

    @Test
    public void getUnsignedInt() {
        assertEquals(         0L, AkServerUtil.getUInt(byteArray(0x00, 0x00, 0x00, 0x00), 0));
        assertEquals(         1L, AkServerUtil.getUInt(byteArray(0x01, 0x00, 0x00, 0x00), 0));
        assertEquals(2147483647L, AkServerUtil.getUInt(byteArray(0xFF, 0xFF, 0xFF, 0x7F), 0));
        assertEquals(2147483648L, AkServerUtil.getUInt(byteArray(0x00, 0x00, 0x00, 0x80), 0));
        assertEquals(4294967294L, AkServerUtil.getUInt(byteArray(0xFE, 0xFF, 0xFF, 0xFF), 0));
        assertEquals(4294967295L, AkServerUtil.getUInt(byteArray(0xFF, 0xFF, 0xFF, 0xFF), 0));
    }

    @Test
    public void getSignedLong() {
        assertEquals(                    0, AkServerUtil.getLong(byteArray(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00), 0));
        assertEquals(                    1, AkServerUtil.getLong(byteArray(0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00), 0));
        assertEquals( 9223372036854775807L, AkServerUtil.getLong(byteArray(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F), 0));
        assertEquals(-9223372036854775808L, AkServerUtil.getLong(byteArray(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80), 0));
        assertEquals(                   -2, AkServerUtil.getLong(byteArray(0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 0));
        assertEquals(                   -1, AkServerUtil.getLong(byteArray(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 0));
    }

    @Test
    public void getUnsignedLong() {
        assertEquals(new BigInteger("0"),
                     AkServerUtil.getULong(byteArray(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00), 0));
        assertEquals(new BigInteger("1"),
                     AkServerUtil.getULong(byteArray(0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00), 0));
        assertEquals(new BigInteger("9223372036854775807"),
                     AkServerUtil.getULong(byteArray(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F), 0));
        assertEquals(new BigInteger("9223372036854775808"),
                     AkServerUtil.getULong(byteArray(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80), 0));
        assertEquals(new BigInteger("18446744073709551614"),
                     AkServerUtil.getULong(byteArray(0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 0));
        assertEquals(new BigInteger("18446744073709551615"),
                     AkServerUtil.getULong(byteArray(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF), 0));
    }
}
