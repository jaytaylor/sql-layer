/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server;

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
