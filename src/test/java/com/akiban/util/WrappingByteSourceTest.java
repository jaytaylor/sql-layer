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

package com.akiban.util;

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
