
package com.akiban.util;

import org.junit.Test;

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
