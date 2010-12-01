package com.akiban.cserver.encoding;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static junit.framework.Assert.*;

public final class DecimalEncoderTest {
    @Test
    public void t1() {
        doTest("1234567890.1234", 14, 4, "0x810DFB38D204D2");
    }

    @Test
    public void t2() {
        doTest("90.1956251262", 12, 10, "0xDA0BA900A602");
    }

    private static void doTest(String expected, int precision, int scale, String bytesHex) {
        byte[] bytes = bytes(bytesHex);
        StringBuilder sb = new StringBuilder();
        BigDecimal actual = DecimalEncoder.decodeAndParse(bytes, 0, precision, scale, sb);

        assertEquals("BigDecimal", new BigDecimal(expected), actual);
        assertEquals("String", expected, sb.toString());
    }

    private static byte[] bytes(String string) {
        if (!string.startsWith("0x")) {
            throw new RuntimeException("not a hex string");
        }
        if ( 0!= (string.length() % 2)) {
            throw new RuntimeException("string must have even number of chars");
        }

        byte[] ret = new byte[ (string.length()-2) / 2 ];
        for (int i=0; i < ret.length; ++i) {
            int high = (Character.digit(string.charAt(i*2+2), 16)) << 4;
            int low = Character.digit(string.charAt(i*2+3), 16);
            ret[i] = (byte) (low + high);
        }
        return ret;
    }

    @Test
    public void testStringToBytes() {
        byte[] actual = bytes("0xBEEFCAFE");
        int[] expectedInts = { 190, 239, 202, 254 };
        byte[] expected = new byte[expectedInts.length];
        for (int i=0; i < expected.length; ++i) {
            expected[i] = (byte)expectedInts[i];
        }

        assertEquals("bytes", Arrays.toString(expected), Arrays.toString(actual));
    }
}
