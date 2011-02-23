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

import com.akiban.util.AkibanAppender;
import com.akiban.util.ArgumentValidation;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import static org.junit.Assert.*;

public class QuoteTest {
    private static final String TEST_STRING = "world\\ isn't this \" a quote?\u0001";
    @Test
    public void testNoneEncoding() {
        StringBuilder sb = new StringBuilder("hello ");
        Quote.NONE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "hello " + TEST_STRING, sb.toString());

    }

    @Test
    public void testDoubleEncoding() {
        StringBuilder sb = new StringBuilder("hello ");
        Quote.DOUBLE_QUOTE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "hello \"world\\\\ isn't this \\\" a quote?\u0001\"", sb.toString());
    }

    @Test
    public void testSingleEncoding() {
        StringBuilder sb = new StringBuilder("hello ");
        Quote.SINGLE_QUOTE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "hello 'world\\\\ isn\\'t this \" a quote?\u0001'", sb.toString());
    }

    @Test
    public void testJSONEncoding() {
        StringBuilder sb = new StringBuilder("hello ");
        Quote.JSON_QUOTE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "hello \"world\\\\ isn't this \\\" a quote?\\u0001\"", sb.toString());
    }

    @Test
    public void writeBytesBasicASCII() {
        doWriteBytesTest("very basic string", "US-ASCII");
    }

    @Test
    public void writeBytesBasicUTF8() {
        doWriteBytesTest("very basic string", "UTF-8");
    }

    @Test(expected=IllegalArgumentException.class)
    public void writeBytesBasicBadEncoding() {
        doWriteBytesTest("very basic string", "UTF-16");
    }

    @Test
    public void writeBytesWithSnowman() {
        doWriteBytesTest("very wintery ☃ string", "UTF-8");
    }

    @Test
    public void snowmanWithPrepend() {
        doWriteBytesTest("very wintery ☃ string", "UTF-8", 15, 0);
    }

    @Test
    public void snowmanWithSuffix() {
        doWriteBytesTest("very wintery ☃ string", "UTF-8", 0, 13);
    }

    @Test
    public void snowmanWithBoth() {
        doWriteBytesTest("very wintery ☃ string", "UTF-8", 15, 13);
    }

    public void doWriteBytesTest(String testString, String charsetName) {
        doWriteBytesTest(testString, charsetName, 0, 0);
    }

    public void doWriteBytesTest(String testString, String charsetName, int preBytes, int postBytes) {
        ArgumentValidation.isGTE("prependBytes", preBytes, 0);

        Charset charset = Charset.forName(charsetName);
        byte[] testBytes = testString.getBytes(charset);
        byte[] expectedBytes = testBytes;

        if (postBytes > 0) {
            assertFalse("last byte was already 0!", testBytes[testBytes.length - 1] == 0);
            byte[] tmp = new byte[testBytes.length + postBytes];
            System.arraycopy(testBytes, 0, tmp, 0, testBytes.length);
            testBytes = tmp;
            assertEquals("last byte", 0, testBytes[testBytes.length - 1]);
        }

        if (preBytes > 0) {
            assertFalse("first byte was already 0!", testBytes[0] == 0);
            byte[] tmp = new byte[testBytes.length + preBytes];
            System.arraycopy(testBytes, 0, tmp, preBytes, testBytes.length);
            testBytes = tmp;
            assertEquals("first byte", 0, testBytes[0]);
        }
        assertEquals("bytes length", expectedBytes.length, testBytes.length - preBytes - postBytes);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintWriter pr = new PrintWriter(os);
        AkibanAppender appender = AkibanAppender.of(os, pr);
        Quote.writeBytes(appender, testBytes, preBytes, expectedBytes.length, charset);
        pr.flush();

        String actualString = new String(os.toByteArray(), charset);
        assertEquals("written string", testString, actualString);
        assertArrayEquals("written bytes", expectedBytes, os.toByteArray());
    }
}
