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

import com.akiban.util.AkibanAppender;
import com.akiban.util.ArgumentValidation;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import static org.junit.Assert.*;

public class QuoteTest {
    private static final String TEST_STRING = "world\\ isn't this \" a quote?\u0001";
    @Test
    public void testNoneEncoding() {
        StringBuilder sb = new StringBuilder();
        Quote.NONE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", TEST_STRING, sb.toString());

    }

    @Test
    public void testDoubleEncoding() {
        StringBuilder sb = new StringBuilder();
        Quote.DOUBLE_QUOTE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "world\\\\ isn't this \\\" a quote?\u0001", sb.toString());
    }

    @Test
    public void testSingleEncoding() {
        StringBuilder sb = new StringBuilder();
        Quote.SINGLE_QUOTE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "world\\\\ isn\\'t this \" a quote?\u0001", sb.toString());
    }

    @Test
    public void testJSONEncoding() {
        StringBuilder sb = new StringBuilder();
        Quote.JSON_QUOTE.append(AkibanAppender.of(sb), TEST_STRING);

        assertEquals("encoded string", "world\\\\ isn't this \\\" a quote?\\u0001", sb.toString());
    }

    @Test
    public void writeBytesBasicASCII_NONE() {
        doWriteBytesTest("very basic string", "US-ASCII", "very basic string", Quote.NONE);
    }

    @Test
    public void writeBytesBasicUTF8_NONE() {
        doWriteBytesTest("very basic string", "UTF-8", "very basic string", Quote.NONE);
    }
    @Test
    public void writeBytesBasicUTF8_DOUBLE() {
        doWriteBytesTest("very basic string", "UTF-8", "very basic string", Quote.DOUBLE_QUOTE);
    }

    @Test(expected=IllegalArgumentException.class)
    public void writeBytesBasicBadEncoding() throws UnsupportedEncodingException {

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintWriter pr = new PrintWriter(os);
        AkibanAppender appender = AkibanAppender.of(os, pr, null);
        byte[] bytes = "some string".getBytes("UTF-16");
        Quote.writeBytes(appender, bytes, 0, bytes.length, Charset.forName("UTF-16"), Quote.NONE);
    }

    @Test
    public void writeBytesWithSnowman_JSON() {
        doWriteBytesTest("very wintery ☃ string", "UTF-8", "very wintery \\u2603 string", Quote.JSON_QUOTE);
    }

    @Test
    public void writeBytesJSONControlChars() {
        doWriteBytesTest("very newline \n string", "UTF-8", "very newline \\n string", Quote.JSON_QUOTE);
    }

    @Test
    public void snowmanWithPrepend() {
        doWriteBytesTest("very wintery ☃ string", "UTF-8", 15, 0, "very wintery ☃ string", Quote.NONE);
    }

    @Test
    public void snowmanWithSuffix() {
        doWriteBytesTest("very wintery ☃ string", "UTF-8", 0, 13, "very wintery ☃ string", Quote.NONE);
    }

    @Test
    public void snowmanWithBoth() {
        doWriteBytesTest("very wintery ☃ string", "UTF-8", 15, 13, "very wintery ☃ string", Quote.NONE);
    }

    public void doWriteBytesTest(String testString, String charsetName, String expectedString, Quote quote) {
        doWriteBytesTest(testString, charsetName, 0, 0, expectedString, quote);
    }

    public void doWriteBytesTest(String testString, String charsetName, int preBytes, int postBytes,
                                 String expectedString, Quote quote) {
        ArgumentValidation.isGTE("prependBytes", preBytes, 0);

        Charset charset = Charset.forName(charsetName);
        byte[] testBytes = testString.getBytes(charset);

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

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintWriter pr;
        try {
          pr = new PrintWriter(new OutputStreamWriter(os, "UTF-8"));
        }
        catch (UnsupportedEncodingException ex) {
          throw new RuntimeException(ex);
        }
        AkibanAppender appender = AkibanAppender.of(os, pr, null);
        Quote.writeBytes(appender, testBytes, preBytes, testBytes.length - preBytes - postBytes, charset, quote);
        pr.flush();

        String actualString = new String(os.toByteArray(), charset);
        assertEquals("written string", expectedString, actualString);
        assertArrayEquals("written bytes", expectedString.getBytes(charset), os.toByteArray());
    }
}
