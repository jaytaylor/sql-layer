package com.akiban.cserver;

import com.akiban.util.AkibanAppender;
import org.junit.Test;

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
}
