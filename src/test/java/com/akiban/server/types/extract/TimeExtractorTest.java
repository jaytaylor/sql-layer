
package com.akiban.server.types.extract;

import org.junit.Assert;
import org.junit.Test;

import com.akiban.server.error.InvalidDateFormatException;

public class TimeExtractorTest extends LongExtractorTestBase {
    public TimeExtractorTest() {
        super(ExtractorsForDates.TIME,
              new TestElement[] {
                new TestElement("00:00:00", 0),
                new TestElement("00:00:01", 1),
                new TestElement("-00:00:01", -1),
                new TestElement("838:59:59", 8385959),
                new TestElement("-838:59:59", -8385959),
                new TestElement("14:20:32", 142032),
                new TestElement("-147:21:01", -1472101L)
              });
    }


    @Test
    public void partiallySpecified() {
        Assert.assertEquals("00:00:02", encodeAndDecode("2"));
        Assert.assertEquals("00:00:20", encodeAndDecode("20"));
        Assert.assertEquals("00:03:21", encodeAndDecode("201"));
        Assert.assertEquals("00:33:31", encodeAndDecode("2011"));
        Assert.assertEquals("00:05:42", encodeAndDecode("5:42"));
        Assert.assertEquals("-00:00:42", encodeAndDecode("-42"));
        Assert.assertEquals("-00:10:02", encodeAndDecode("-10:02"));
    }

    @Test(expected=InvalidDateFormatException.class)
    public void invalidNumber() {
        encodeAndDecode("20111zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void noNumbers() {
        encodeAndDecode("zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void tooManyParts() {
        encodeAndDecode("01:02:03:04");
    }
}
