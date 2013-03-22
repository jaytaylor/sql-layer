
package com.akiban.server.types.extract;

import org.junit.Test;

import com.akiban.server.error.InvalidDateFormatException;

public class TimestampExtractorTest extends LongExtractorTestBase {
    public TimestampExtractorTest() {
        super(ExtractorsForDates.TIMESTAMP,
              new TestElement[] {
                new TestElement("0000-00-00 00:00:00", 0),
                new TestElement("1970-01-01 00:00:01", 1),
                new TestElement("2009-02-13 23:31:30", 1234567890),
                new TestElement("2009-02-13 23:31:30", 1234567890),
                new TestElement("2038-01-19 03:14:07", 2147483647),
                new TestElement("1986-10-28 00:00:00", 530841600),
                new TestElement("2011-04-10 18:34:00", 1302460440L)
              });

        // Make expected output deterministic
        ConverterTestUtils.setGlobalTimezone("UTC");
    }


    @Test(expected=InvalidDateFormatException.class)
    public void invalidNumber() {
        encodeAndDecode("20111zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void noNumbers() {
        encodeAndDecode("zebra");
    }
}
