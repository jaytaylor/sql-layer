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

package com.akiban.server.types.extract;

import org.junit.Test;
import static org.junit.Assert.*;

import com.akiban.server.error.InvalidDateFormatException;

import java.math.BigInteger;

public class DateTimeExtractorTest extends LongExtractorTestBase {
    public DateTimeExtractorTest() {
        super(ExtractorsForDates.DATETIME,
              new TestElement[] {
                new TestElement("0000-00-00 00:00:00", 0),
                new TestElement("1000-01-01 00:00:00", 10000101000000L),
                new TestElement("9999-12-31 23:59:59", 99991231235959L),
                new TestElement("2011-04-10 17:04:03", Long.valueOf(20110410170403L)),
                new TestElement("1986-10-28 05:20:00", BigInteger.valueOf(19861028052000L)),
                // Fraction is allowed but ignored
                new TestElement("2012-04-25 08:52:17", 20120425085217L),
                new TestElement("2012-04-25 08:52:17.999999", 20120425085217L, "2012-04-25 08:52:17"),
                // Timezone is allowed but ignored
                new TestElement("2012-04-27 15:23:00+00:00", 20120427152300L, "2012-04-27 15:23:00"),
                new TestElement("2012-04-27 15:23:00+09:00", 20120427152300L, "2012-04-27 15:23:00"),
                new TestElement("2012-04-27 15:23:00-05:30", 20120427152300L, "2012-04-27 15:23:00"),
                new TestElement("2012-04-27 15:23:00-11:00", 20120427152300L, "2012-04-27 15:23:00"),
                // Both are allowed but ignored
                new TestElement("2012-04-27 15:25:45.123456+5:00", 20120427152545L, "2012-04-27 15:25:45"),
                // Ignore leading/trailing spacing
                new TestElement("  2012-04-27 16:06:12      ", 20120427160612L, "2012-04-27 16:06:12"),
              });
    }

    @Test
    public void onlyDatePart() {
        encodeAndDecode("2000-01-01");
        long actual = ExtractorsForDates.DATETIME.getLong("2000-01-01");
        assertEquals(20000101000000L, actual);
    }

    @Test(expected=InvalidDateFormatException.class)
    public void datePartNotNumber() {
        encodeAndDecode("2011-01-01zebra 00:00:00");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void timePartNotNumber() {
        encodeAndDecode("2011-01-01 00:00:00zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void partialDatePart() {
        encodeAndDecode("2011-01 00:00:00");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void partialTimePart() {
        encodeAndDecode("2011-01-01 00:00");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void noNumbers() {
        encodeAndDecode("zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void tooManyDateParts() {
        encodeAndDecode("2000-01-01-01 00:00:00");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void tooManyTimeParts() {
        encodeAndDecode("2000-01-01 00:00:00:00");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void onlyTimePart() {
        encodeAndDecode("10:11:12");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void noSpaceBetweenDateAndType() {
        encodeAndDecode("2012-04-2711:05:30");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void dateAndOnlyFractionalSeconds() {
        encodeAndDecode("2012-04-27 .123456");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void dateAndOnlyTimezone() {
        encodeAndDecode("2012-04-27 -07:25");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void dateAndOnlyFractionlSecondsTimezone() {
        encodeAndDecode("2012-04-27 .123456+05:30");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void dateAndFractionalNoSpace() {
        encodeAndDecode("12-11-10.123");
    }
}
