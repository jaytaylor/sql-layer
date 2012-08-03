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

package com.akiban.server.types3.aksql.aktypes;

import com.akiban.server.types3.aksql.aktypes.AkInterval.AkIntervalMonthsFormat;
import com.akiban.server.types3.aksql.aktypes.AkInterval.AkIntervalSecondsFormat;
import com.akiban.server.types3.aksql.aktypes.AkInterval.IntervalFormat;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class AkIntervalTest {

    @Test
    public void conversionsForLong() {
        long nanos = new TimeBuilder()
                .add(3, TimeUnit.DAYS)
                .add(5, TimeUnit.HOURS)
                .add(13, TimeUnit.MINUTES)
                .add(17, TimeUnit.SECONDS)
                .add(43, TimeUnit.MICROSECONDS)
                .add(666, TimeUnit.NANOSECONDS) // will be truncated, since we're working with micros
                .getNanos();
        long raw = AkInterval.secondsRawFrom(nanos, TimeUnit.NANOSECONDS);

        long amount; // will go from days, to hours, etc to nanos
        amount = checkAndGet(raw, TimeUnit.DAYS, 3);
        amount = checkAndGet(raw, TimeUnit.HOURS, (amount*24) + 5);
        amount = checkAndGet(raw, TimeUnit.MINUTES, (amount*60) + 13);
        amount = checkAndGet(raw, TimeUnit.SECONDS, (amount*60) + 17);
        amount = checkAndGet(raw, TimeUnit.MICROSECONDS, (amount*1000000) + 43);
        checkAndGet(raw, TimeUnit.NANOSECONDS, amount*1000); // the 666 extra were truncated
    }

    @Test
    public void testDaysParser() {
        new TimeBuilder(3, TimeUnit.DAYS)
                .checkParse(AkIntervalSecondsFormat.DAY, "3")
                .badParse(AkIntervalSecondsFormat.DAY, "3-4");
    }

    @Test
    public void testHoursParser() {
        new TimeBuilder(3, TimeUnit.HOURS)
                .checkParse(AkIntervalSecondsFormat.HOUR, "3")
                .badParse(AkIntervalSecondsFormat.HOUR, "3-4");
    }

    @Test
    public void testMinutesParser() {
        new TimeBuilder(43, TimeUnit.MINUTES)
                .checkParse(AkIntervalSecondsFormat.MINUTE, "43")
                .badParse(AkIntervalSecondsFormat.MINUTE, "43-4");
    }

    @Test
    public void testSecondsParser() {
        new TimeBuilder(11, TimeUnit.SECONDS)
                .checkParse(AkIntervalSecondsFormat.SECOND, "11")
                .badParse(AkIntervalSecondsFormat.SECOND, "11-4");
    }

    @Test
    public void testSecondsFractionalParser() {
        new TimeBuilder(123456, TimeUnit.MICROSECONDS)
                .checkParse(AkIntervalSecondsFormat.SECOND, "0.123456")
                .checkParse(AkIntervalSecondsFormat.SECOND, "0.1234567") // 7 should get truncated
                .checkParse(AkIntervalSecondsFormat.SECOND, ".123456")
                .badParse(AkIntervalSecondsFormat.SECOND, "4 5");
    }

    @Test
    public void testDaysToHoursParser() {
        new TimeBuilder(2, TimeUnit.DAYS).add(5, TimeUnit.HOURS)
                .checkParse(AkIntervalSecondsFormat.DAY_HOUR, "2 5")
                .badParse(AkIntervalSecondsFormat.DAY_HOUR, "2");
    }

    @Test
    public void testDaysToMinutesParser() {
        new TimeBuilder(2, TimeUnit.DAYS).add(5, TimeUnit.HOURS).add(13, TimeUnit.MINUTES)
                .checkParse(AkIntervalSecondsFormat.DAY_MINUTE, "2 5:13")
                .badParse(AkIntervalSecondsFormat.DAY_MINUTE, "2");
    }

    @Test
    public void testDaysToSecondsParser() {
        new TimeBuilder(2, TimeUnit.DAYS).add(5, TimeUnit.HOURS).add(13, TimeUnit.MINUTES).add(27, TimeUnit.SECONDS)
                .checkParse(AkIntervalSecondsFormat.DAY_SECOND, "2 5:13:27")
                .badParse(AkIntervalSecondsFormat.DAY_SECOND, "2");
    }

    @Test
    public void testHoursToMinutesParser() {
        new TimeBuilder(35, TimeUnit.HOURS).add(13, TimeUnit.MINUTES)
                .checkParse(AkIntervalSecondsFormat.HOUR_MINUTE, "35:13")
                .badParse(AkIntervalSecondsFormat.HOUR_MINUTE, "2");
    }

    @Test
    public void testHoursToSecondsParser() {
        new TimeBuilder(35, TimeUnit.HOURS).add(13, TimeUnit.MINUTES).add(52, TimeUnit.SECONDS)
                .checkParse(AkIntervalSecondsFormat.HOUR_SECOND, "35:13:52")
                .badParse(AkIntervalSecondsFormat.HOUR_SECOND, "2");
    }

    @Test
    public void testMinutesToSecondsParser() {
        new TimeBuilder(14, TimeUnit.MINUTES).add(52, TimeUnit.SECONDS)
                .checkParse(AkIntervalSecondsFormat.MINUTE_SECOND, "14:52")
                .badParse(AkIntervalSecondsFormat.MINUTE_SECOND, "2");
    }

    @Test
    public void testYearsParser() {
        monthsFormatsTested.add(AkIntervalMonthsFormat.YEAR);
        assertEquals("years", (43*12), AkIntervalMonthsFormat.YEAR.parse("43"));
        badParse(AkIntervalMonthsFormat.YEAR, "123-3");
    }

    @Test
    public void testMonthsParser() {
        monthsFormatsTested.add(AkIntervalMonthsFormat.MONTH);
        assertEquals("months", 43, AkIntervalMonthsFormat.MONTH.parse("43"));
        badParse(AkIntervalMonthsFormat.MONTH, "123-3");
    }

    @Test
    public void testYearsToMonthsParser() {
        monthsFormatsTested.add(AkIntervalMonthsFormat.YEAR_MONTH);
        assertEquals("years-months", (43*12) + 7, AkIntervalMonthsFormat.YEAR_MONTH.parse("43-7"));
        badParse(AkIntervalMonthsFormat.YEAR_MONTH, "43-13");
        badParse(AkIntervalMonthsFormat.YEAR_MONTH, "123");
    }

    @AfterClass
    public static void confirmAllTested() {
        confirmAllTested(AkIntervalSecondsFormat.class, secondsFormatsTested);
        confirmAllTested(AkIntervalMonthsFormat.class, monthsFormatsTested);
    }

    private static <T extends Enum<T>> void confirmAllTested(Class<T> clazz, Set<T> tested) {
        Set<T> untested = Sets.difference(EnumSet.allOf(clazz), tested);
        if (!untested.isEmpty())
            fail("untested formats for parsing: " + untested);
    }

    private static final class TimeBuilder {
        long nanos;

        public TimeBuilder() {
            // nothing to do
        }

        public TimeBuilder(long amount, TimeUnit unit) {
            add(amount,  unit);
        }

        public TimeBuilder add(long amount, TimeUnit unit) {
            nanos += unit.toNanos(amount);
            return this;
        }

        public long getNanos() {
            return nanos;
        }

        public TimeBuilder checkParse(AkIntervalSecondsFormat format, String string) {
            secondsFormatsTested.add(format);
            long actual = format.parse(string);
            assertEquals("nanoseconds", nanos, AkInterval.secondsIntervalAs(actual, TimeUnit.NANOSECONDS));
            return this;
        }

        public TimeBuilder badParse(AkIntervalSecondsFormat format, String string) {
            AkIntervalTest.badParse(format, string);
            return this;
        }
    }

    private static void badParse(IntervalFormat format, String string) {
        try {
            format.parse(string);
            fail("expected failure when parsing '" + string + "' as " + format);
        } catch (RuntimeException e) {
            // expected!
        }
    }

    /**
     * Checks the value and then returns the <tt>expected</tt> variable, so that it can be reused.
     */
    private long checkAndGet(long rawAmount, TimeUnit expectedUnit, long expectedValue) {
        assertEquals(expectedUnit.name(), expectedValue, AkInterval.secondsIntervalAs(rawAmount, expectedUnit));
        return expectedValue;
    }

    private static final Set<AkIntervalSecondsFormat> secondsFormatsTested
            = EnumSet.noneOf(AkIntervalSecondsFormat.class);

    private static final Set<AkIntervalMonthsFormat> monthsFormatsTested
            = EnumSet.noneOf(AkIntervalMonthsFormat.class);
}
