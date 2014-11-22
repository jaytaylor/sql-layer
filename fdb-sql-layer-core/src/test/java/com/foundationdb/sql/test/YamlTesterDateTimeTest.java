/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.test;

import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.TimeZone;

import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.foundationdb.sql.test.YamlTester.DateTimeChecker;
import com.foundationdb.sql.test.YamlTester.TimeChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests the !date, !time underpinnings to insure they are accurate
 */
public class YamlTesterDateTimeTest {
    private static final Logger LOG = LoggerFactory.getLogger(YamlTesterDateTimeTest.class);

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    static
    {
        String timezone="UTC";
        DateTimeZone.setDefault(DateTimeZone.forID(timezone));
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
    }
    
    @Test
    public void testTimeTag() {
        Calendar cal = getCalendar();
        String time = String.format("%02d:%02d:%02d",
                cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND));
        test(time);
        cal = getCalendar();
        cal.roll(Calendar.SECOND, -30);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        test(time);
        cal = getCalendar();
        cal.roll(Calendar.SECOND, 30);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        test(time);
    }

    private static Calendar getCalendar() {
        Calendar cal = Calendar.getInstance(UTC);
        cal.setTimeInMillis(System.currentTimeMillis());
        return cal;
    }

    @Test
    public void testTimeTag_Negative() {
        Calendar cal = getCalendar();
        cal.roll(Calendar.MINUTE, 5);
        String time = String.format("%02d:%02d:%02d",
                cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND));
        testFail(time);
        cal = getCalendar();
        cal.roll(Calendar.HOUR_OF_DAY, 1);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        testFail(time);
        cal = getCalendar();
        cal.roll(Calendar.MINUTE, 2);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        testFail(time);
        cal = getCalendar();
        cal.roll(Calendar.MINUTE, -1);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        testFail(time);
    }

    @Test
    public void testDateTimeTag() {
        Calendar cal = getCalendar();
        String time = formatDateTime(cal);
        testdt(time);
        cal = getCalendar();
        cal.roll(Calendar.SECOND, -30);
        time = formatDateTime(cal);
        testdt(time);
        cal = getCalendar();
        cal.roll(Calendar.SECOND, 30);
        time = formatDateTime(cal);
        testdt(time);
    }

    private String formatDateTime(Calendar cal) {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%03d",
                cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
                cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),
                cal.get(Calendar.MILLISECOND));
    }

    @Test
    public void testDateTimeTag_Negative() {
        Calendar cal = getCalendar();
        cal.roll(Calendar.MINUTE, 5);
        String time = formatDateTime(cal);
        testdtFail(time);
        cal = getCalendar();
        cal.roll(Calendar.HOUR_OF_DAY, 1);
        time = formatDateTime(cal);
        testdtFail(time);
        cal = getCalendar();
        cal.roll(Calendar.MINUTE, 2);
        time = formatDateTime(cal);
        testdtFail(time);
        cal = getCalendar();
        cal.roll(Calendar.MINUTE, -1);
        time = formatDateTime(cal);
        testdtFail(time);
    }

    private static void test(String output) {
        boolean result = new TimeChecker().compareExpected(output);
        if (!result) {
            fail("Time check failed with " + output);
        }
    }

    private static void testFail(String output) {
        boolean result = new TimeChecker().compareExpected(output);
        if (result) {
            fail("Time check failed with " + output);
        }
    }

    private static void testdt(String output) {
        boolean result = new DateTimeChecker().compareExpected(output);
        if (!result) {
            fail("Time check failed with " + output);
        } else {
            LOG.debug(output);
        }
    }

    private static void testdtFail(String output) {
        boolean result = new DateTimeChecker().compareExpected(output);
        if (result) {
            fail("Time check failed with " + output);
        } else {
            LOG.debug(output);
        }
    }

}
