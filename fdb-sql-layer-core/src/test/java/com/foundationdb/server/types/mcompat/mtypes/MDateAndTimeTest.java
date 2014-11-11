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

package com.foundationdb.server.types.mcompat.mtypes;

import com.foundationdb.server.error.InvalidDateFormatException;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.StringType;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.*;
import static com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.StringType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MDateAndTimeTest
{
    @Test
    public void testAdjustTwoDigitYear() {
        assertEquals(2000, adjustTwoDigitYear(0));
        assertEquals(2013, adjustTwoDigitYear(13));
        assertEquals(2069, adjustTwoDigitYear(69));
        assertEquals(1970, adjustTwoDigitYear(70));
        assertEquals(1999, adjustTwoDigitYear(99));
        assertEquals(100, adjustTwoDigitYear(100));
    }

    @Test
    public void testParseDateOrTime() {
        // TIME
        doParseDateOrTime(TIME_ST, "1 1:1:0", 0, 0, 0, 25, 1, 0);
        doParseDateOrTime(TIME_ST, "-12:0:2", 0, 0, 0, -12, 0, 2);
        doParseDateOrTime(TIME_ST, "-1 3:4:5", 0, 0, 0, -27, 4, 5);
        doParseDateOrTime(TIME_ST, "12:30:10", 0, 0, 0, 12, 30, 10);
        doParseDateOrTime(TIME_ST, "25:30:10", 0, 0, 0, 25, 30, 10);
        doParseDateOrTime(TIME_ST, "25:30:10-0400", 0, 0, 0, 25, 30, 10);
        doParseDateOrTime(TIME_ST, "25:30:10-04:00", 0, 0, 0, 25, 30, 10);
        doParseDateOrTime(TIME_ST, "25:30:10-040000", 0, 0, 0, 25, 30, 10);
        doParseDateOrTime(TIME_ST, "25:30:10-04:00:00", 0, 0, 0, 25, 30, 10);
        // DATE
        doParseDateOrTime(DATE_ST, "2002-12-30", 2002, 12, 30, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "2002/12/30", 2002, 12, 30, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "02-12-30", 2002, 12, 30, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "02/12/30", 2002, 12, 30, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "95-12-30", 1995, 12, 30, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "95/12/30", 1995, 12, 30, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "2014-08-27-0400", 2014, 8, 27, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "2014-08-27-04:00", 2014, 8, 27, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "2014-08-27-040000", 2014, 8, 27, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "2014-08-27-04:00:00", 2014, 8, 27, 0, 0, 0);
        // DATE with zeros
        doParseDateOrTime(DATE_ST, "0000-00-00", 0, 0, 0, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "0000-00-01", 0, 0, 1, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "0000-01-00", 0, 1, 0, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "0000-01-01", 0, 1, 1, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "0001-00-00", 1, 0, 0, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "0001-00-01", 1, 0, 1, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "0001-01-00", 1, 1, 0, 0, 0, 0);
        doParseDateOrTime(DATE_ST, "0001-01-01", 1, 1, 1, 0, 0, 0);
        // DATETIME
        doParseDateOrTime(DATETIME_ST, "1900-1-2 12:30:10", 1900, 1, 2, 12, 30, 10);
        doParseDateOrTime(DATETIME_ST, "2012-04-09T12:45:00", 2012, 4, 9, 12, 45, 0);
        doParseDateOrTime(DATETIME_ST, "2013-04-03T14:55:08.249Z-05:00", 2013, 4, 3, 14, 55, 8);
        doParseDateOrTime(DATETIME_ST, "2013/04/03 14.55.08", 2013, 4, 3, 14, 55, 8);
        doParseDateOrTime(DATETIME_ST, "2014-05-21 16:00:35.091000-0400", 2014, 5, 21, 16, 0, 35);
        doParseDateOrTime(DATETIME_ST, "2014-05-21 16:00:35.091000-04:00", 2014, 5, 21, 16, 0, 35);
        doParseDateOrTime(DATETIME_ST, "2014-05-21 16:00:35.091000-040000", 2014, 5, 21, 16, 0, 35);
        doParseDateOrTime(DATETIME_ST, "2014-05-21 16:00:35.091000-04:00:00", 2014, 5, 21, 16, 0, 35);
        // INVALID
        doParseDateOrTime(INVALID_TIME_ST, "", 0, 0, 0, 0, 0, 0);
        doParseDateOrTime(INVALID_DATE_ST, "01-01-02am", 2001, 1, 0, 0, 0, 0);
        doParseDateOrTime(INVALID_DATETIME_ST, "1900-1-2 25:30:10", 1900, 1, 2, 25, 30, 10);
        // UNPARSABLE
        doParseDateOrTime(UNPARSABLE, "01:02:03:04", 0, 0, 0, 0, 0, 0);
        doParseDateOrTime(UNPARSABLE, "2012-04-09TT12:45:00", 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testGetLastDay() {
        assertEquals(-1, getLastDay(2013, -1));
        assertEquals(31, getLastDay(2013, 1));
        assertEquals(28, getLastDay(2013, 2));
        assertEquals(31, getLastDay(2013, 3));
        assertEquals(30, getLastDay(2013, 4));
        assertEquals(31, getLastDay(2013, 5));
        assertEquals(30, getLastDay(2013, 6));
        assertEquals(31, getLastDay(2013, 7));
        assertEquals(31, getLastDay(2013, 8));
        assertEquals(30, getLastDay(2013, 9));
        assertEquals(31, getLastDay(2013, 10));
        assertEquals(30, getLastDay(2013, 11));
        assertEquals(31, getLastDay(2013, 12));
        assertEquals(-1, getLastDay(2013, 13));
        // Feb, by 400
        assertEquals(29, getLastDay(2000, 2));
        // Feb, not by 400, by 100
        assertEquals(28, getLastDay(1900, 2));
        // Feb, not by 400, not by 100, by 4
        assertEquals(29, getLastDay(1904, 2));
    }

    @Test
    public void testParseAndEncodeDate() {
        // Assumed to be convenience around parse + encode, just sanity check
        assertEquals(MDateAndTime.encodeDate(2013, 1, 30), parseAndEncodeDate("13-01-30"));
        assertEquals(MDateAndTime.encodeDate(2013, 1, 30), parseAndEncodeDate("2013-01-30"));
        assertEquals(MDateAndTime.encodeDate(2013, 1, 30), parseAndEncodeDate("2013-01-30 10:11:12"));
    }

    @Test
    public void testParseAndEncodeDateTime() {
        // Assumed to be convenience around + encode, just sanity check
        assertEquals(MDateAndTime.encodeDateTime(2013, 1, 30, 0, 0, 0), parseAndEncodeDateTime("13-01-30"));
        assertEquals(MDateAndTime.encodeDateTime(2013, 1, 30, 0, 0, 0), parseAndEncodeDateTime("2013-01-30"));
    }

    @Test
    public void testParseTimeZone() {
        assertEquals(DateTimeZone.forOffsetHoursMinutes(0, 0), parseTimeZone("0:0"));
        assertEquals(DateTimeZone.forOffsetHoursMinutes(0, 0), parseTimeZone("00:00"));
        assertEquals(DateTimeZone.forOffsetHoursMinutes(5, 20), parseTimeZone("+05:20"));
        assertEquals(DateTimeZone.forOffsetHoursMinutes(-5, 45), parseTimeZone("-05:45"));
        assertEquals(DateTimeZone.forOffsetHoursMinutes(0, 0), parseTimeZone("UTC"));
        assertEquals(DateTimeZone.forID("MET"), parseTimeZone("met"));
        assertEquals(DateTimeZone.forID("MET"), parseTimeZone("MET"));
        assertEquals(DateTimeZone.forID("America/Los_Angeles"), parseTimeZone("America/Los_Angeles"));
        try {
            parseTimeZone("asdf");
        } catch(InvalidDateFormatException e) {
            // None
        }
        try {
            parseTimeZone("00x:00y");
        } catch(InvalidDateFormatException e) {
            // None
        }
    }

    @Test
    public void testParseDate_long() {
        // Assumed to be implemented with parseDateTime.
        // Sanity
        arrayEquals(dt(0), parseDate(0));
        arrayEquals(dt(1995, 5, 1), parseDate(19950501));
        // But ignores HH:MM:SS
        arrayEquals(dt(1995, 5, 1), parseDate(19950501101112L));
    }

    @Test
    public void testParseDateTime_long() {
        arrayEquals(dt(0), parseDateTime(0));
        try {
            parseDateTime(100);
            fail("expected invalid");
        } catch(InvalidDateFormatException e) {
            // Expected
        }
        arrayEquals(dt(2000, 5, 1), parseDateTime(501));
        arrayEquals(dt(2005, 5, 1), parseDateTime(50501));
        // Defensible
        arrayEquals(dt(1995, 5, 1), parseDateTime(950501));
        arrayEquals(dt(1995, 5, 1), parseDateTime(19950501));
        arrayEquals(dt(1995, 12, 31), parseDateTime(19951231));
        arrayEquals(dt(2000, 1, 1), parseDateTime(20000101));
        arrayEquals(dt(9999, 12, 31), parseDateTime(99991231));
        // And zeros
        arrayEquals(dt(2000, 0, 0), parseDateTime(20000000));
        arrayEquals(dt(2000, 0, 1), parseDateTime(20000001));
        arrayEquals(dt(2000, 1, 0), parseDateTime(20000100));
        arrayEquals(dt(2000, 3, 0), parseDateTime(20000300));
        arrayEquals(dt(1999, 3, 0), parseDateTime(19990300));
        // Catches invalid
        try {
            parseDateTime(20000231);
            fail("expected invalid");
        } catch(InvalidDateFormatException e) {
            // Expected
        }
        arrayEquals(dt(110, 11, 12, 0, 0, 0), parseDateTime(1101112));
        arrayEquals(dt(1995, 5, 1, 10, 11, 12), parseDateTime(950501101112L));
        arrayEquals(dt(1995, 5, 1, 10, 11, 12), parseDateTime(19950501101112L));
    }

    @Test
    public void testIsValidDate() {
        assertEquals(true, isValidDate_Zeros(dt(0, 0, 0)));
        assertEquals(false, isValidDate_NoZeros(dt(0, 0, 0)));
        assertEquals(false, isValidDate_NoZeros(dt(2013, 0, 0)));
        assertEquals(false, isValidDate_NoZeros(dt(2013, 1, 0)));
        assertEquals(true, isValidDate_NoZeros(dt(2013, 1, 31)));
        
        assertEquals(true, isValidDate(dt(0, 1, 31), ZeroFlag.YEAR));
        assertEquals(false, isValidDate(dt(2013, 0, 31), ZeroFlag.YEAR));
        assertEquals(false, isValidDate(dt(2013, 1, 0), ZeroFlag.YEAR));

        assertEquals(true, isValidDate(dt(2013, 0, 31), ZeroFlag.MONTH));
        assertEquals(false, isValidDate(dt(0, 1, 31), ZeroFlag.MONTH));
        assertEquals(false, isValidDate(dt(2013, 1, 0), ZeroFlag.MONTH));

        assertEquals(true, isValidDate(dt(2013, 1, 0), ZeroFlag.DAY));
        assertEquals(false, isValidDate(dt(0, 1, 31), ZeroFlag.DAY));
        assertEquals(false, isValidDate(dt(2013, 0, 31), ZeroFlag.DAY));
    }

    @Test
    public void testIsValidHrMinSec() {
        assertEquals(true, isValidHrMinSec(0, 0, 0, false, false));
        assertEquals(true, isValidHrMinSec(0, 0, 0, false, true));
        assertEquals(true, isValidHrMinSec(0, 0, 0, true, false));

        assertEquals(false, isValidHrMinSec(0, 0, 60, true, true));
        assertEquals(false, isValidHrMinSec(0, 0, 60, true, false));
        assertEquals(false, isValidHrMinSec(0, 60, 0, true, true));
        assertEquals(false, isValidHrMinSec(0, 60, 0, true, false));

        assertEquals(true, isValidHrMinSec(23, 0, 0, true, true));
        assertEquals(true, isValidHrMinSec(23, 0, 0, true, false));
        assertEquals(false, isValidHrMinSec(24, 0, 0, true, true));
        assertEquals(true, isValidHrMinSec(24, 0, 0, true, false));

        assertEquals(false, isValidHrMinSec(0, 0, -1, true, true));
        assertEquals(false, isValidHrMinSec(0, -1, 0, true, true));
        assertEquals(false, isValidHrMinSec(-1, 0, 0, true, true));

        assertEquals(true, isValidHrMinSec(0, 0, -1, true, false));
        assertEquals(true, isValidHrMinSec(0, -1, 0, true, false));
        assertEquals(true, isValidHrMinSec(-1, 0, 0, true, false));
        assertEquals(false, isValidHrMinSec(-1, -1, 0, true, false));
        assertEquals(false, isValidHrMinSec(-1, -1, -1, true, false));

        assertEquals(false, isValidHrMinSec(900, 0, 0, true, false));
        assertEquals(true, isValidHrMinSec(900, 0, 0, false, false));
    }

    @Test
    public void testTimeToString() {
        assertEquals("00:00:00", timeToString(0, 0, 0));
        assertEquals("-01:10:10", timeToString(-1, 10, 10));
        assertEquals("838:59:59", timeToString(838, 59, 59));
    }


    private static void arrayEquals(long[] expected, long[] actual) {
        assertEquals(Arrays.toString(expected), Arrays.toString(actual));
    }

    private static void doParseDateOrTime(StringType expectedType, String st, long... expected) {
        long[] actual = new long[6];
        StringType actualType = parseDateOrTime(st, actual);
        assertEquals("Type: ", expectedType, actualType);
        assertEquals(Arrays.toString(expected), Arrays.toString(actual));
    }

    private static long[] dt(long... vals) {
        return Arrays.copyOf(vals, 6);
    }
}
