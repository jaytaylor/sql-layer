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

import com.foundationdb.server.types.mcompat.mtypes.MDatetimes.StringType;
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.server.types.mcompat.mtypes.MDatetimes.StringType.*;
import static org.junit.Assert.assertEquals;

public class MDatetimesTest
{
    private static void doParseDateOrTime(StringType expectedType, String st, long... expected) {
        long[] actual = new long[6];
        StringType actualType = MDatetimes.parseDateOrTime(st, actual);
        assertEquals("Type: ", expectedType, actualType);
        assertEquals(Arrays.toString(expected), Arrays.toString(actual));
    }

    @Test
    public void parseDateOrTime() {
        // TIME
        doParseDateOrTime(TIME_ST, "1 1:1:0", 0, 0, 0, 25, 1, 0);
        doParseDateOrTime(TIME_ST, "-12:0:2", 0, 0, 0, -12, 0, 2);
        doParseDateOrTime(TIME_ST, "-1 3:4:5", 0, 0, 0, -27, 4, 5);
        doParseDateOrTime(TIME_ST, "12:30:10", 0, 0, 0, 12, 30, 10);
        doParseDateOrTime(TIME_ST, "25:30:10", 0, 0, 0, 25, 30, 10);
        // DATE
        doParseDateOrTime(DATE_ST, "2002-12-30", 2002, 12, 30, 0, 0, 0);
        // DATETIME
        doParseDateOrTime(DATETIME_ST, "1900-1-2 12:30:10", 1900, 1, 2, 12, 30, 10);
        doParseDateOrTime(DATETIME_ST, "2012-04-09T12:45:00", 2012, 4, 9, 12, 45, 0);
        doParseDateOrTime(DATETIME_ST, "2013-04-03T14:55:08.249Z-05:00", 2013, 4, 3, 14, 55, 8);
        // INVALID_DATETIME
        doParseDateOrTime(INVALID_DATETIME_ST, "1900-1-2 25:30:10", 1900, 1, 2, 25, 30, 10);
        doParseDateOrTime(UNPARSABLE, "2012-04-09TT12:45:00", 0, 0, 0, 0, 0, 0);
    }
}
