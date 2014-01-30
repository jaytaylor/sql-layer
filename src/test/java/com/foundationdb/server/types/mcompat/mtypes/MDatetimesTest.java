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
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes.StringType;
import java.util.ArrayList;
import java.util.List;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import org.junit.Test;

import static com.foundationdb.server.types.mcompat.mtypes.MDatetimes.StringType.*;
import static org.junit.Assert.*;

public class MDatetimesTest
{
    @Test
    public void parseTimeTest()
    {
        doTest(TIME_ST, "12:30:10",
                        0, 0, 0, 12, 30, 10);
    }
    
    private static void doTest(StringType expectedType, String st, long...expected)
    {
        long actual[] = new long[6];
        StringType actualType = MDatetimes.parseDateOrTime(st, actual);
        
        assertEquals("Type: ", expectedType,
                               actualType);
        
        // convert to lists for a nice error msg, if any
        assertEquals(toList(expected),
                     toList(actual));
        
    }
    
    @Test
    public void parseTimeWithDayTest()
    {
        doTest(TIME_ST, "1 1:1:0",
                        0, 0, 0, 25, 1, 0);
    }

    @Test
    public void parseNegTime()
    {
        doTest(TIME_ST, "-12:0:2",
                        0, 0, 0, -12, 0, 2);
    }
    
    @Test
    public void parseNegTimeWithDay()
    {
        doTest(TIME_ST, "-1 3:4:5",
                        0, 0, 0, -27, 4, 5);
    }
    
    @Test
    public void parseDate()
    {
        doTest(DATE_ST, "2002-12-30",
                        2002, 12, 30, 0, 0, 0);
    }
    
    @Test
    public void parseDateTime()
    {
        doTest(DATETIME_ST, "1900-1-2 12:30:10",
                            1900, 1, 2, 12, 30, 10);
    }
    
    @Test
    public void testInvalidTime() // test invalid time in DATETIME
    {
        doTest(INVALID_DATETIME_ST, "1900-1-2 25:30:10",
                                     1900, 1, 2, 25, 30, 10);
    }
    
    @Test
    public void testTime() // 25:30:10 is a valid TIME though
    {
        doTest(TIME_ST, "25:30:10",
                        0, 0, 0, 25, 30, 10);
    }
    @Test
    public void testDateTimeT() {
        doTest(DATETIME_ST, "2012-04-09T12:45:00",
                2012, 4, 9, 12, 45, 0);
    }
    
    @Test (expected=InvalidDateFormatException.class)
    public void testDateTimeTT() {
        doTest(INVALID_DATETIME_ST, "2012-04-09TT12:45:00",
                2012, 4, 9, 12, 45, 0);
    }
    
    @Test
    public void testDateTimeTZone() {
        doTest(DATETIME_ST, "2013-04-03T14:55:08.249Z-05:00",
                2013, 4, 3, 14, 55, 8);
    }
    
    private static List<Long> toList(long...exp)
    {
        List<Long> list = new ArrayList<>(exp.length);
        
        for (long val : exp)
            list.add(val);
        
        return list;
    }
}
