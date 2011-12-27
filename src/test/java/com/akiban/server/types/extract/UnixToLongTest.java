/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.types.extract;

import java.util.TimeZone;
import com.akiban.server.types.AkType;
import org.junit.Test;
import java.util.Calendar;

import static org.junit.Assert.assertEquals;

public class UnixToLongTest
{
    TimeZone defaultTimeZone = TimeZone.getDefault();

    @Test
    public void testDate()
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        
        long unix = Extractors.getLongExtractor(AkType.DATE).stdLongToUnix(1008673L);
        assertEquals(0, unix);

        long stdLong = Extractors.getLongExtractor(AkType.DATE).unixToStdLong(0);
        assertEquals(1008673L, stdLong);
    }

    @Test
    public void testDateTime()
    {        
        long unix = Extractors.getLongExtractor(AkType.DATETIME).stdLongToUnix(20061107123010L);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        calendar.set(Calendar.YEAR, 2006);
        calendar.set(Calendar.MONTH, 10);
        calendar.set(Calendar.DAY_OF_MONTH, 7);
        calendar.set(Calendar.HOUR_OF_DAY, 12);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 10);
        calendar.set(Calendar.MILLISECOND, 0);
        assertEquals((long)calendar.getTimeInMillis(), unix);

        long stdDate = Extractors.getLongExtractor(AkType.DATETIME).unixToStdLong(unix);
        assertEquals(20061107123010L, stdDate);
    }

    @Test
    public void testTime()
    {
        long stdLong = 123010L;
        long unix = Extractors.getLongExtractor(AkType.TIME).stdLongToUnix(stdLong);

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        calendar.set(Calendar.HOUR_OF_DAY, 12);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 10);
        calendar.set(Calendar.MILLISECOND, 0);

        long stdLong1 = Extractors.getLongExtractor(AkType.TIME).unixToStdLong(unix);
        long stdLong2 = Extractors.getLongExtractor(AkType.TIME).unixToStdLong(calendar.getTimeInMillis());

        assertEquals(stdLong, stdLong1);
        assertEquals(stdLong, stdLong2);
    }

    @Test
    public void testYear()
    {
        int year = 1991;
        long unix = Extractors.getLongExtractor(AkType.YEAR).stdLongToUnix(year);

        long stdLong1 = Extractors.getLongExtractor(AkType.YEAR).unixToStdLong(unix);
        assertEquals(year, stdLong1);

        TimeZone.setDefault(defaultTimeZone);
    }
}
