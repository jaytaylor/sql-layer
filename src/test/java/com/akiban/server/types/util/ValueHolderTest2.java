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

package com.akiban.server.types.util;

import java.util.TimeZone;
import java.util.Calendar;
import java.util.Date;
import com.akiban.server.types.AkType;
import org.junit.Test;
import org.joda.time.DateTime;

import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;

public class ValueHolderTest2 
{
    @Test
    public void testValueHolderDate ()
    {
        ValueHolder expected = new ValueHolder(AkType.DATE,1008673L ); // JAN - 01 - 1970
        ValueHolder actual = new ValueHolder(AkType.DATE, new DateTime(0, DateTimeZone.UTC));

        assertEquals(expected.getDate(), actual.getDate());
    }

    @Test
    public void testValueHolderTime ()
    {
        ValueHolder expected = new ValueHolder(AkType.TIME, 123010L); // 12:30:10

        Calendar time = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        time.set(Calendar.HOUR_OF_DAY, 12);
        time.set(Calendar.MINUTE, 30);
        time.set(Calendar.SECOND, 10);
        ValueHolder actual = new ValueHolder(AkType.TIME, new DateTime(time.getTimeInMillis(), DateTimeZone.UTC));

        assertEquals(expected.getTime(), actual.getTime());
    }

    @Test
    public void testValueHolderDateTime ()
    {
        ValueHolder expected = new ValueHolder(AkType.DATETIME, 20061107123010L); //2006-11-07 12:30:10
        Calendar datetime = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        datetime.set(Calendar.YEAR, 2006);
        datetime.set(Calendar.MONTH, 10); // month in Calendar is 0-based
        datetime.set(Calendar.DAY_OF_MONTH, 7);
        datetime.set(Calendar.HOUR_OF_DAY, 12);
        datetime.set(Calendar.MINUTE, 30);
        datetime.set(Calendar.SECOND, 10);
        ValueHolder actual = new ValueHolder(AkType.DATETIME, new DateTime(datetime.getTime(), DateTimeZone.UTC));

        assertEquals(expected.getDateTime(), actual.getDateTime());
    }

    @Test
    public void testValueHolderTimestamp ()
    {
        ValueHolder expected = new ValueHolder (AkType.TIMESTAMP,0L); // epoch
        ValueHolder actual = new ValueHolder(AkType.TIMESTAMP, new DateTime(0, DateTimeZone.UTC));

        assertEquals(expected.getTimestamp(), actual.getTimestamp());
    }
     
}
