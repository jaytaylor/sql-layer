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

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.joda.time.MutableDateTime;

public class ExtractorForDateTime extends ObjectExtractor<MutableDateTime>
{
    ExtractorForDateTime ()
    {
        super(AkType.LONG); // wrong!
    }

    @Override
    public MutableDateTime getObject(ValueSource source)
    {
        if (source.isNull()) return null;
        long [] ymd_hms;
        switch(source.getConversionType())
        {
            case DATE:      ymd_hms = ExtractorsForDates.DATE.getYearMonthDayHourMinuteSecond(source.getDate()); break;
            case DATETIME:  ymd_hms = ExtractorsForDates.DATETIME.getYearMonthDayHourMinuteSecond(source.getDateTime()); break;
            case TIMESTAMP: return new MutableDateTime(source.getTimestamp() * 1000); 
            case TIME:      ymd_hms = ExtractorsForDates.TIME.getYearMonthDayHourMinuteSecond(source.getTime()); break;
            case VARCHAR:   return getObject(source.getString());
            case TEXT:      return getObject(source.getText());
            case YEAR:      ymd_hms = ExtractorsForDates.YEAR.getYearMonthDayHourMinuteSecond(source.getYear()); break;
            default:        throw unsupportedConversion(source.getConversionType());
        }

        return new MutableDateTime((int)ymd_hms[0], (int)ymd_hms[1], (int)ymd_hms[2],
                (int)ymd_hms[3], (int)ymd_hms[4], (int)ymd_hms[5], 0);
    }

    @Override
    public MutableDateTime getObject(String string)
    {
        return MutableDateTime.parse(string);
    }
}
