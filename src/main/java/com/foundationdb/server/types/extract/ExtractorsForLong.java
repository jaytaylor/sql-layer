/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.types.extract;

import com.foundationdb.server.error.InvalidCharToNumException;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.ValueSourceIsNullException;
import java.math.RoundingMode;
import org.joda.time.DateTimeZone;

class ExtractorsForLong extends LongExtractor {

    static final LongExtractor LONG = new ExtractorsForLong(AkType.LONG) ;
    static final LongExtractor INT = new ExtractorsForLong(AkType.INT);
    static final LongExtractor U_INT = new ExtractorsForLong(AkType.U_INT);

    @Override
    public String asString(long value) {
        return Long.toString(value);
    }

    @Override
    public long getLong(String string) {
        try {
            return Long.parseLong(string);
        } catch (NumberFormatException ex) {
            throw new InvalidCharToNumException (string); 
        }
    }

    @Override
    public long getLong(ValueSource source) {
        if (source.isNull())
            throw new ValueSourceIsNullException();
        AkType type = source.getConversionType();
        switch (type) {
        case LONG:      return source.getLong();
        case INT:       return source.getInt();
        case U_INT:     return source.getUInt();
        case U_BIGINT:  return source.getUBigInt().longValue();
        case FLOAT:     return Math.round(source.getFloat());
        case U_FLOAT:   return Math.round(source.getUFloat());
        case DOUBLE:    return Math.round(source.getDouble());
        case U_DOUBLE:  return Math.round(source.getUDouble());
        case TEXT:
            try {
                return Math.round(Double.parseDouble(source.getText()));    
            } catch (NumberFormatException ex) {
                throw new InvalidCharToNumException (source.getText());
            }
        case VARCHAR:
            try {
                return Math.round(Double.parseDouble(source.getString()));
            } catch (NumberFormatException ex) {
                throw new InvalidCharToNumException (source.getString());
            }
        case DECIMAL:   return source.getDecimal().setScale(0, RoundingMode.HALF_UP).longValue();
        case DATE:      return source.getDate();
        case DATETIME:  return source.getDateTime();
        case TIME:      return source.getTime();
        case TIMESTAMP: return source.getTimestamp();
        case YEAR:      return source.getYear();
        case INTERVAL_MILLIS:  return source.getInterval_Millis();
        case INTERVAL_MONTH:  return source.getInterval_Month();
        default: throw unsupportedConversion(type);
        }
    }

    private ExtractorsForLong(AkType targetConversionType) {
        super(targetConversionType);
    }

    @Override
    public long stdLongToUnix(long longVal) {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long unixToStdLong(long unixVal) {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }
    
    @Override
    public long[] getYearMonthDayHourMinuteSecond (long value) {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }
    
    @Override
    public long getEncoded (long [] ymd_hms) {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long stdLongToUnix(long longVal, DateTimeZone tz)
    {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long unixToStdLong(long unixVal, DateTimeZone tz)
    {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long[] getYearMonthDayHourMinuteSecond(long value, DateTimeZone tz)
    {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long getEncoded(long[] ymd_hms, DateTimeZone tz)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
