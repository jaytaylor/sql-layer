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

import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

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
        case DECIMAL: return source.getDecimal().setScale(0, RoundingMode.HALF_UP).longValue();
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
    
}
