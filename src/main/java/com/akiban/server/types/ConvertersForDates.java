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

package com.akiban.server.types;

import com.persistit.exception.ConversionException;
import com.sun.org.apache.bcel.internal.generic.Type;

import java.text.ParseException;
import java.text.SimpleDateFormat;

abstract class ConvertersForDates extends LongConverter {

    final static LongConverter DATE = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getDate(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putDate(value); }
        @Override protected AkType nativeConversionType() { return AkType.DATE; }

        @Override
        protected long doParse(String string) {
            // YYYY-MM-DD
            final String values[] = string.split("-");
            long y = 0, m = 0, d = 0;
            switch(values.length) {
            case 3: d = Integer.parseInt(values[2]); // fall
            case 2: m = Integer.parseInt(values[1]); // fall
            case 1: y = Integer.parseInt(values[0]); break;
            default:
                throw new IllegalArgumentException("Invalid date string");
            }
            return d + m*32 + y*512;
        }
    };

    final static LongConverter DATETIME = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getDateTime(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putDateTime(value); }
        @Override protected AkType nativeConversionType() { return AkType.DATETIME; }

        @Override
        protected long doParse(String string) {
            final String parts[] = string.split(" ");
            if(parts.length != 2) {
                throw new IllegalArgumentException("Invalid DATETIME string");
            }

            final String dateParts[] = parts[0].split("-");
            if(dateParts.length != 3) {
                throw new IllegalArgumentException("Invalid DATE portion");
            }

            final String timeParts[] = parts[1].split(":");
            if(timeParts.length != 3) {
                throw new IllegalArgumentException("Invalid TIME portion");
            }

            return  Long.parseLong(dateParts[0]) * DATETIME_YEAR_SCALE +
                    Long.parseLong(dateParts[1]) * DATETIME_MONTH_SCALE +
                    Long.parseLong(dateParts[2]) * DATETIME_DAY_SCALE +
                    Long.parseLong(timeParts[0]) * DATETIME_HOUR_SCALE +
                    Long.parseLong(timeParts[1]) * DATETIME_MIN_SCALE +
                    Long.parseLong(timeParts[2]) * DATETIME_SEC_SCALE;
        }
    };

    final static LongConverter TIME = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getTime(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putTime(value); }
        @Override protected AkType nativeConversionType() { return AkType.TIME; }

        @Override
        protected long doParse(String string) {
            // (-)HH:MM:SS
            int mul = 1;
            if(string.length() > 0 && string.charAt(0) == '-') {
                mul = -1;
                string = string.substring(1);
            }
            int hours = 0;
            int minutes = 0;
            int seconds = 0;
            int offset = 0;
            final String values[] = string.split(":");
            switch(values.length) {
            case 3: hours   = Integer.parseInt(values[offset++]); // fall
            case 2: minutes = Integer.parseInt(values[offset++]); // fall
            case 1: seconds = Integer.parseInt(values[offset]);   break;
            default:
                throw new IllegalArgumentException("Invalid TIME string");
            }
            minutes += seconds/60;
            seconds %= 60;
            hours += minutes/60;
            minutes %= 60;
            return mul * (hours* TIME_HOURS_SCALE + minutes* TIME_MINUTES_SCALE + seconds);
        }
    };

    final static LongConverter TIMESTAMP = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getTimestamp(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putTimestamp(value); }
        @Override protected AkType nativeConversionType() { return AkType.TIMESTAMP; }

        @Override
        protected long doParse(String string) {
            try {
                return SDF.parse(string).getTime() / 1000;
            } catch(ParseException e) {
                throw new IllegalArgumentException(e);
            }
        }
    };

    final static LongConverter YEAR = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getYear(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putYear(value); }
        @Override protected AkType nativeConversionType() { return AkType.YEAR; }

        @Override
        protected long doParse(String string) {
            long value = Long.parseLong(string);
            return value == 0 ? 0 : (value - 1900);
        }
    };

    protected abstract long doGetLong(ConversionSource source);
    protected abstract long doParse(String string);

    @Override
    public long getLong(ConversionSource source) {
        AkType type = source.getConversionType();
        if (type == nativeConversionType()) {
            return doGetLong(source);
        }
        switch (type) {
        case TEXT:      return doParse(source.getText());
        case VARCHAR:   return doParse(source.getString());
        case LONG:      return source.getLong();
        case INT:       return source.getInt();
        case U_INT:     return source.getUInt();
        default: throw unsupportedConversion(source);
        }
    }

    private ConvertersForDates() {}
    
    // consts

    private static final long DATETIME_DATE_SCALE = 1000000L;
    private static final long DATETIME_YEAR_SCALE = 10000L * DATETIME_DATE_SCALE;
    private static final long DATETIME_MONTH_SCALE = 100L * DATETIME_DATE_SCALE;
    private static final long DATETIME_DAY_SCALE = 1L * DATETIME_DATE_SCALE;
    private static final long DATETIME_HOUR_SCALE = 10000L;
    private static final long DATETIME_MIN_SCALE = 100L;
    private static final long DATETIME_SEC_SCALE = 1L;

    private static final long TIME_HOURS_SCALE = 10000;
    private static final long TIME_MINUTES_SCALE = 100;
    
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
}
