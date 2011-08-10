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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

abstract class ConvertersForDates extends LongConverter {

    final static ConvertersForDates DATE = new ConvertersForDates() {
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

        @Override
        String asString(long value) {
            final long year = value / 512;
            final long month = (value / 32) % 16;
            final long day = value % 32;
            return String.format("%04d-%02d-%02d", year, month, day);
        }
    };

    final static ConvertersForDates DATETIME = new ConvertersForDates() {
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

        @Override
        String asString(long value) {

            final long year = (value / DATETIME_YEAR_SCALE);
            final long month = (value / DATETIME_MONTH_SCALE) % 100;
            final long day = (value / DATETIME_DAY_SCALE) % 100;
            final long hour = (value / DATETIME_HOUR_SCALE) % 100;
            final long minute = (value / DATETIME_MIN_SCALE) % 100;
            final long second = (value / DATETIME_SEC_SCALE) % 100;
            return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    year, month, day, hour, minute, second);
        }
    };

    final static ConvertersForDates TIME = new ConvertersForDates() {
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

        @Override
        String asString(long value) {
            final long abs = Math.abs(value);
            final long hour = abs / TIME_HOURS_SCALE;
            final long minute = (abs - hour* TIME_HOURS_SCALE) / TIME_MINUTES_SCALE;
            final long second = abs - hour* TIME_HOURS_SCALE - minute* TIME_MINUTES_SCALE;
            return String.format("%s%02d:%02d:%02d", abs != value ? "-" : "", hour, minute, second);
        }
    };

    final static ConvertersForDates TIMESTAMP = new ConvertersForDates() {
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

        @Override
        String asString(long value) {
            return SDF.format(new Date(value*1000));
        }
    };

    final static ConvertersForDates YEAR = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getYear(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putYear(value); }
        @Override protected AkType nativeConversionType() { return AkType.YEAR; }

        @Override
        protected long doParse(String string) {
            long value = Long.parseLong(string);
            return value == 0 ? 0 : (value - 1900);
        }

        @Override
        String asString(long value) {
            final long year = (value == 0) ? 0 : (1900 + value);
            return String.format("%04d", year);
        }
    };

    protected abstract long doGetLong(ConversionSource source);
    protected abstract long doParse(String string);
    abstract String asString(long value);

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
