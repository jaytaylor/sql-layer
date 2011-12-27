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

import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

abstract class ExtractorsForDates extends LongExtractor {

    /**
     * Encoder for working with dates when stored as a 3 byte int using
     * the encoding of DD + MM x 32 + YYYY x 512. This is how MySQL stores the
     * SQL DATE type.
     * See: http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
     */
    final static ExtractorsForDates DATE = new ExtractorsForDates(AkType.DATE) {
        @Override protected long doGetLong(ValueSource source) {
            return source.getDate();
        }

        @Override
        public long getLong(String string) {
            // YYYY-MM-DD
            final String values[] = string.split("-");
            long y = 0, m = 0, d = 0;
            try {
                switch(values.length) {
                case 3: d = Integer.parseInt(values[2]); // fall
                case 2: m = Integer.parseInt(values[1]); // fall
                case 1: y = Integer.parseInt(values[0]); break;
                default:
                    throw new InvalidDateFormatException ("date", string);
                }
            } catch (NumberFormatException ex) {
                throw new InvalidDateFormatException ("date", string);
            }
            return d + m*32 + y*512;
        }

        @Override
        public String asString(long value) {
            final long year = value / 512;
            final long month = (value / 32) % 16;
            final long day = value % 32;
            return String.format("%04d-%02d-%02d", year, month, day);
        }

        @Override
        public long stdLongToUnix(long longVal) {
            long year = longVal / 512;
            long month = longVal / 32 % 16;
            long day = longVal % 32;
            return Calculator.getMillis((int)year, (int)month, (int)day, 0, 0, 0);
        }

        @Override
        public long unixToStdLong(long unixVal) {
            long ymd[] = Calculator.getYearMonthDay(unixVal);
            return (long)ymd[0] * 512 + (long)ymd[1] *32 + ymd[2];
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value) {
            final long year = value / 512;
            final long month = (value / 32) % 16;
            final long day = value % 32;
            return new long[] {year, month, day, 0, 0, 0};
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            return ymd_hms[0] * 512 + ymd_hms[1] * 32 + ymd_hms[2];
        }
    };

    /**
     * Encoder for working with dates and times when stored as an 8 byte int
     * encoded as (YY*10000 MM*100 + DD)*1000000 + (HH*10000 + MM*100 + SS).
     * This is how MySQL stores the SQL DATETIME type.
     * See: http://dev.mysql.com/doc/refman/5.5/en/datetime.html
     */
    final static ExtractorsForDates DATETIME = new ExtractorsForDates(AkType.DATETIME) {
        @Override protected long doGetLong(ValueSource source) {
            return source.getDateTime();
        }

        @Override
        public long getLong(String string) {
            final String parts[] = string.split(" ");
            if(parts.length != 2) {
                throw new InvalidDateFormatException ("date time", string);
            }

            final String dateParts[] = parts[0].split("-");
            if(dateParts.length != 3) {
                throw new InvalidDateFormatException ("date", parts[0]);
            }

            final String timeParts[] = parts[1].split(":");
            if(timeParts.length != 3) {
                throw new InvalidDateFormatException ("time", parts[1]);
            }

            try {
            return  Long.parseLong(dateParts[0]) * DATETIME_YEAR_SCALE +
                    Long.parseLong(dateParts[1]) * DATETIME_MONTH_SCALE +
                    Long.parseLong(dateParts[2]) * DATETIME_DAY_SCALE +
                    Long.parseLong(timeParts[0]) * DATETIME_HOUR_SCALE +
                    Long.parseLong(timeParts[1]) * DATETIME_MIN_SCALE +
                    Long.parseLong(timeParts[2]) * DATETIME_SEC_SCALE;
            } catch (NumberFormatException ex) {
                throw new InvalidDateFormatException ("date time", string);
            }
        }

        @Override
        public String asString(long value) {

            final long year = (value / DATETIME_YEAR_SCALE);
            final long month = (value / DATETIME_MONTH_SCALE) % 100;
            final long day = (value / DATETIME_DAY_SCALE) % 100;
            final long hour = (value / DATETIME_HOUR_SCALE) % 100;
            final long minute = (value / DATETIME_MIN_SCALE) % 100;
            final long second = (value / DATETIME_SEC_SCALE) % 100;
            return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    year, month, day, hour, minute, second);
        }

        @Override
        public long stdLongToUnix(long longVal) {
            long year = longVal / DATETIME_YEAR_SCALE;
            long month = longVal / DATETIME_MONTH_SCALE % 100;
            long day = longVal / DATETIME_DAY_SCALE % 100;
            long hour = longVal / DATETIME_HOUR_SCALE % 100;
            long minute = longVal / DATETIME_MIN_SCALE % 100;
            long second = longVal / DATETIME_SEC_SCALE % 100;
            return Calculator.getMillis((int)year, (int)month, (int)day, (int)hour, (int)minute, (int)second);
        }

        @Override
        public long unixToStdLong(long unixVal) {
            long rst[] = Calculator.getYMDHMS(unixVal);
            return (rst[0] * 10000 + rst[1] * 100 + rst[2]) *1000000L + rst[3] * 10000 + rst[4] * 100 + rst[5];
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond (long value) {
            final long year = (value / DATETIME_YEAR_SCALE);
            final long month = (value / DATETIME_MONTH_SCALE) % 100;
            final long day = (value / DATETIME_DAY_SCALE) % 100;
            long hour = value / DATETIME_HOUR_SCALE % 100;
            long minute = value / DATETIME_MIN_SCALE % 100;
            long second = value / DATETIME_SEC_SCALE % 100;
            return new long[] {year, month, day, hour, minute, second};
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            return ymd_hms[0] * DATETIME_YEAR_SCALE +
                   ymd_hms[1] * DATETIME_MONTH_SCALE +
                   ymd_hms[2] * DATETIME_DAY_SCALE +
                   ymd_hms[3] * DATETIME_HOUR_SCALE +
                   ymd_hms[4] * DATETIME_MIN_SCALE +
                   ymd_hms[5] * DATETIME_SEC_SCALE;
        }
    };

    /**
     * Encoder for working with time when stored as a 3 byte int encoded as
     * HH*10000 + MM*100 + SS. This is how MySQL stores the SQL TIME type.
     * See: http://dev.mysql.com/doc/refman/5.5/en/time.html
     * and  http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
     */
    final static ExtractorsForDates TIME = new ExtractorsForDates(AkType.TIME) {
        @Override protected long doGetLong(ValueSource source) {
            return source.getTime();
        }

        @Override
        public long getLong(String string) {
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
            try {
                switch(values.length) {
                case 3: hours   = Integer.parseInt(values[offset++]); // fall
                case 2: minutes = Integer.parseInt(values[offset++]); // fall
                case 1: seconds = Integer.parseInt(values[offset]);   break;
                default:
                    throw new InvalidDateFormatException ("time", string);
                }
            } catch (NumberFormatException ex) {
                throw new InvalidDateFormatException ("time", string);
            }
            minutes += seconds/60;
            seconds %= 60;
            hours += minutes/60;
            minutes %= 60;
            return mul * (hours* TIME_HOURS_SCALE + minutes* TIME_MINUTES_SCALE + seconds);
        }

        @Override
        public String asString(long value) {
            final long abs = Math.abs(value);
            final long hour = abs / TIME_HOURS_SCALE;
            final long minute = (abs - hour* TIME_HOURS_SCALE) / TIME_MINUTES_SCALE;
            final long second = abs - hour* TIME_HOURS_SCALE - minute* TIME_MINUTES_SCALE;
            return String.format("%s%02d:%02d:%02d", abs != value ? "-" : "", hour, minute, second);
        }

        @Override
        public long stdLongToUnix(long longVal) {
            boolean pos;
            long abs = ((pos = longVal >= 0L) ? longVal : -longVal);
            long hour = abs / TIME_HOURS_SCALE;
            long minute = (abs - hour* TIME_HOURS_SCALE) / TIME_MINUTES_SCALE;
            long second = abs - hour* TIME_HOURS_SCALE - minute* TIME_MINUTES_SCALE;

            // .TIME doesn't have date field, so assume 1970,1,1 as the date
            return Calculator.getMillis(1970, 1, 1, (int)hour, (int)minute, (int)second) * (pos ? 1 :-1);
        }

        @Override
        public long unixToStdLong(long unixVal) {
            int rst[] = Calculator.getHrMinSec(unixVal);
            return rst[0]* TIME_HOURS_SCALE + rst[1]* TIME_MINUTES_SCALE + rst[2];
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value) {
            long abs = value > 0 ? value : - value;
            long hour = abs / TIME_HOURS_SCALE;
            long minute = (abs - hour* TIME_HOURS_SCALE) / TIME_MINUTES_SCALE;
            long second = abs - hour* TIME_HOURS_SCALE - minute* TIME_MINUTES_SCALE;
            return new long [] {0, 1, 1, hour, minute, second};
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            return ymd_hms[3] * 10000 +
                   ymd_hms[4] * 100 +
                   ymd_hms[5];
        }
    };

    /**
     * Encoder for working with time when stored as a 4 byte int (standard
     * UNIX timestamp). This is how MySQL stores the SQL TIMESTAMP type.
     * See: http://dev.mysql.com/doc/refman/5.5/en/timestamp.html
     * and  http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
     */
    final static ExtractorsForDates TIMESTAMP = new ExtractorsForDates(AkType.TIMESTAMP) {
        @Override protected long doGetLong(ValueSource source)             { return source.getTimestamp(); }

        @Override
        public long getLong(String string) {
            if (TIMESTAMP_ZERO_STRING.equals(string))
                return 0;
            try {
                return timestampFormat().parse(string).getTime() / 1000;
            } catch(ParseException e) {
                throw new InvalidDateFormatException ("timestamp", string);                
            }
        }

        @Override
        public String asString(long value) {
            return value == 0
                    ? TIMESTAMP_ZERO_STRING
                    : timestampFormat().format(new Date(value * 1000));
        }

        @Override
        public long stdLongToUnix(long longVal) {
            return longVal * 1000;
        }

        @Override
        public long unixToStdLong(long unixVal) {
            return unixVal / 1000;
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value) {
            return Calculator.getYMDHMS(value * 1000);
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            return Calculator.getMillis((int)ymd_hms[0], 
                                        (int)ymd_hms[1], 
                                        (int)ymd_hms[2], 
                                        (int)ymd_hms[3], 
                                        (int)ymd_hms[4], 
                                        (int)ymd_hms[5]);
        }
    };

    /**
     * Encoder for working with years when stored as a 1 byte int in the
     * range of 0, 1901-2155.  This is how MySQL stores the SQL YEAR type.
     * See: http://dev.mysql.com/doc/refman/5.5/en/year.html
     */
    final static ExtractorsForDates YEAR = new ExtractorsForDates(AkType.YEAR) {
        @Override protected long doGetLong(ValueSource source) {
            return source.getYear();
        }

        @Override
        public long getLong(String string) {
            try {
                long value = Long.parseLong(string);
                return value == 0 ? 0 : (value - 1900);
            } catch (NumberFormatException ex) {
                throw new InvalidDateFormatException ("year", string);
            }
        }

        @Override
        public String asString(long value) {
            final long year = (value == 0) ? 0 : (1900 + value);
            return String.format("%d", year);
        }

        @Override
        public long stdLongToUnix(long longVal) {
            return Calculator.getMillis(longVal == 0 ? 0 :1900 + (int)longVal, 1, 1, 0, 0, 0);
        }

        @Override
        public long unixToStdLong(long unixVal) {
           long yr = Calculator.getYear(unixVal);
           return yr == 0L ? 0 : yr - 1900 ;
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value) {
            return new long[] {value == 0 ? 0 : 1900 + value, 1, 1, 0, 0, 0};
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            return ymd_hms[0] == 0 ? 0 : 1900 + ymd_hms[0];
        }
    };

    final static ExtractorsForDates INTERVAL_MILLIS = new ExtractorsForDates(AkType.INTERVAL_MILLIS)
    {
        @Override
        protected long doGetLong(ValueSource source){
            AkType type = source.getConversionType();
            switch (type){
                case INTERVAL_MILLIS:  return source.getInterval_Millis();
                case DECIMAL:   return source.getDecimal().longValue();
                case DOUBLE:    return (long)source.getDouble();
                case U_BIGINT:  return source.getUBigInt().longValue();
                case VARCHAR:   return getLong(source.getString());
                case TEXT:      return getLong(source.getText());
                case LONG:      return source.getLong();
                case INT:       return source.getInt();
                case U_INT:     return source.getUInt();
                case U_DOUBLE:  return (long)source.getUDouble();
                default:        throw unsupportedConversion(type);
            }
        }

        /**
         * format: millisecs between two events
         */
        @Override
        public long getLong (String st){            
              try {
                long value = Long.parseLong(st);
                return value;
            } catch (NumberFormatException ex) {
                throw new InvalidDateFormatException ("interval", st);
            }
        }

        /*
         * return interval in milisec
         */
        @Override
        public String asString (long value){
            return value + "";
        }

        @Override
        public long stdLongToUnix(long longVal) {
             return longVal; // interval_millis is already in millisec
        }

        @Override
        public long unixToStdLong(long unixVal) {
            return unixVal; 
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value) {
            throw new UnsupportedOperationException("Unsupported: Cannot extract Year/Month/.... from INTERVAL_MILLIS");
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            throw new UnsupportedOperationException("Unsupported: Cannot encode Year/Month/... from INTERVAL_MILLIS");
        }
    };

    final static ExtractorsForDates INTERVAL_MONTH = new ExtractorsForDates(AkType.INTERVAL_MONTH)
    {
        @Override
        protected long doGetLong(ValueSource source){
            AkType type = source.getConversionType();
            switch (type){
                case INTERVAL_MONTH:  return source.getInterval_Month();
                case DECIMAL:   return source.getDecimal().longValue();
                case DOUBLE:    return (long)source.getDouble();
                case U_BIGINT:  return source.getUBigInt().longValue();
                case VARCHAR:   return getLong(source.getString());
                case TEXT:      return getLong(source.getText());
                case LONG:      return source.getLong();
                case INT:       return source.getInt();
                case U_INT:     return source.getUInt();
                case U_DOUBLE:  return (long)source.getUDouble();
                default:        throw unsupportedConversion(type);
            }
        }

        /**
         * format: months between two events
         */
        @Override
        public long getLong (String st){            
              try {
                long value = Long.parseLong(st);
                return value;
            } catch (NumberFormatException ex) {
                throw new InvalidDateFormatException ("interval", st);
            }
        }

        /*
         * return interval in months
         */
        @Override
        public String asString (long value){
            return value + "";
        }

        @Override
        public long stdLongToUnix(long longVal) {
             throw new UnsupportedOperationException ("INTERVAL_MONTH to unix is not supported");
        }

        @Override
        public long unixToStdLong(long unixVal) {
            throw new UnsupportedOperationException ("unix to INTERVAL_MONTH not supported");
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value) {
            throw new UnsupportedOperationException("Unsupported: Cannot extract Year/Month/... from INTERVAL_MONTH");
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            throw new UnsupportedOperationException("Unsupported: encode Year/Month/... to INTERVAL_MONTH");

        }
    };

    private static class Calculator {
        private static Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT")); // Assume timezone is UTC for now

        public static long getMillis (int year, int mon, int day, int hr, int min, int sec) {
            calendar.set(Calendar.YEAR, year);
            calendar.set(Calendar.MONTH, mon -1); // month in calendar is 0-based
            calendar.set(Calendar.DAY_OF_MONTH, day );
            calendar.set(Calendar.HOUR_OF_DAY, hr);
            calendar.set(Calendar.MINUTE, min);
            calendar.set(Calendar.SECOND, sec);
            calendar.set(Calendar.MILLISECOND, 0);
            
            return calendar.getTimeInMillis();
        }

        public static long getYear (long millis)  {
            calendar.setTimeInMillis(millis);
            return calendar.get(Calendar.YEAR);
        }

        public static int getMonth (long millis) {
            calendar.setTimeInMillis(millis);
            return calendar.get(Calendar.MONTH) + 1; // month in Calendar is 0-based
        }

        public static int getDay (long millis) {
            calendar.setTimeInMillis(millis);
            return calendar.get(Calendar.DAY_OF_MONTH);
        }

        public static int getHour (long millis) {
            calendar.setTimeInMillis(millis);
            return calendar.get(Calendar.HOUR_OF_DAY);
        }

        public static int getMinute (long millis) {
            calendar.setTimeInMillis(millis);
            return calendar.get(Calendar.MINUTE);
        }

        public static int getSec (long millis) {
            calendar.setTimeInMillis(millis);
            return calendar.get(Calendar.SECOND);
        }

        public static long[] getYearMonthDay (long millis) {
            calendar.setTimeInMillis(millis);
            return new long[] {calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) +1, calendar.get(Calendar.DAY_OF_MONTH)};
        }

        public static int[] getHrMinSec (long millis) {
            calendar.setTimeInMillis(millis);
            return new int[] {calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND)};
        }
        public static long[] getYMDHMS (long millis) {
            calendar.setTimeInMillis(millis);
            return new long[] {calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) +1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND)};
        }
    }

    protected abstract long doGetLong(ValueSource source);

    @Override
    public long getLong(ValueSource source) {
        if (source.isNull())
            throw new ValueSourceIsNullException();
        AkType type = source.getConversionType();
        if (type == targetConversionType() || targetConversionType() == AkType.INTERVAL_MILLIS
                                           || targetConversionType() == AkType.INTERVAL_MONTH) {
            return doGetLong(source);
        }
        switch (type) {
        case TEXT:      return getLong(source.getText());
        case VARCHAR:   return getLong(source.getString());
        case INT:       return source.getInt();
        case U_INT:     return source.getUInt();
        case LONG:      return source.getLong();
        default: throw unsupportedConversion(type);
        }
    }

    // testing hooks

    static void setGlobalTimezone(String timezone) {
        dateFormatProvider.set(new DateFormatProvider(timezone));
    }

    private static DateFormat timestampFormat() {
        return dateFormatProvider.get().get();
    }

    // for use in this method

    private ExtractorsForDates(AkType targetConversionType) {
        super(targetConversionType);
    }

    // class state

    private static final AtomicReference<DateFormatProvider> dateFormatProvider
            = new AtomicReference<DateFormatProvider>(new DateFormatProvider(TimeZone.getDefault().getID()));

    // consts

    private static final long DATETIME_DATE_SCALE = 1000000L;
    private static final long DATETIME_YEAR_SCALE = 10000L * DATETIME_DATE_SCALE;
    private static final long DATETIME_MONTH_SCALE = 100L * DATETIME_DATE_SCALE;
    private static final long DATETIME_DAY_SCALE = 1L * DATETIME_DATE_SCALE;
    private static final long DATETIME_HOUR_SCALE = 10000L;
    private static final long DATETIME_MIN_SCALE = 100L;
    private static final long DATETIME_SEC_SCALE = 1L;

    private static final String TIMESTAMP_ZERO_STRING = "0000-00-00 00:00:00";

    private static final long TIME_HOURS_SCALE = 10000;
    private static final long TIME_MINUTES_SCALE = 100;

    // nested class

    private static class DateFormatProvider {

        public DateFormatProvider(final String timezoneName) {
            this.dateFormatRef = new ThreadLocal<DateFormat>() {
                @Override
                protected DateFormat initialValue() {
                    DateFormat result = new SimpleDateFormat(TIMEZONE_FORMAT);
                    result.setTimeZone(TimeZone.getTimeZone(timezoneName));
                    return result;
                }
            };
        }

        public DateFormat get() {
            return dateFormatRef.get();
        }

        private final ThreadLocal<DateFormat> dateFormatRef;

        private static final String TIMEZONE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    }
}
