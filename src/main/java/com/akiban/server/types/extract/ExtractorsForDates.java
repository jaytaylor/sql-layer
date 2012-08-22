/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types.extract;

import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.error.InvalidIntervalFormatException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;

import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.EnumSet;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
            String values[];
            // YYY-MM-DD HH:MM:SS
            String parts[] = string.split(" ");
            if (parts.length == 2)
                values = parts[0].split("-"); // truncate time fields
            else
                values = string.split("-");
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
            return stdLongToUnix(longVal, DateTimeZone.getDefault());
        }
        
        @Override
        public long stdLongToUnix(long longVal, DateTimeZone tz) {
            long year = longVal / 512;
            long month = longVal / 32 % 16;
            long day = longVal % 32;
            return Calculator.getMillis((int)year, (int)month, (int)day, 0, 0, 0, tz);
        }

        @Override
        public long unixToStdLong(long unixVal) {
            return unixToStdLong(unixVal, DateTimeZone.getDefault());
        }
        
        @Override
        public long unixToStdLong(long unixVal, DateTimeZone tz) {
            long ymd[] = Calculator.getYearMonthDay(unixVal, tz);
            return (long)ymd[0] * 512 + (long)ymd[1] *32 + ymd[2];
        }

        @Override
        public long [] getYearMonthDayHourMinuteSecond (long value) {
            return getYearMonthDayHourMinuteSecond(value, DateTimeZone.getDefault());
        }
        
        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value, DateTimeZone tz) {
            final long year = value / 512;
            final long month = (value / 32) % 16;
            final long day = value % 32;
            return new long[] {year, month, day, 0, 0, 0};
        }

        @Override
        public long getEncoded(long [] ymd_hms) {
            return getEncoded(ymd_hms, DateTimeZone.getDefault());
        }
                
        @Override
        public long getEncoded(long[] ymd_hms, DateTimeZone tz) {
            return ymd_hms[0] * 512 + ymd_hms[1] * 32 + ymd_hms[2];
        }

        @Override
        protected long doConvert(ValueSource source)
        {
            long ymd[] = {0, 0, 0};

            switch (source.getConversionType())
            {
                case DATETIME:   ymd = DATETIME.getYearMonthDayHourMinuteSecond(source.getDateTime()); break;
                case TIMESTAMP:  ymd = TIMESTAMP.getYearMonthDayHourMinuteSecond(source.getTimestamp()); break;
                default:         unsupportedConversion(source.getConversionType());
            }
            return getEncoded(ymd);
        }
    };

    /**
     * Encoder for working with dates and times when stored as an 8 byte int
     * encoded as (YY*10000 MM*100 + DD)*1000000 + (HH*10000 + MM*100 + SS).
     * This is how MySQL stores the SQL DATETIME type.
     * See: http://dev.mysql.com/doc/refman/5.5/en/datetime.html
     */
    final static ExtractorsForDates DATETIME = new ExtractorsForDates(AkType.DATETIME) {
        private final String ZERO_STR = "0";
        // Capture DATE and TIME into groups (1 and 5) with TIME being optional, each individual fragment also grouped
        private final int DATE_GROUP = 1;
        private final int DATE_YEAR_GROUP = 2;
        private final int DATE_MONTH_GROUP = 3;
        private final int DATE_DAY_GROUP = 4;
        private final int TIME_GROUP = 5;
        private final int TIME_HOUR_GROUP = 6;
        private final int TIME_MINUTE_GROUP = 7;
        private final int TIME_SECOND_GROUP = 8;
        private final int TIME_FRAC_GROUP = 9;
        private final int TIME_TIMEZONE_GROUP = 10;
        private final Pattern PARSE_PATTERN = Pattern.compile(
                "^((\\d+)-(\\d+)-(\\d+))(\\s+(\\d+):(\\d+):(\\d+)(\\.\\d+)?([+-]\\d+:\\d+)?)?$"
        );

        @Override protected long doGetLong(ValueSource source) {
            return source.getDateTime();
        }

        @Override
        public long getLong(String string) {
            Matcher m = PARSE_PATTERN.matcher(string.trim());

            if (!m.matches() || m.group(DATE_GROUP) == null) {
                throw new InvalidDateFormatException("datetime", string);
            }

            String year = m.group(DATE_YEAR_GROUP);
            String month = m.group(DATE_MONTH_GROUP);
            String day = m.group(DATE_DAY_GROUP);
            String hour = ZERO_STR;
            String minute = ZERO_STR;
            String seconds = ZERO_STR;

            if(m.group(TIME_GROUP) != null) {
                hour = m.group(TIME_HOUR_GROUP);
                minute = m.group(TIME_MINUTE_GROUP);
                seconds = m.group(TIME_SECOND_GROUP);
            }

            try {
            return  Long.parseLong(year) * DATETIME_YEAR_SCALE +
                    Long.parseLong(month) * DATETIME_MONTH_SCALE +
                    Long.parseLong(day) * DATETIME_DAY_SCALE +
                    Long.parseLong(hour) * DATETIME_HOUR_SCALE +
                    Long.parseLong(minute) * DATETIME_MIN_SCALE +
                    Long.parseLong(seconds) * DATETIME_SEC_SCALE;
            } catch (NumberFormatException ex) {
                throw new InvalidDateFormatException("datetime", string);
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
            return stdLongToUnix(longVal, DateTimeZone.getDefault());
        }
        
        @Override
        public long stdLongToUnix(long longVal, DateTimeZone tz) {
            long year = longVal / DATETIME_YEAR_SCALE;
            long month = longVal / DATETIME_MONTH_SCALE % 100;
            long day = longVal / DATETIME_DAY_SCALE % 100;
            long hour = longVal / DATETIME_HOUR_SCALE % 100;
            long minute = longVal / DATETIME_MIN_SCALE % 100;
            long second = longVal / DATETIME_SEC_SCALE % 100;
            return Calculator.getMillis((int)year, (int)month, (int)day, (int)hour, (int)minute, (int)second, tz);
        }

        @Override
        public long unixToStdLong(long unixVal) {
            return unixToStdLong(unixVal, DateTimeZone.getDefault());
        }
        
        @Override
        public long unixToStdLong(long unixVal, DateTimeZone tz) {
            long rst[] = Calculator.getYMDHMS(unixVal, tz);
            return (rst[0] * 10000 + rst[1] * 100 + rst[2]) *1000000L + rst[3] * 10000 + rst[4] * 100 + rst[5];
        }

        // TODO: support converting tz
        @Override
        public long[] getYearMonthDayHourMinuteSecond (long value) {
            return getYearMonthDayHourMinuteSecond(value, DateTimeZone.getDefault());
        }
        
        @Override
        public long[] getYearMonthDayHourMinuteSecond (long value, DateTimeZone tz) {
            final long year = (value / DATETIME_YEAR_SCALE);
            final long month = (value / DATETIME_MONTH_SCALE) % 100;
            final long day = (value / DATETIME_DAY_SCALE) % 100;
            long hour = value / DATETIME_HOUR_SCALE % 100;
            long minute = value / DATETIME_MIN_SCALE % 100;
            long second = value / DATETIME_SEC_SCALE % 100;
            return new long[] {year, month, day, hour, minute, second};
        }

        @Override
        public long getEncoded (long[] ymd_hms) {
            return getEncoded(ymd_hms, DateTimeZone.getDefault());
        }
        @Override
        public long getEncoded(long[] ymd_hms, DateTimeZone tz) {
            return ymd_hms[0] * DATETIME_YEAR_SCALE +
                   ymd_hms[1] * DATETIME_MONTH_SCALE +
                   ymd_hms[2] * DATETIME_DAY_SCALE +
                   ymd_hms[3] * DATETIME_HOUR_SCALE +
                   ymd_hms[4] * DATETIME_MIN_SCALE +
                   ymd_hms[5] * DATETIME_SEC_SCALE;
        }

        @Override
        protected long doConvert(ValueSource source)
        {
            long ymd[];
            switch (source.getConversionType())
            {
                case DATE:      ymd = DATE.getYearMonthDayHourMinuteSecond(source.getDate()); break;
                case TIMESTAMP: ymd = TIMESTAMP.getYearMonthDayHourMinuteSecond(source.getTimestamp()); break;
                default:    throw unsupportedConversion(source.getConversionType());
            }
            return getEncoded(ymd);
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
            return stdLongToUnix(longVal, DateTimeZone.getDefault());
        }
        
        @Override
        public long stdLongToUnix(long longVal, DateTimeZone tz) {
            boolean pos;
            long abs = ((pos = longVal >= 0L) ? longVal : -longVal);
            long hour = abs / TIME_HOURS_SCALE;
            long minute = (abs - hour* TIME_HOURS_SCALE) / TIME_MINUTES_SCALE;
            long second = abs - hour* TIME_HOURS_SCALE - minute* TIME_MINUTES_SCALE;

            // .TIME doesn't have date field, so assume 1970,1,1 as the date
            return Calculator.getMillis(1970, 1, 1, (int)hour, (int)minute, (int)second, tz) * (pos ? 1 :-1);
        }

        @Override
        public long unixToStdLong (long unixVal) {
            return unixToStdLong(unixVal, DateTimeZone.getDefault());
        }
        
        @Override
        public long unixToStdLong(long unixVal, DateTimeZone tz) {
            int rst[] = Calculator.getHrMinSec(unixVal, tz);
            return rst[0]* TIME_HOURS_SCALE + rst[1]* TIME_MINUTES_SCALE + rst[2];
        }

        // TODO: support converting tz
        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value) {
            return getYearMonthDayHourMinuteSecond(value, DateTimeZone.getDefault());
        }
        
        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value, DateTimeZone tz) {
            long abs = value > 0 ? value : - value;
            long hour = abs / TIME_HOURS_SCALE;
            long minute = (abs - hour* TIME_HOURS_SCALE) / TIME_MINUTES_SCALE;
            long second = abs - hour* TIME_HOURS_SCALE - minute* TIME_MINUTES_SCALE;
            return new long [] {0, 1, 1, hour, minute, second};
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            return getEncoded(ymd_hms, DateTimeZone.getDefault());
        }
        
        @Override
        public long getEncoded(long[] ymd_hms, DateTimeZone tz) {
            return ymd_hms[3] * 10000 +
                   ymd_hms[4] * 100 +
                   ymd_hms[5];
        }

        @Override
        protected long doConvert(ValueSource source)
        {
            switch (source.getConversionType())
            {
                case YEAR:
                case DATE:      return 0;
                case DATETIME:  return source.getDateTime() % 1000000L;
                default:        return getEncoded(TIMESTAMP.getYearMonthDayHourMinuteSecond(source.getTimestamp()));
            }
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

        // TODO: support converting tz
        @Override
        public long stdLongToUnix(long longVal, DateTimeZone tz) {
            return longVal * 1000;
        }
        
        @Override
        public long stdLongToUnix(long longVal) {
            return longVal * 1000;
        }

        @Override
        public long unixToStdLong (long unixVal, DateTimeZone tz) {
            return unixVal / 1000;
        }
        
        @Override
        public long unixToStdLong(long unixVal) {
            return unixVal / 1000;
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value, DateTimeZone tz) {
            return Calculator.getYMDHMS(value * 1000, tz);
        }
        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value) {
            return Calculator.getYMDHMS(value * 1000);
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            return getEncoded(ymd_hms, DateTimeZone.getDefault());
        }
        
        @Override
        public long getEncoded(long[] ymd_hms, DateTimeZone tz) {
            return Calculator.getMillis((int)ymd_hms[0], 
                                        (int)ymd_hms[1], 
                                        (int)ymd_hms[2], 
                                        (int)ymd_hms[3], 
                                        (int)ymd_hms[4], 
                                        (int)ymd_hms[5],
                                        tz);
        }

        @Override
        protected long doConvert(ValueSource source)
        {
            long rawLong = 0;
            switch(source.getConversionType())
            {
                case DATE:      rawLong = source.getDate();
                                return rawLong == 0 ? 0 : DATE.stdLongToUnix(rawLong)/ 1000;
                case DATETIME:  rawLong = source.getDateTime();
                                return rawLong == 0 ? 0 : DATETIME.stdLongToUnix(rawLong) / 1000;
                default:        throw unsupportedConversion(source.getConversionType());
            }
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
            return stdLongToUnix(longVal, DateTimeZone.getDefault());
        }
        
        @Override
        public long stdLongToUnix(long longVal, DateTimeZone tz) {
            return Calculator.getMillis(longVal == 0 ? 0 :1900 + (int)longVal, 1, 1, 0, 0, 0, tz);
        }

        @Override
        public long unixToStdLong(long unixVal) {
            return unixToStdLong(unixVal, DateTimeZone.getDefault());
        }
        
        @Override
        public long unixToStdLong(long unixVal, DateTimeZone tz) {
           long yr = Calculator.getYear(unixVal, tz);
           return yr == 0L ? 0 : yr - 1900 ;
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value) {
            return getYearMonthDayHourMinuteSecond(value, DateTimeZone.getDefault());
        }
        
        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value, DateTimeZone tz) {
            return new long[] {value == 0 ? 0 : 1900 + value, 1, 1, 0, 0, 0};
        }

        @Override
        public long getEncoded(long[] ymd_hms) {
            return getEncoded(ymd_hms, DateTimeZone.getDefault());
        }
        
        @Override
        public long getEncoded(long[] ymd_hms, DateTimeZone tz) {
            return ymd_hms[0] == 0 ? 0 : ymd_hms[0] - 1900;
        }

        @Override
        protected long doConvert(ValueSource source)
        {
            long ymd[];
            switch (source.getConversionType())
            {
                case DATE:      ymd = DATE.getYearMonthDayHourMinuteSecond(source.getDate()); break;
                case DATETIME:  ymd = DATETIME.getYearMonthDayHourMinuteSecond(source.getDateTime()); break;
                case TIMESTAMP: ymd = TIMESTAMP.getYearMonthDayHourMinuteSecond(source.getTimestamp()); break;
                default:        throw unsupportedConversion(source.getConversionType());
            }
            return getEncoded(ymd);
        }
    };

    final static ExtractorsForDates INTERVAL_MILLIS = new ExtractorsForDates(AkType.INTERVAL_MILLIS)
    {
        @Override
        protected long doGetLong(ValueSource source){
            AkType type = source.getConversionType();
            switch (type){
                case INTERVAL_MILLIS:  return source.getInterval_Millis();
                case DECIMAL:   return source.getDecimal().setScale(0, RoundingMode.HALF_UP).longValue();
                case DOUBLE:    return Math.round(source.getDouble());
                case U_BIGINT:  return source.getUBigInt().longValue();
                case VARCHAR:   return getLong(source.getString());
                case TEXT:      return getLong(source.getText());
                case LONG:      return source.getLong();
                case INT:       return source.getInt();
                case U_INT:     return source.getUInt();
                case U_DOUBLE:  return Math.round(source.getUDouble());
                case FLOAT:     return Math.round(source.getFloat());
                case U_FLOAT:   return Math.round(source.getUFloat());
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
                throw new InvalidIntervalFormatException ("INTERVAL_MILLIS", st);
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
            throw new UnsupportedOperationException("Unsupported: Cannot encode Year/Month/... to INTERVAL_MILLIS");
        }

        @Override
        protected long doConvert(ValueSource source)
        {
            throw unsupportedConversion(source.getConversionType());
        }

        // TODO: Could support converting value in one timezone to  another
        @Override
        public long stdLongToUnix(long longVal, DateTimeZone tz)
        {
            return longVal;
        }

        @Override
        public long unixToStdLong(long unixVal, DateTimeZone tz)
        {
            return unixVal;
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value, DateTimeZone tz)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long getEncoded(long[] ymd_hms, DateTimeZone tz)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    };

    final static ExtractorsForDates INTERVAL_MONTH = new ExtractorsForDates(AkType.INTERVAL_MONTH)
    {
        @Override
        protected long doGetLong(ValueSource source){
            AkType type = source.getConversionType();
            switch (type){
                case INTERVAL_MONTH:  return source.getInterval_Month();
                case DECIMAL:   return source.getDecimal().setScale(0, RoundingMode.HALF_UP).longValue();
                case DOUBLE:    return Math.round(source.getDouble());
                case U_BIGINT:  return source.getUBigInt().longValue();
                case VARCHAR:   return getLong(source.getString());
                case TEXT:      return getLong(source.getText());
                case LONG:      return source.getLong();
                case INT:       return source.getInt();
                case U_INT:     return source.getUInt();
                case U_DOUBLE:  return Math.round(source.getUDouble());
                case FLOAT:     return Math.round(source.getFloat());
                case U_FLOAT:   return Math.round(source.getUFloat());
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
                throw new InvalidIntervalFormatException ("INTERVAL_MONTH", st);
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

        @Override
        protected long doConvert(ValueSource source)
        {
            throw unsupportedConversion(source.getConversionType());
        }

        // TODO: could support converting value in one timezone to another
        @Override
        public long stdLongToUnix(long longVal, DateTimeZone tz)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long unixToStdLong(long unixVal, DateTimeZone tz)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long[] getYearMonthDayHourMinuteSecond(long value, DateTimeZone tz)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long getEncoded(long[] ymd_hms, DateTimeZone tz)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    };

    private static class Calculator {
        public static long getMillis (int year, int mon, int day, int hr, int min, int sec, DateTimeZone tz) {
            return new DateTime(year, mon, day, hr, min, sec, tz).getMillis();
        }
        
        public static long getMillis (int year, int mon, int day, int hr, int min, int sec) {
            return (new DateTime(year, mon, day, hr, min, sec, DateTimeZone.getDefault())).getMillis();
        }

        public static long getYear (long millis, DateTimeZone tz) {
            return new DateTime(millis, tz).getYear();
        }
        
        public static long getYear (long millis)  {
            return (new DateTime(millis, DateTimeZone.getDefault())).getYear();
        }

        public static long getMonth (long millis, DateTimeZone tz) {
            return new DateTime(millis, tz).getMonthOfYear();
        }
        
        public static int getMonth (long millis) {
            return (new DateTime(millis, DateTimeZone.getDefault())).getMonthOfYear();
        }

        public static int getDay (long millis, DateTimeZone tz) {
            return new DateTime(millis, tz).getDayOfMonth();
        }
        
        public static int getDay (long millis) {
            return (new DateTime(millis, DateTimeZone.getDefault())).getDayOfMonth();
        }

        public static int getHour (long millis, DateTimeZone tz) {
            return new DateTime(millis, tz).getHourOfDay();
        }
        
        public static int getHour (long millis) {
            return (new DateTime(millis, DateTimeZone.getDefault())).getHourOfDay();
        }

        public static int getMinute (long millis, DateTimeZone tz) {
            return new DateTime(millis, tz).getMinuteOfHour();
        }
        
        public static int getMinute (long millis) {
            return (new DateTime(millis, DateTimeZone.getDefault())).getMinuteOfHour();
        }

        public static int getSec (long millis, DateTimeZone tz) {
            return new DateTime(millis, tz).getSecondOfMinute();
        }
        
        public static int getSec (long millis) {
            return (new DateTime(DateTimeZone.getDefault())).getSecondOfMinute();
        }

        public static long[] getYearMonthDay (long millis, DateTimeZone tz) {
            DateTime date = new DateTime(millis, tz);
            return new long[] {date.getYear(), date.getMonthOfYear(), date.getDayOfMonth()};
        }
        
        public static long[] getYearMonthDay (long millis) {
            DateTime date = new DateTime(millis, DateTimeZone.getDefault());
            return new long[] {date.getYear(), date.getMonthOfYear(), date.getDayOfMonth()};
        }

        public static int[] getHrMinSec (long millis, DateTimeZone tz) {
            DateTime date = new DateTime(millis, tz);
            return new int[] {date.getHourOfDay(), date.getMinuteOfHour(), date.getSecondOfMinute()};
        }

        public static int[] getHrMinSec (long millis) {
            DateTime date = new DateTime(millis, DateTimeZone.getDefault());
            return new int[] {date.getHourOfDay(), date.getMinuteOfHour(), date.getSecondOfMinute()};
        }
        
        public static long[] getYMDHMS(long millis, DateTimeZone tz)
        {
            DateTime date = new DateTime(millis, tz);
            return new long[]
                    {
                        date.getYear(), date.getMonthOfYear(), date.getDayOfMonth(),
                        date.getHourOfDay(), date.getMinuteOfHour(), date.getSecondOfMinute()
                    };
        }
        
        public static long[] getYMDHMS(long millis)
        {
            DateTime date = new DateTime(millis, DateTimeZone.getDefault());

            return new long[]
                    {
                        date.getYear(), date.getMonthOfYear(), date.getDayOfMonth(),
                        date.getHourOfDay(), date.getMinuteOfHour(), date.getSecondOfMinute()
                    };
        }
    }

    private static final EnumSet<AkType> DATETIMES = EnumSet.of(AkType.DATE, AkType.DATETIME, AkType.TIME, AkType.TIMESTAMP, AkType.YEAR);
    protected abstract long doGetLong(ValueSource source);
    protected abstract long doConvert(ValueSource source);

    @Override
    public long getLong(ValueSource source) {
        if (source.isNull())
            throw new ValueSourceIsNullException();
        AkType type = source.getConversionType();
        if (type == targetConversionType() || targetConversionType() == AkType.INTERVAL_MILLIS
                                           || targetConversionType() == AkType.INTERVAL_MONTH) {
            return doGetLong(source);
        }
        if (DATETIMES.contains(type)) return doConvert(source);
        
        switch (type) {
        case TEXT:      return getLong(source.getText());
        case VARCHAR:   return getLong(source.getString());
        case INT:       return source.getInt();
        case U_INT:     return source.getUInt();
        case LONG:      return source.getLong();
        case U_FLOAT:   return Math.round(source.getUFloat());
        case FLOAT:     return Math.round(source.getFloat());
        case DOUBLE:    return Math.round(source.getDouble());
        case U_DOUBLE:  return Math.round(source.getUDouble());
        case DECIMAL:   return source.getDecimal().setScale(0, RoundingMode.HALF_UP).longValue();
        default: throw unsupportedConversion(type);
        }
    }

    // testing hooks

    static void setGlobalTimezone(String timezone) {
        dateFormatProvider.set(new DateFormatProvider(timezone));
        DateTimeZone.setDefault(DateTimeZone.forID(timezone));
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
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
