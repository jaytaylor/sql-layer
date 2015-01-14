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
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TBundleID;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.mcompat.MParsers;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.mcompat.mcasts.CastUtils;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import java.text.DateFormatSymbols;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.joda.time.base.AbstractDateTime;
import org.joda.time.base.BaseDateTime;

public class MDateAndTime
{
    private static final TBundleID MBundleID = MBundle.INSTANCE.id();

    public static final NoAttrTClass DATE = new NoAttrTClass(MBundleID,
            "date", AkCategory.DATE_TIME, FORMAT.DATE, 1, 1, 3, UnderlyingType.INT_32, MParsers.DATE, 10, TypeId.DATE_ID)
    {
        @Override
        public TClass widestComparable()
        {
            return DATETIME;
        }
    };
    public static final NoAttrTClass DATETIME = new NoAttrTClass(MBundleID,
            "datetime", AkCategory.DATE_TIME, FORMAT.DATETIME,  1, 1, 8, UnderlyingType.INT_64, MParsers.DATETIME, 19, TypeId.DATETIME_ID);
    public static final NoAttrTClass TIME = new NoAttrTClass(MBundleID,
            "time", AkCategory.DATE_TIME, FORMAT.TIME, 1, 1, 3, UnderlyingType.INT_32, MParsers.TIME, 8, TypeId.TIME_ID);
    public static final NoAttrTClass YEAR = new NoAttrTClass(MBundleID,
            "year", AkCategory.DATE_TIME, FORMAT.YEAR, 1, 1, 1, UnderlyingType.INT_16, MParsers.YEAR, 4, TypeId.YEAR_ID);
    public static final NoAttrTClass TIMESTAMP = new NoAttrTClass(MBundleID,
            "timestamp", AkCategory.DATE_TIME, FORMAT.TIMESTAMP, 1, 1, 4, UnderlyingType.INT_32, MParsers.TIMESTAMP, 19, TypeId.TIMESTAMP_ID);

    /** Locale.getLanguage() -> String[] of month names. */
    public static final Map<String, String[]> MONTHS;

    static {
        Locale[] supportedLocales = { Locale.ENGLISH };
        Map<String, String[]> months = new HashMap<>();
        for(Locale l : supportedLocales) {
            DateFormatSymbols fm = new DateFormatSymbols(l);
            months.put(l.getLanguage(), fm.getMonths());
        }
        MONTHS = Collections.unmodifiableMap(months);
    }

    
    public static enum FORMAT implements TClassFormatter {
        DATE {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(dateToString(source.getInt32()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                out.append("DATE '");
                out.append(dateToString(source.getInt32()));
                out.append("'");
            }
        }, 
        DATETIME {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(dateTimeToString(source.getInt64()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                out.append("TIMESTAMP '");
                out.append(dateTimeToString(source.getInt64()));
                out.append("'");
            }
        }, 
        TIME {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(timeToString(source.getInt32()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                out.append("TIME '");
                out.append(timeToString(source.getInt32()));
                out.append("'");
            }
        }, 
        YEAR {         
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out)
            {
                short raw = source.getInt16();
                if (raw == 0)
                    out.append("0000");
                else
                    out.append(raw + 1900);
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                format(type, source, out);
            }
        }, 
        TIMESTAMP {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(timestampToString(source.getInt32(), null));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                out.append("TIMESTAMP '");
                out.append(timestampToString(source.getInt32(), null));
                out.append("'");
            }
        };

        @Override
        public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
            out.append('"');
            format(type, source, out);
            out.append('"');
        }
    }

    public static String getMonthName(int numericRep, String locale, TExecutionContext context) {
        String[] monthNames = MONTHS.get(locale);
        if(monthNames == null) {
            context.reportBadValue("Unsupported locale: " + locale);
            return null;
        }
        numericRep -= 1;
        if(numericRep > monthNames.length || numericRep < 0) {
            context.reportBadValue("Month out of range: " + numericRep);
            return null;
        }
        return monthNames[numericRep];
    }
    
    public static long[] fromJodaDateTime(AbstractDateTime date) {
        return new long[] {
            date.getYear(),
            date.getMonthOfYear(),
            date.getDayOfMonth(),
            date.getHourOfDay(),
            date.getMinuteOfHour(),
            date.getSecondOfMinute()
        };
    }

    public static MutableDateTime toJodaDateTime(long[] dt, String tz) {
        return toJodaDateTime(dt, DateTimeZone.forID(tz));
    }

    public static MutableDateTime toJodaDateTime(long[] dt, DateTimeZone dtz) {
        return new MutableDateTime((int)dt[YEAR_INDEX],
                                   (int)dt[MONTH_INDEX],
                                   (int)dt[DAY_INDEX],
                                   (int)dt[HOUR_INDEX],
                                   (int)dt[MIN_INDEX],
                                   (int)dt[SEC_INDEX],
                                   0,
                                   dtz);
    }

    public static String dateToString(int encodedDate) {
        long[] dt = decodeDate(encodedDate);
        System.out.println(String.format("Returning: %d-%d-%d %d:%d:%d",dt[YEAR_INDEX],
                                         dt[MONTH_INDEX],
                                         dt[DAY_INDEX],
                                         dt[HOUR_INDEX],
                                         dt[MIN_INDEX],
                                         dt[SEC_INDEX]));
        long[] dt2 = decodeDateTime(encodedDate);
        System.out.println(String.format("Full: %d-%d-%d %d:%d:%d",dt2[YEAR_INDEX],
                                         dt2[MONTH_INDEX],
                                         dt2[DAY_INDEX],
                                         dt2[HOUR_INDEX],
                                         dt2[MIN_INDEX],
                                         dt2[SEC_INDEX]));
        return String.format("%04d-%02d-%02d", dt[YEAR_INDEX], dt[MONTH_INDEX], dt[DAY_INDEX]);
    }

    /**
     * Parse {@code input} as a DATE and then {@link #encodeDate(long, long, long)}.
     * @throws InvalidDateFormatException
     */
    public static int parseAndEncodeDate(String input) {
        long[] dt = new long[6];
        StringType type = parseDateOrTime(input, dt);
        switch(type) {
            case DATE_ST:
            case DATETIME_ST:
                return encodeDate(dt);
        }
        throw new InvalidDateFormatException("date", input);
    }

    /** Convert millis to a date in the given timezone and then {@link #encodeDate(long, long, long)}. */
    public static int encodeDate(long millis, String tz) {
        DateTime dt = new DateTime(millis, DateTimeZone.forID(tz));
        return encodeDate(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
    }

    /** Convenience for {@link #encodeDate(long, long, long)}. */
    public static int encodeDate(long[] dt) {
        return encodeDate(dt[YEAR_INDEX], dt[MONTH_INDEX], dt[DAY_INDEX]);
    }

    /** Encode the year, month and date in the MySQL internal DATE format. */
    public static int encodeDate(long y, long m, long d) {
        return (int)(y * 512 + m * 32 + d);
    }

    /** Decode the MySQL internal DATE format long into an array. */
    public static long[] decodeDate(long encodedDate) {
        return new long[] {
            encodedDate / 512,
            encodedDate / 32 % 16,
            encodedDate % 32,
            0,
            0,
            0
        };
    }

    /** Parse an *external* date long (e.g. YYYYMMDD => 20130130) into a array. */
    public static long[] parseDate(long val) {
        long[] dt = parseDateTime(val);
        dt[HOUR_INDEX] = dt[MIN_INDEX] = dt[SEC_INDEX] = 0;
        return dt;
    }

    /** Parse an *external* datetime long (e.g. YYYYMMDDHHMMSS => 20130130) into a array. */
    public static long[] parseDateTime(long val) {
        if((val != 0) && (val <= 100)) {
            throw new InvalidDateFormatException("date", Long.toString(val));
        }
        // Pad out HHMMSS if needed
        if(val < DATETIME_MONTH_SCALE) {
            val *= DATETIME_DATE_SCALE;
        }
        // External is same as internal, though a two-digit year may need converted
        long[] dt = decodeDateTime(val);
        if(val != 0) {
            dt[YEAR_INDEX] = adjustTwoDigitYear(dt[YEAR_INDEX]);
        }
        if(!isValidDateTime_Zeros(dt)) {
            throw new InvalidDateFormatException("date", Long.toString(val));
        }
        if(!isValidHrMinSec(dt, true, true)) {
            throw new InvalidDateFormatException("datetime", Long.toString(val));
        }
        return dt;
    }

    /** Decode {@code encodedDateTime} and format it as a string. */
    public static String dateTimeToString(long encodedDateTime) {
        long[] dt = decodeDateTime(encodedDateTime);
        return dateTimeToString(dt);
    }

    /** Format {@code dt} as a string. */
    public static String dateTimeToString(long[] dt) {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                             dt[YEAR_INDEX],
                             dt[MONTH_INDEX],
                             dt[DAY_INDEX],
                             dt[HOUR_INDEX],
                             dt[MIN_INDEX],
                             dt[SEC_INDEX]);
    }
    
    /**
     * Attempt to parse {@code str} into as DATE and/or TIME.
     * Return type indicates success, almost success (INVALID_*) or completely unparsable.
     */
    public static StringType parseDateOrTime(String st, long[] dt) {
        assert dt.length >= MAX_INDEX;

        st = st.trim();
        String year = "0";
        String month = "0";
        String day = "0";
        String hour = "0";
        String minute = "0";
        String seconds = "0";

        Matcher matcher;
        if((matcher = DATE_PATTERN.matcher(st)).matches()) {
            StringType type = StringType.DATE_ST;
            year = matcher.group(DATE_YEAR_GROUP);
            month = matcher.group(DATE_MONTH_GROUP);
            day = matcher.group(DATE_DAY_GROUP);
            if(matcher.group(TIME_GROUP) != null) {
                type = StringType.DATETIME_ST;
                hour = matcher.group(TIME_HOUR_GROUP);
                minute = matcher.group(TIME_MINUTE_GROUP);
                seconds = matcher.group(TIME_SECOND_GROUP);
            }
            if(stringsToLongs(dt, true, year, month, day, hour, minute, seconds) &&
               isValidDateTime_Zeros(dt)) {
                return type;
            }
            return (type == StringType.DATETIME_ST) ? StringType.INVALID_DATETIME_ST : StringType.INVALID_DATE_ST;
        }
        else if((matcher = TIME_WITH_DAY_PATTERN.matcher(st)).matches())
        {   
            day = matcher.group(MDateAndTime.TIME_WITH_DAY_DAY_GROUP);
            hour = matcher.group(MDateAndTime.TIME_WITH_DAY_HOUR_GROUP);
            minute = matcher.group(MDateAndTime.TIME_WITH_DAY_MIN_GROUP);
            seconds = matcher.group(MDateAndTime.TIME_WITH_DAY_SEC_GROUP);
            if(stringsToLongs(dt, false, year, month, day, hour, minute, seconds) &&
               isValidHrMinSec(dt, false, false)) {
                // adjust DAY to HOUR 
                int sign = 1;
                if(dt[DAY_INDEX] < 0) {
                    dt[DAY_INDEX] *= (sign = -1);
                }
                dt[HOUR_INDEX] = sign * (dt[HOUR_INDEX] += dt[DAY_INDEX] * 24);
                dt[DAY_INDEX] = 0;
                return StringType.TIME_ST;
            }
            return StringType.INVALID_TIME_ST;
        }
        else if((matcher = TIME_WITHOUT_DAY_PATTERN.matcher(st)).matches())
        {
            hour = matcher.group(MDateAndTime.TIME_WITHOUT_DAY_HOUR_GROUP);
            minute = matcher.group(MDateAndTime.TIME_WITHOUT_DAY_MIN_GROUP);
            seconds = matcher.group(MDateAndTime.TIME_WITHOUT_DAY_SEC_GROUP);
            if(stringsToLongs(dt, false, year, month, day, hour, minute, seconds) &&
               isValidHrMinSec(dt, false, false)) {
                return StringType.TIME_ST;
            }
            return StringType.INVALID_TIME_ST;
        }
        else // last attempt, split by any DELIM and look for 3 or 6 parts
        {
            String[] parts = st.split("\\s++");
            if(parts.length == 2) {
                String[] dTok = parts[0].split(DELIM);
                String[] tTok = parts[1].split(DELIM);
                if((dTok.length == 3) && (tTok.length == 3)) {
                    if(stringsToLongs(dt, true, dTok[0], dTok[1], dTok[2], tTok[0], tTok[1], tTok[2]) &&
                       isValidDateTime_Zeros(dt)) {
                        return StringType.DATETIME_ST;
                    }
                    return StringType.INVALID_DATETIME_ST;
                }
            } else if(parts.length == 1) {
                String[] dTok = parts[0].split(DELIM);
                if(dTok.length == 3) {
                    if(stringsToLongs(dt, true, dTok[0], dTok[1], dTok[2]) &&
                       isValidDateTime_Zeros(dt)) {
                        return StringType.DATE_ST;
                    }
                    return StringType.INVALID_DATE_ST;
                }
            }
        }
        return StringType.UNPARSABLE;
    }

    /**
     * Parse {@code input} as a DATETIME and then {@link #encodeDateTime(long, long, long, long, long, long)}.
     * @throws InvalidDateFormatException
     */
    public static long parseAndEncodeDateTime(String input) {
        long[] dt = new long[6];
        StringType type = parseDateOrTime(input, dt);
        switch(type) {
            case DATE_ST:
            case DATETIME_ST:
                return encodeDateTime(dt);
        }
        throw new InvalidDateFormatException("datetime", input);
    }

    /** Convert millis to a DateTime and {@link #encodeDateTime(BaseDateTime)}. */
    public static long encodeDateTime(long millis, String tz) {
        DateTime dt = new DateTime(millis, DateTimeZone.forID(tz));
        return encodeDateTime(dt);
    }

    /** Pass components of {@code dt} to {@link #encodeDateTime(long, long, long, long, long, long)}. */
    public static long encodeDateTime(BaseDateTime dt) {
        return encodeDateTime(dt.getYear(),
                              dt.getMonthOfYear(),
                              dt.getDayOfMonth(),
                              dt.getHourOfDay(),
                              dt.getMinuteOfHour(),
                              dt.getSecondOfMinute());
    }

    /** Convenience for {@link #encodeDateTime(long, long, long, long, long, long)}. */
    public static long encodeDateTime(long[] dt) {
        return encodeDateTime(dt[YEAR_INDEX],
                              dt[MONTH_INDEX],
                              dt[DAY_INDEX],
                              dt[HOUR_INDEX],
                              dt[MIN_INDEX],
                              dt[SEC_INDEX]);
    }

    /** Encode the given date and time in the MySQL DATETIME internal format long. */
    public static long encodeDateTime(long year, long month, long day, long hour, long min, long sec) {
        return year * DATETIME_YEAR_SCALE +
               month * DATETIME_MONTH_SCALE +
               day * DATETIME_DAY_SCALE +
               hour * DATETIME_HOUR_SCALE +
               min * DATETIME_MIN_SCALE +
               sec;
    }

    /** Decode the MySQL DATETIME internal format long into a array. */
    public static long[] decodeDateTime(long encodedDateTime) {
        return new long[] {
            encodedDateTime / DATETIME_YEAR_SCALE,
            encodedDateTime / DATETIME_MONTH_SCALE % 100,
            encodedDateTime / DATETIME_DAY_SCALE % 100,
            encodedDateTime / DATETIME_HOUR_SCALE % 100,
            encodedDateTime / DATETIME_MIN_SCALE % 100,
            encodedDateTime % 100
        };
    }
    
    public static String timeToString(int encodedTime) {
        long[] dt = decodeTime(encodedTime);
        return timeToString(dt);
    }

    public static String timeToString(long[] dt) {
        return timeToString(dt[HOUR_INDEX], dt[MIN_INDEX], dt[SEC_INDEX]);
    }

    public static String timeToString(long h, long m, long s) {
        return String.format("%s%02d:%02d:%02d",
                             isHrMinSecNegative(h, m, s) ? "-" : "",
                             Math.abs(h),
                             Math.abs(m),
                             Math.abs(s));
    }

    public static void timeToDatetime(long[] dt) {
        dt[YEAR_INDEX] = adjustTwoDigitYear(dt[HOUR_INDEX]);
        dt[MONTH_INDEX] = dt[MIN_INDEX];
        dt[DAY_INDEX] = dt[SEC_INDEX];
        // erase the time portion
        dt[HOUR_INDEX] = 0;
        dt[MIN_INDEX] = 0;
        dt[SEC_INDEX] = 0;
    }
    
    public static int parseTime(String string, TExecutionContext context) {
        long[] dt = new long[6];
        StringType type = parseDateOrTime(string, dt);
        switch(type) {
            case TIME_ST:
            case DATE_ST:
            case DATETIME_ST:
                if(isValidDate_Zeros(dt) && isValidHrMinSec(dt, true, true)) {
                    dt[YEAR_INDEX] = dt[MONTH_INDEX] = dt[DAY_INDEX] = 0;
                    break;
                }
                // fall
            break;
            default:
                throw new InvalidDateFormatException("TIME", string);

        }
        if(!isValidHrMinSec(dt, false, false)) {
            throw new InvalidDateFormatException("time", string);
        }
        return encodeTime(dt, context);
    }

    public static long[] decodeTime(long encodedTime) {
        boolean isNegative = (encodedTime < 0);
        if(isNegative) {
            encodedTime = -encodedTime;
        }
        // TODO: Fake date is just asking for trouble but numerous callers depend on it.
        long ret[] =  new long[] {
            1970,
            1,
            1,
            encodedTime / DATETIME_HOUR_SCALE,
            encodedTime / DATETIME_MIN_SCALE % 100,
            encodedTime % 100
        };
        if(isNegative) {
            for(int i = HOUR_INDEX; i < ret.length; ++i) {
                if(ret[i] != 0) {
                    ret[i] = -ret[i];
                    break;
                }
            }
         }
         return ret;
    }
    
    /** Convert {@code millis} to a DateTime and {@link #encodeTime(long, long, long, TExecutionContext)}. */
    public static int encodeTime(long millis, String tz) {
        DateTime dt = new DateTime(millis, DateTimeZone.forID(tz));
        return encodeTime(dt.getHourOfDay(),
                          dt.getMinuteOfHour(),
                          dt.getSecondOfMinute(),
                          null);
    }

    /** Convenience for {@link #encodeTime(long, long, long, TExecutionContext)}. */
    public static int encodeTime(long[] dt, TExecutionContext context) {
        return encodeTime(dt[HOUR_INDEX],
                          dt[MIN_INDEX],
                          dt[SEC_INDEX],
                          context);
    }

    /** Encode hour, minute and second as a MySQL internal TIME value. {@code context} may be null. */
    public static int encodeTime(long h, long m, long s, TExecutionContext context) {
        int sign = isHrMinSecNegative(h, m, s) ? -1 : 1;
        long ret = sign * (Math.abs(h) * DATETIME_HOUR_SCALE + (Math.abs(m) * DATETIME_MIN_SCALE) + Math.abs(s));
        if(context != null) {
            return (int)CastUtils.getInRange(TIME_MAX, TIME_MIN, ret, context);
        }
        return (int)((ret < TIME_MIN) ? TIME_MIN : (ret > TIME_MAX ? TIME_MAX : ret));
    }

    /** Parse {@code input} as a TIMESTAMP in the {@code tz} timezone. */
    public static int parseAndEncodeTimestamp(String input, String tz, TExecutionContext context) {
        long[] dt = new long[6];
        StringType type = parseDateOrTime(input, dt);
        switch(type) {
            case DATE_ST:
            case DATETIME_ST:
                return encodeTimestamp(dt, tz, context);
            default:
                // e.g. SELECT UNIX_TIMESTAMP('1920-21-01 00:00:00') -> 0
                context.warnClient(new InvalidDateFormatException("timestamp", input));
                return 0;
        }
    }

    /** Decode {@code encodedTimestamp} using the {@code tz} timezone. */
    public static long[] decodeTimestamp(long encodedTimestamp, String tz)  {
        DateTime dt = new DateTime(encodedTimestamp * 1000L, DateTimeZone.forID(tz));
        return new long[] {
            dt.getYear(),
            dt.getMonthOfYear(),
            dt.getDayOfMonth(),
            dt.getHourOfDay(),
            dt.getMinuteOfHour(),
            dt.getSecondOfMinute()
        };
    }

    /** Convenience for {@link #toJodaDateTime(long[], String)} and {@link #encodeTimestamp(long, TExecutionContext)}. */
    public static int encodeTimestamp(long[] dt, String tz, TExecutionContext context) {
        return encodeTimestamp(toJodaDateTime(dt, tz), context);
    }

    /** Convert {@code dateTime} to milliseconds and {@link #encodeTimestamp(long, TExecutionContext)}. */
    public static int encodeTimestamp(BaseDateTime dateTime, TExecutionContext context) {
        return encodeTimestamp(dateTime.getMillis(), context);
    }

    /** Encode {@code millis} as an internal MySQL TIMESTAMP. Clamps to MIN/MAX range. */
    public static int encodeTimestamp(long millis, TExecutionContext context) {
        return (int)CastUtils.getInRange(TIMESTAMP_MAX, TIMESTAMP_MIN, millis / 1000L, TS_ERROR_VALUE, context);
    }

    public static boolean isValidTimestamp(BaseDateTime dt) {
        long millis = dt.getMillis();
        return (millis >= TIMESTAMP_MIN) && (millis <= TIMESTAMP_MAX);
    }

    /** Encode {@code dt} as a MySQL internal TIMESTAMP. Range is unchecked. */
    public static int getTimestamp(long[] dt, String tz) {
        MutableDateTime dateTime = toJodaDateTime(dt, tz);
        return (int)(dateTime.getMillis() / 1000L);
    }

    /** Decode {@code encodedTimestamp} and format as a string. */
    public static String timestampToString(long encodedTimestamp, String tz) {
        long[] dt = decodeTimestamp(encodedTimestamp, tz);
        return MDateAndTime.dateTimeToString(dt);
    }

    /** Parse an hour:min or named timezone. */
    public static DateTimeZone parseTimeZone(String tz) {
        try {
            Matcher m = TZ_PATTERN.matcher(tz);
            if(m.matches()) {
                int hourSign = "-".equals(m.group(TZ_SIGN_GROUP)) ? -1 : 1;
                int hour = Integer.parseInt(m.group(TZ_HOUR_GROUP));
                int min = Integer.parseInt(m.group(TZ_MINUTE_GROUP));
                return DateTimeZone.forOffsetHoursMinutes(hourSign * hour, min);
            } else {
                // Upper 'utc' but not 'America/New_York'
                if(tz.indexOf('/') == -1) {
                    tz = tz.toUpperCase();
                }
                return DateTimeZone.forID(tz);
            }
        } catch(IllegalArgumentException e) {
            throw new InvalidDateFormatException("timezone", tz);
        }
    }

    /** {@code true} if date from {@code dt} is valid, disallowing the zero year. */
    public static boolean isValidDateTime_Zeros(long[] dt) {
        return isValidDateTime(dt, ZeroFlag.YEAR, ZeroFlag.MONTH, ZeroFlag.DAY);
    }

    public static boolean isValidDateTime(long[] dt, ZeroFlag... flags) {
        return (dt != null) &&
            isValidDate(dt, flags) &&
            isValidHrMinSec(dt, true, true);
    }

    public static boolean isHrMinSecNegative(long[] dt) {
        return isHrMinSecNegative(dt[HOUR_INDEX], dt[MIN_INDEX], dt[SEC_INDEX]);
    }
    public static boolean isHrMinSecNegative(long h, long m, long s) {
        return (h < 0) || (m < 0) || (s < 0);
    }

    /** {@code true} if time from {@code dt} is usable in functions and expressions. */
    public static boolean isValidHrMinSec(long[] dt, boolean checkHour, boolean isFromDateTime) {
        return isValidHrMinSec(dt[HOUR_INDEX], dt[MIN_INDEX], dt[SEC_INDEX], checkHour, isFromDateTime);
    }
 
    public static boolean isValidHrMinSec(long h, long m, long s, boolean checkHour, boolean isFromDateTime) {
        if(isFromDateTime) {
            // DATETIME limited to a single, positive day
            return (h >= 0) && (h <= 23) &&
                   (m >= 0) && (m <= 59) &&
                   (s >= 0) && (s <= 59);
        }
        // Otherwise must be in +-838:59:59
        int zeroCount = (h < 0 ? 1 : 0) + (m < 0 ? 1 : 0) + (s < 0 ? 1 : 0);
        return (!checkHour || (h >= -838) && (h <= 838)) &&
               (m >= -59) && (m <= 59) &&
               (s >= -59) && (s <= 59) &&
               (zeroCount <= 1);
    }

    public static boolean isZeroDayMonth(long[] dt) {
        return (dt[DAY_INDEX] == 0) || (dt[MONTH_INDEX] == 0);
    }

    public static boolean isValidDate_Zeros(long[] dt) {
        return isValidDate(dt, ZeroFlag.YEAR, ZeroFlag.MONTH, ZeroFlag.DAY);
    }

    public static boolean isValidDate_NoZeros(long[] dt) {
        return isValidDate(dt);
    }

    public static boolean isValidDate(long[] dt, ZeroFlag... flags) {
        return isValidDate(dt[YEAR_INDEX], dt[MONTH_INDEX], dt[DAY_INDEX], flags);
    }

    public static boolean isValidDate(long y, long m, long d, ZeroFlag... flags) {
        long last = getLastDay(y, m);
        return (last > 0) &&
            (d <= last) &&
            (y > 0 || contains(flags, ZeroFlag.YEAR)) &&
            (m > 0 || contains(flags, ZeroFlag.MONTH)) &&
            (d > 0 || contains(flags, ZeroFlag.DAY));
    }

    /** Convenience for {@link #getLastDay(long, long)}. */
    public static long getLastDay(long[] dt) {
        return getLastDay((int)dt[YEAR_INDEX], (int)dt[MONTH_INDEX]);
    }

    /** Get the last day for the given month in the given year. */
    public static long getLastDay(long year, long month) {
        switch((int)month) {
            case 2:
                if((year % 400 == 0) || ((year % 4 == 0) && (year % 100 != 0))) {
                    return 29;
                }
                return 28;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30L;
            case 0:
            case 1:
            case 3:
            case 5:
            case 7:
            case 8:
            case 10:
            case 12:
                return 31L;
            default:
                return -1;
        }
    }

    /**
     * MySQL docs for DATE, DATETIME and TIMESTAMP:
     * Year values in the range 00-69 are converted to 2000-2069.
     * Year values in the range 70-99 are converted to 1970-1999.
     */
    protected static long adjustTwoDigitYear(long year) {
        if(year <= 69) {
            year = year + 2000;
        } else if(year < 100) {
            year = 1900 + year;
        }
        return year;
    }

    /** {@link Long#parseLong(String)} each string. Return false if any failed. */
    private static boolean stringsToLongs(long[] dt, boolean convertYear, String... parts) {
        assert parts.length <= dt.length;
        for(int i = 0; i < parts.length; ++i) {
            try {
                if(parts[i] == null) {
                    return false;
                }
                dt[i] = Long.parseLong(parts[i].trim());
                // Must be *exactly* two-digit to get converted
                if(convertYear && (i == YEAR_INDEX) && (parts[i].length() == 2)) {
                    dt[i] = adjustTwoDigitYear(dt[i]);
                }
            } catch(NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    private static boolean contains(ZeroFlag[] flags, ZeroFlag flag) {
        for(ZeroFlag f : flags) {
            if(f == flag) {
                return true;
            }
        }
        return false;
    }


    public static final int YEAR_INDEX = 0;
    public static final int MONTH_INDEX = 1;
    public static final int DAY_INDEX = 2;
    public static final int HOUR_INDEX = 3;
    public static final int MIN_INDEX = 4;
    public static final int SEC_INDEX = 5;
    public static final int MAX_INDEX = SEC_INDEX;

    private static final long DATETIME_DATE_SCALE = 1000000L;
    private static final long DATETIME_YEAR_SCALE = 10000L * DATETIME_DATE_SCALE;
    private static final long DATETIME_MONTH_SCALE = 100L * DATETIME_DATE_SCALE;
    private static final long DATETIME_DAY_SCALE = DATETIME_DATE_SCALE;
    private static final long DATETIME_HOUR_SCALE = 10000L;
    private static final long DATETIME_MIN_SCALE = 100L;
    
    private static final int DATE_GROUP = 1;
    private static final int DATE_YEAR_GROUP = 2;
    private static final int DATE_MONTH_GROUP = 3;
    private static final int DATE_DAY_GROUP = 4;
    private static final int TIME_GROUP = 5;
    private static final int TIME_HOUR_GROUP = 7;
    private static final int TIME_MINUTE_GROUP = 8;
    private static final int TIME_SECOND_GROUP = 9;
    private static final int TIME_FRAC_GROUP = 10;
    private static final int TIME_TIMEZONE_GROUP = 11;
    private static final Pattern DATE_PATTERN 
            = Pattern.compile("^((\\d+)-(\\d+)-(\\d+))(([T]{1}|\\s+)(\\d+):(\\d+):(\\d+)(\\.\\d+)?)?[Z]?(\\s*[+-]\\d+:?\\d+(:?\\d+)?)?$");
    
    private static final int TIME_WITH_DAY_DAY_GROUP = 2;
    private static final int TIME_WITH_DAY_HOUR_GROUP = 3;
    private static final int TIME_WITH_DAY_MIN_GROUP = 4;
    private static final int TIME_WITH_DAY_SEC_GROUP = 5;
    private static final Pattern TIME_WITH_DAY_PATTERN
            = Pattern.compile("^(([-+]?\\d+)\\s+(\\d+):(\\d+):(\\d+)(\\.\\d+)?[Z]?(\\s*[+-]\\d+:?\\d+(:?\\d+)?)?)?$");

    private static final int TIME_WITHOUT_DAY_HOUR_GROUP = 2;
    private static final int TIME_WITHOUT_DAY_MIN_GROUP = 3;
    private static final int TIME_WITHOUT_DAY_SEC_GROUP = 4;
    private static final Pattern TIME_WITHOUT_DAY_PATTERN
            = Pattern.compile("^(([-+]?\\d+):(\\d+):(\\d+)(\\.\\d+)?[Z]?(\\s*[+-]\\d+:?\\d+(:?\\d+)?)?)?$");

    private static final int TZ_SIGN_GROUP = 1;
    private static final int TZ_HOUR_GROUP = 2;
    private static final int TZ_MINUTE_GROUP = 3;
    // This allows one digit hour or minute, which Joda does not.
    private static final Pattern TZ_PATTERN = Pattern.compile("([-+]?)([\\d]{1,2}):([\\d]{1,2})");

    // delimiter for a date/time/datetime string. MySQL allows almost anything to be the delimiter
    private static final String DELIM = "\\W";
    
    // upper and lower limit of TIMESTAMP value
    // as per http://dev.mysql.com/doc/refman/5.5/en/datetime.html
    public static final long TIMESTAMP_MIN = DateTime.parse("1970-01-01T00:00:01Z").getMillis();
    public static final long TIMESTAMP_MAX = DateTime.parse("2038-01-19T03:14:07Z").getMillis();
    public static final long TS_ERROR_VALUE = 0L;
    
    // upper and lower limti of TIME value
    // as per http://dev.mysql.com/doc/refman/5.5/en/time.html
    public static final int TIME_MAX = 8385959;
    public static final int TIME_MIN = -8385959;
    
    public static boolean isValidType(StringType type)
    {
        switch(type) {
            case DATE_ST:
            case DATETIME_ST:
            case TIME_ST:
                return true;
            default:
                return false;
        }
    }

    public static enum ZeroFlag {
        YEAR,
        MONTH,
        DAY
    }

    public static enum StringType
    {
        DATE_ST,
        DATETIME_ST,
        TIME_ST,
        INVALID_DATE_ST,
        INVALID_DATETIME_ST,
        INVALID_TIME_ST,
        UNPARSABLE
    }
}

