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
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.TBundleID;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.mcompat.MParsers;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.mcompat.mcasts.CastUtils;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import java.text.DateFormatSymbols;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalFieldValueException;
import org.joda.time.MutableDateTime;
import org.joda.time.base.AbstractDateTime;
import org.joda.time.base.BaseDateTime;

public class MDatetimes
{
    private static final TBundleID MBundleID = MBundle.INSTANCE.id();

    // TODO: The serialization size of these old Type instances saved a byte.  Can
    // remove this if we are willing to consume that extra byte and render existing
    // volumes unreadable. (Changing the serializationSize field to 3 would
    // incompatibly change cost estimates.)
    static class DTMediumInt extends NoAttrTClass {
        public DTMediumInt(TBundleID bundle, String name, Enum<?> category, TClassFormatter formatter, int internalRepVersion, int serializationVersion, int serializationSize, UnderlyingType underlyingType, com.foundationdb.server.types.TParser parser, int defaultVarcharLen, TypeId typeId) {
            super(bundle, name, category, formatter, internalRepVersion, serializationVersion, serializationSize, underlyingType, parser, defaultVarcharLen, typeId);
        }

        @Override
        public int fixedSerializationSize(TInstance type) {
            assert (4 == super.fixedSerializationSize(type));
            return 3;
        }
    }

    public static final NoAttrTClass DATE = new DTMediumInt(MBundleID,
            "date", AkCategory.DATE_TIME, FORMAT.DATE, 1, 1, 4, UnderlyingType.INT_32, MParsers.DATE, 10, TypeId.DATE_ID)
    {
        public TClass widestComparable()
        {
            return DATETIME;
        }
    };
    public static final NoAttrTClass DATETIME = new NoAttrTClass(MBundleID,
            "datetime", AkCategory.DATE_TIME, FORMAT.DATETIME,  1, 1, 8, UnderlyingType.INT_64, MParsers.DATETIME, 19, TypeId.DATETIME_ID);
    public static final NoAttrTClass TIME = new DTMediumInt(MBundleID,
            "time", AkCategory.DATE_TIME, FORMAT.TIME, 1, 1, 4, UnderlyingType.INT_32, MParsers.TIME, 8, TypeId.TIME_ID);
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
                out.append(datetimeToString(source.getInt64()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                out.append("TIMESTAMP '");
                out.append(datetimeToString(source.getInt64()));
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
        public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out) {
            out.append('"');
            format(type, source, out);
            out.append('"');
        }
    }


    public static String getMonthName(int numericRep, String locale, TExecutionContext context)
    {
        return getVal(numericRep - 1, locale, context, MONTHS, "month", 11, 0);
    }

    static String getVal (int numericRep, 
                          String locale,
                          TExecutionContext context,
                          Map<String, String[]> map,
                          String name,
                          int max, int min)
    {
        if (numericRep > max || numericRep < min)
        {
            context.reportBadValue(name + " out of range: " + numericRep);
            return null;
        }
        
        String ret[] = map.get(locale);
        if (ret == null)
        {
            context.reportBadValue("Unsupported locale: " + locale);
            return null;
        }
        
        return ret[numericRep];
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

    public static MutableDateTime toJodaDateTime(long[] ymdhms, String tz) {
        return toJodaDateTime(ymdhms, DateTimeZone.forID(tz));
    }

    public static MutableDateTime toJodaDateTime(long[] ymdhms, DateTimeZone dtz) {
        return new MutableDateTime((int)ymdhms[YEAR_INDEX],
                                   (int)ymdhms[MONTH_INDEX],
                                   (int)ymdhms[DAY_INDEX],
                                   (int)ymdhms[HOUR_INDEX],
                                   (int)ymdhms[MIN_INDEX],
                                   (int)ymdhms[SEC_INDEX],
                                   0,
                                   dtz);
    }

    public static String dateToString(int date) {
        long[] ymd = decodeDate(date);
        return String.format("%04d-%02d-%02d", ymd[YEAR_INDEX], ymd[MONTH_INDEX], ymd[DAY_INDEX]);
    }

    public static int parseDate(String st, TExecutionContext context)
    {
        String tks[];
        
        // date and time tokens
        String datetime[] = st.split(" ");
        if (datetime.length == 2)
            tks = datetime[0].split("-"); // ignore the time part
        else
            tks = st.split("-");
        
        try
        {
            int year, month, day;
            if (tks.length == 3)
            {
                year = Integer.parseInt(tks[0]);
                month = Integer.parseInt(tks[1]);
                day = (int) CastUtils.parseInRange(tks[2], Long.MAX_VALUE, Long.MIN_VALUE, context);
            }
            else if (tks.length == 1)
            {
                long[] ymd = parseDate(Long.parseLong(tks[0]));
                year = (int)ymd[YEAR_INDEX];
                month = (int)ymd[MONTH_INDEX];
                day = (int)ymd[DAY_INDEX];
            }
            else
                throw new InvalidDateFormatException("date", st);

            if (!isValidDayMonth(year, month, day))
                throw new InvalidDateFormatException("date", st);
            else
                return year * 512
                    + month * 32
                    + day;
        }
        catch (NumberFormatException ex)
        {
            throw new InvalidDateFormatException("date", st);
        }
    }

    /** Convert millis to a date in the given timezone and then {@link #encodeDate(long, long, long)}. */
    public static int encodeDate(long millis, String tz) {
        DateTime dt = new DateTime(millis, DateTimeZone.forID(tz));
        return encodeDate(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
    }

    /** Convenience for {@link #encodeDate(long, long, long)}. */
    public static int encodeDate(long[] ymd) {
        return encodeDate(ymd[YEAR_INDEX], ymd[MONTH_INDEX], ymd[DAY_INDEX]);
    }

    /** Encode the year, month and date in the MySQL internal DATE format. */
    public static int encodeDate(long y, long m, long d) {
        return (int)(y * 512 + m * 32 + d);
    }

    /** Decode the MySQL internal DATE format long into a ymdhms array. */
    public static long[] decodeDate(long val) {
        return new long[] { val / 512, val / 32 % 16, val % 32, 0, 0, 0 };
    }

    /** Parse an *external* date long (e.g. YYYYMMDD => 20130130) into a ymdhms array. */
    public static long[] parseDate(long val) {
        long[] ymdhms = parseDateTime(val);
        ymdhms[HOUR_INDEX] = ymdhms[MIN_INDEX] = ymdhms[SEC_INDEX] = 0;
        return ymdhms;
    }

    /** Parse an *external* datetime long (e.g. YYYYMMDDHHMMSS => 20130130) into a ymdhms array. */
    public static long[] parseDateTime(long val) {
        if((val != 0) && (val <= 100)) {
            throw new InvalidDateFormatException("date", Long.toString(val));
        }
        // Pad out HHMMSS if needed
        if(val < DATETIME_MONTH_SCALE) {
            val *= DATETIME_DATE_SCALE;
        }
        // External is same as internal, though a two-digit year may need converted
        long[] ymdhms = decodeDateTime(val);
        if(val != 0) {
            ymdhms[YEAR_INDEX] = adjustTwoDigitYear(ymdhms[YEAR_INDEX]);
        }
        if(!isValidDayMonth(ymdhms)) {
            throw new InvalidDateFormatException("date", Long.toString(val));
        }
        if(!isValidHrMinSec(ymdhms, true)) {
            throw new InvalidDateFormatException("datetime", Long.toString(val));
        }
        return ymdhms;
    }
    
    public static String datetimeToString(long internalDatetime) {
        long[] ymdhms = decodeDateTime(internalDatetime);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                             ymdhms[YEAR_INDEX],
                             ymdhms[MONTH_INDEX],
                             ymdhms[DAY_INDEX],
                             ymdhms[HOUR_INDEX],
                             ymdhms[MIN_INDEX],
                             ymdhms[SEC_INDEX]);
    }
    
    /**
     * Attempt to parse {@code str} into as DATE and/or TIME.
     * Return type indicates success, almost success (INVALID_*) or completely unparsable.
     */
    public static StringType parseDateOrTime(String st, long[] ymd) {
        assert ymd.length >= MAX_INDEX;

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
            if(stringsToLongs(ymd, true, year, month, day, hour, minute, seconds) && isValidDatetime(ymd)) {
                return type;
            }
            return (type == StringType.DATETIME_ST) ? StringType.INVALID_DATETIME_ST : StringType.INVALID_DATE_ST;
        }
        else if((matcher = TIME_WITH_DAY_PATTERN.matcher(st)).matches())
        {   
            day = matcher.group(MDatetimes.TIME_WITH_DAY_DAY_GROUP);
            hour = matcher.group(MDatetimes.TIME_WITH_DAY_HOUR_GROUP);
            minute = matcher.group(MDatetimes.TIME_WITH_DAY_MIN_GROUP);
            seconds = matcher.group(MDatetimes.TIME_WITH_DAY_SEC_GROUP);
            if(stringsToLongs(ymd, false, year, month, day, hour, minute, seconds) && isValidHrMinSec(ymd, false)) {
                // adjust DAY to HOUR 
                int sign = 1;
                if(ymd[DAY_INDEX] < 0) {
                    ymd[DAY_INDEX] *= (sign = -1);
                }
                ymd[HOUR_INDEX] = sign * (ymd[HOUR_INDEX] += ymd[DAY_INDEX] * 24);
                ymd[DAY_INDEX] = 0;
                return StringType.TIME_ST;
            }
            return StringType.INVALID_TIME_ST;
        }
        else if((matcher = TIME_WITHOUT_DAY_PATTERN.matcher(st)).matches())
        {
            hour = matcher.group(MDatetimes.TIME_WITHOUT_DAY_HOUR_GROUP);
            minute = matcher.group(MDatetimes.TIME_WITHOUT_DAY_MIN_GROUP);
            seconds = matcher.group(MDatetimes.TIME_WITHOUT_DAY_SEC_GROUP);
            if(stringsToLongs(ymd, false, year, month, day, hour, minute, seconds) && isValidHrMinSec(ymd, false)) {
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
                    if(stringsToLongs(ymd, true, dTok[0], dTok[1], dTok[2], tTok[0], tTok[1], tTok[2]) &&
                       isValidDatetime(ymd)) {
                        return StringType.DATETIME_ST;
                    }
                    return StringType.INVALID_DATETIME_ST;
                }
            } else if(parts.length == 1) {
                String[] dTok = parts[0].split(DELIM);
                if(dTok.length == 3) {
                    if(stringsToLongs(ymd, true, dTok[0], dTok[1], dTok[2]) && isValidDayMonth(ymd)) {
                        return StringType.DATE_ST;
                    }
                    return StringType.INVALID_DATE_ST;
                }
            }
        }
        return StringType.UNPARSABLE;
    }

    public static long parseAndEncodeDateTime(String input) {
        long[] ymdhms = new long[6];
        StringType type = parseDateOrTime(input, ymdhms);
        switch(type) {
            case DATE_ST:
            case DATETIME_ST:
                if(isValidDatetime(ymdhms)) {
                    return encodeDateTime(ymdhms);
                }
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
    public static long encodeDateTime(long[] ymdhms) {
        return encodeDateTime(ymdhms[YEAR_INDEX],
                              ymdhms[MONTH_INDEX],
                              ymdhms[DAY_INDEX],
                              ymdhms[HOUR_INDEX],
                              ymdhms[MIN_INDEX],
                              ymdhms[SEC_INDEX]);
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

    /** Decode the MySQL DATETIME internal format long into a ymdhms array. */
    public static long[] decodeDateTime(long val) {
        return new long[] {
            val / DATETIME_YEAR_SCALE,
            val / DATETIME_MONTH_SCALE % 100,
            val / DATETIME_DAY_SCALE % 100,
            val / DATETIME_HOUR_SCALE % 100,
            val / DATETIME_MIN_SCALE % 100,
            val % 100
        };
    }
    
    public static String timeToString(int val)
    {
        String sign = "";
        if (val < 0) {
            val = - val;
            sign = "-";
        }
        int h  = (int)(val / DATETIME_HOUR_SCALE);
        int m = (int)(val / DATETIME_MIN_SCALE) % 100;
        int s = (int)val % 100;

        return String.format("%s%d:%02d:%02d", sign, h, m, s);
    }

    public static void timeToDatetime(long time[])
    {
        time[YEAR_INDEX] = adjustTwoDigitYear(time[HOUR_INDEX]);
        time[MONTH_INDEX] = time[MIN_INDEX];
        time[DAY_INDEX] = time[SEC_INDEX];
        
        // erase the time portion
        time[HOUR_INDEX] = 0;
        time[MIN_INDEX] = 0;
        time[SEC_INDEX] = 0;
        
        return;
    }
    
    public static int parseTime (String string, TExecutionContext context)
    {
          // (-)HH:MM:SS
        int mul = 1;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int offset = 0;
        boolean shortTime = false;
        if (string.length() > 0 && string.charAt(0) == '-')
        {
            mul = -1;
            string = string.substring(1);
        }

        hhmmss:
        {
            if (string.length() > 8 )
            {
                Matcher timeNoday = TIME_WITHOUT_DAY_PATTERN.matcher(string);
                if (timeNoday.matches()) {
                    try {
                        hours = Integer.parseInt(timeNoday.group(MDatetimes.TIME_WITHOUT_DAY_HOUR_GROUP));
                        minutes = Integer.parseInt(timeNoday.group(MDatetimes.TIME_WITHOUT_DAY_MIN_GROUP));
                        seconds = Integer.parseInt(timeNoday.group(MDatetimes.TIME_WITHOUT_DAY_SEC_GROUP));
                        break hhmmss;
                    }
                    catch (NumberFormatException ex)
                    {
                        throw new InvalidDateFormatException("time", string);
                    }
                }

                String parts[] = string.split(" ");

                // just get the TIME part
                if (parts.length == 2)
                {
                    String datePts[] = parts[0].split("-");
                    try
                    {
                        switch (datePts.length)
                        {
                            case 1: // <some value> hh:mm:ss ==> make sure <some value> is a numeric value
                                hours = Integer.parseInt(datePts[0]) * 24;
                                break;
                            case 3: // YYYY-MM-dd hh:mm:ss
                                shortTime = true;
                                if (isValidDayMonth(Integer.parseInt(datePts[0]),
                                                    Integer.parseInt(datePts[1]),
                                                    Integer.parseInt(datePts[2])))
                                    break;
                                // fall thru
                            default:
                                throw new InvalidDateFormatException("time", string);
                        }
                    }
                    catch (NumberFormatException ex)
                    {
                        throw new InvalidDateFormatException("time", string);
                    }

                    string = parts[1];
                }
            }

            final String values[] = string.split(":");

            try
            {
                if (values.length == 1) 
                {
                    long[] hms = decodeTime(Long.parseLong(values[offset]));
                    hours += hms[HOUR_INDEX];
                    minutes = (int)hms[MIN_INDEX];
                    seconds = (int)hms[SEC_INDEX];
                }
                else 
                {
                    switch (values.length)
                    {
                    case 3:
                        hours += Integer.parseInt(values[offset++]); // fall
                    case 2:
                        minutes = Integer.parseInt(values[offset++]); // fall
                    case 1:
                        seconds = Integer.parseInt(values[offset]);
                        break;
                    default:
                        throw new InvalidDateFormatException("time", string);
                    }

                    minutes += seconds / 60;
                    seconds %= 60;
                    hours += minutes / 60;
                    minutes %= 60;
                }
            }
            catch (NumberFormatException ex)
            {
                throw new InvalidDateFormatException("time", string);
            }
        }

        if (!isValidHrMinSec(hours, minutes, seconds, shortTime))
            throw new InvalidDateFormatException("time", string);
        
        long ret = mul * (hours* DATETIME_HOUR_SCALE + minutes* DATETIME_MIN_SCALE + seconds);
        
        return (int)CastUtils.getInRange(TIME_MAX, TIME_MIN, ret, context);
    }

    public static long[] decodeTime(long val)
    {
        int sign;
        if (val < 0)
            val *= sign = -1;
        else
            sign = 1;
        
         long ret[] =  new long[]
         {
            1970,
            1,
            1,
            val / DATETIME_HOUR_SCALE,
            val / DATETIME_MIN_SCALE % 100,
            val % 100
         };
   
         if (sign < 0)
         {
             int ind = HOUR_INDEX;
             // find the first element that is NOT zero
             while (ret[ind] == 0 && ind < ret.length)
                ++ind; 

             // and place the sign on it!
            if (ind < ret.length)
                ret[ind] *= -1;
         }        
         return ret;
    }
    
    /**
     * TODO: same as encodeDate(long, String)'s
     * 
     * @param millis: number of millis second from UTC in the sepcified timezone
     * @param tz
     * @return the (MySQL) encoded TIME value
     */
    public static int encodeTime(long millis, String tz) {
        DateTime dt = new DateTime(millis, DateTimeZone.forID(tz));
        return dt.getHourOfDay() * TIMESTAMP_HOUR_SCALE +
               dt.getMinuteOfHour() * TIMESTAMP_MIN_SCALE +
               dt.getSecondOfMinute();
    }
    
    public static int encodeTime(long[] val)
    {
        int n = HOUR_INDEX;
        int sign = 1;
        
        while (n < val.length && val[n] >= 0)
            ++n;
        
        if (n < val.length)
            val[n] = val[n] * (sign = -1);

        
        return (int)(val[HOUR_INDEX] * TIMESTAMP_HOUR_SCALE
                    + val[MIN_INDEX] * TIMESTAMP_MIN_SCALE
                    + val[SEC_INDEX]) * sign;
    }
    
    public static int encodeTime(long hr, long min, long sec, TExecutionContext context)
    {
        if (min < 0 || sec < 0)
            throw new InvalidParameterValueException("Invalid time value");
      
        int mul;
        
        if (hr < 0)
            hr *= mul = -1;
        else if (min < 0)
            min *= mul = -1;
        else if (sec < 0)
            sec *= mul = -1;
        else
            mul = 1;
        
        long ret = mul * (hr * DATETIME_HOUR_SCALE + min * DATETIME_MIN_SCALE + sec);
        return (int)CastUtils.getInRange(TIME_MAX, TIME_MIN, ret, context);
    }

    public static int parseTimestamp (String ts, String tz, TExecutionContext context)
    {
        Matcher m = DATE_PATTERN.matcher(ts.trim());

            if (!m.matches() || m.group(DATE_GROUP) == null) 
            throw new InvalidDateFormatException("datetime", ts);

        String year = m.group(DATE_YEAR_GROUP);
        String month = m.group(DATE_MONTH_GROUP);
        String day = m.group(DATE_DAY_GROUP);
        String hour = "0";
        String minute = "0";
        String seconds = "0";

        if (m.group(TIME_GROUP) != null)
        {
            hour = m.group(TIME_HOUR_GROUP);
            minute = m.group(TIME_MINUTE_GROUP);
            seconds = m.group(TIME_SECOND_GROUP);
        }

        try
        {
            long millis = new DateTime(Integer.parseInt(year),
                                       Integer.parseInt(month),
                                       Integer.parseInt(day),
                                       Integer.parseInt(hour),
                                       Integer.parseInt(minute),
                                       Integer.parseInt(seconds),
                                       0,
                                       DateTimeZone.forID(tz)
                                      ).getMillis();
            return (int)CastUtils.getInRange(TIMESTAMP_MAX, TIMESTAMP_MIN, millis / 1000L, TS_ERROR_VALUE, context);
        }
        catch (IllegalFieldValueException | NumberFormatException e)
        {
            context.warnClient(new InvalidDateFormatException("timestamp", ts));
            return 0; // e.g. SELECT UNIX_TIMESTAMP('1920-21-01 00:00:00') -> 0
        }
    }

    public static long[] decodeTimestamp(long ts, String tz)  {
        DateTime dt = new DateTime(ts * 1000L, DateTimeZone.forID(tz));
        return new long[] {
            dt.getYear(),
            dt.getMonthOfYear(),
            dt.getDayOfMonth(),
            dt.getHourOfDay(),
            dt.getMinuteOfHour(),
            dt.getSecondOfMinute()
        };
    }

    public static int encodeTimestamp(long val[], String tz, TExecutionContext context)
    {
        DateTime dt = new DateTime((int)val[YEAR_INDEX], (int)val[MONTH_INDEX], (int)val[DAY_INDEX],
                                   (int)val[HOUR_INDEX], (int)val[MIN_INDEX], (int)val[SEC_INDEX], 0,
                                   DateTimeZone.forID(tz));
        
        return (int)CastUtils.getInRange(TIMESTAMP_MAX, TIMESTAMP_MIN, dt.getMillis() / 1000L, TS_ERROR_VALUE, context);
    }

    public static long encodeTimetamp(long millis, TExecutionContext context)
    {
        return CastUtils.getInRange(TIMESTAMP_MAX, TIMESTAMP_MIN, millis / 1000L, TS_ERROR_VALUE, context);
    }

    public static boolean isValidTimestamp(BaseDateTime dt) {
        long millis = dt.getMillis();
        return (millis >= TIMESTAMP_MIN) && (millis <= TIMESTAMP_MAX);
    }

    /**
     * @param val array encoding year, month, day, hour, min, sec
     * @param tz
     * @return a unix timestamp (w/o range-checking)
     */
    public static int getTimestamp(long val[], String tz)
    {
        return (int)(new DateTime((int)val[YEAR_INDEX], (int)val[MONTH_INDEX], (int)val[DAY_INDEX],
                            (int)val[HOUR_INDEX], (int)val[MIN_INDEX], (int)val[SEC_INDEX], 0,
                            DateTimeZone.forID(tz)).getMillis() / 1000L);
    }

    public static String timestampToString(long ts, String tz)
    {
        long ymd[] = decodeTimestamp(ts, tz);
        
        return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                             ymd[YEAR_INDEX],
                             ymd[MONTH_INDEX],
                             ymd[DAY_INDEX],
                             ymd[HOUR_INDEX],
                             ymd[MIN_INDEX],
                             ymd[SEC_INDEX]);
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

    public static boolean isValidDatetime (long ymdhms[])
    {
        return ymdhms != null && isValidDayMonth(ymdhms) && isValidHrMinSec(ymdhms, true);
    }
    
    public static boolean isValidHrMinSec (long hms[], boolean shortTime)
    {
        // if time is from a DATETIME
        if (shortTime)
        {
            return hms[HOUR_INDEX] >= 0 && hms[HOUR_INDEX] < 24
                    && hms[MIN_INDEX] >= 0 && hms[MIN_INDEX] < 60 
                    && hms[SEC_INDEX] >= 0 && hms[SEC_INDEX] < 60;
            
        }
        else // if TIME is NOT from a DATETIME
        {
            // hh:mm:ss
            // One (and only one) of these three parts can be negative
            // and that would be the first part that is non-zero
            //
            // For eg., -12:13:12, or 00:-13:12 or 00:00:-12
            //
            // This is enforced by the decodeTime method, but just check to be sure!
            assert hms[HOUR_INDEX] >= 0 && hms[MIN_INDEX] >= 0 && hms[SEC_INDEX] >= 0
                   || ((hms[HOUR_INDEX] < 0) ^ 
                       (hms[MIN_INDEX] < 0) ^ 
                       (hms[SEC_INDEX] < 0))
                    : "TIME value probably decoded incorrectly!";
            
            return hms[MIN_INDEX] < 60 && hms[SEC_INDEX] < 60;
        }
    }
 
    public static boolean isValidHrMinSec(int hr, int min, int sec, boolean shortTime)
    {
        return hr >= 0 
                && (!shortTime || hr < 24) // if time portion is from a DATETIME, hour should be less than 24
                && min >= 0 && min < 60 
                && sec >= 0 && sec < 60;
    }
    
    public static boolean isZeroDayMonth(long ymd[])
    {
        return ymd[DAY_INDEX] == 0 || ymd[MONTH_INDEX] == 0;
    }

    public static boolean isValidDayMonth(int year, int month, int day)
    {
        long last = getLastDay(year, month);
        return last > 0 && day <= last;
    }

    public static boolean isValidDayMonth(long ymd[])
    {
        long last = getLastDay(ymd);
        return last > 0 && ymd[DAY_INDEX] <= last;
    }

    public static long getLastDay(long[] ymd) {
        return getLastDay((int)ymd[YEAR_INDEX], (int)ymd[MONTH_INDEX]);
    }

    public static long getLastDay(int year, int month)
    {
        switch(month) {
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
    private static boolean stringsToLongs(long[] ymdhms, boolean convertYear, String... parts) {
        assert parts.length <= ymdhms.length;
        for(int i = 0; i < parts.length; ++i) {
            try {
                ymdhms[i] = Long.parseLong(parts[i].trim());
                // Must be *exactly* two-digit to get converted
                if(convertYear && (i == YEAR_INDEX) && (parts[i].length() == 2)) {
                    ymdhms[i] = adjustTwoDigitYear(ymdhms[i]);
                }
            } catch(NumberFormatException e) {
                return false;
            }
        }
        return true;
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
    private static final long DATETIME_DAY_SCALE = 1L * DATETIME_DATE_SCALE;
    private static final long DATETIME_HOUR_SCALE = 10000L;
    private static final long DATETIME_MIN_SCALE = 100L;
    
    private static final int TIMESTAMP_HOUR_SCALE = 10000;
    private static final int TIMESTAMP_MIN_SCALE = 100;
    
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
            = Pattern.compile("^((\\d+)-(\\d+)-(\\d+))(([T]{1}|\\s+)(\\d+):(\\d+):(\\d+)(\\.\\d+)?)?[Z]?(\\s*[+-]\\d+:\\d+(:\\d+)?)?$");
    
    private static final int TIME_WITH_DAY_DAY_GROUP = 2;
    private static final int TIME_WITH_DAY_HOUR_GROUP = 3;
    private static final int TIME_WITH_DAY_MIN_GROUP = 4;
    private static final int TIME_WITH_DAY_SEC_GROUP = 5;
    private static final Pattern TIME_WITH_DAY_PATTERN
            = Pattern.compile("^(([-+]?\\d+)\\s+(\\d+):(\\d+):(\\d+)(\\.\\d+)?[Z]?(\\s*[+-]\\d+:\\d+(:\\d+)?)?)?$");

    private static final int TIME_WITHOUT_DAY_HOUR_GROUP = 2;
    private static final int TIME_WITHOUT_DAY_MIN_GROUP = 3;
    private static final int TIME_WITHOUT_DAY_SEC_GROUP = 4;
    private static final Pattern TIME_WITHOUT_DAY_PATTERN
            = Pattern.compile("^(([-+]?\\d+):(\\d+):(\\d+)(\\.\\d+)?[Z]?(\\s*[+-]\\d+:\\d+(:\\d+)?)?)?$");

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
    
    private static final EnumSet<StringType> validTypes = EnumSet.of(StringType.DATETIME_ST,
                                                                     StringType.DATE_ST,
                                                                     StringType.TIME_ST);
    public static boolean isValidType(StringType stType)
    {
        return validTypes.contains(stType);
    }
    
    public static enum StringType
    {
        DATE_ST, DATETIME_ST, TIME_ST,
        INVALID_DATE_ST, INVALID_DATETIME_ST, INVALID_TIME_ST,
        UNPARSABLE
    }
}

