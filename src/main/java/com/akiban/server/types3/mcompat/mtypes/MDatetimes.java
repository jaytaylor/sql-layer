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

package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TParsers;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.common.types.NoAttrTClass;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.mcompat.mcasts.CastUtils;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import java.text.DateFormatSymbols;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.akiban.sql.types.TypeId;
import com.akiban.util.AkibanAppender;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalFieldValueException;
import org.joda.time.MutableDateTime;

public class MDatetimes
{
    private static final TBundleID MBundleID = MBundle.INSTANCE.id();
    
    public static final NoAttrTClass DATE = new NoAttrTClass(MBundleID,
            "date", AkCategory.DATE_TIME, FORMAT.DATE, 1, 1, 4, PUnderlying.INT_32, TParsers.DATE, TypeId.DATE_ID);
    public static final NoAttrTClass DATETIME = new NoAttrTClass(MBundleID,
            "datetime", AkCategory.DATE_TIME, FORMAT.DATETIME,  1, 1, 8, PUnderlying.INT_64, TParsers.DATETIME, TypeId.DATETIME_ID);
    public static final NoAttrTClass TIME = new NoAttrTClass(MBundleID,
            "time", AkCategory.DATE_TIME, FORMAT.TIME, 1, 1, 4, PUnderlying.INT_32, TParsers.TIME, TypeId.TIME_ID);
    public static final NoAttrTClass YEAR = new NoAttrTClass(MBundleID,
            "year", AkCategory.DATE_TIME, FORMAT.YEAR, 1, 1, 1, PUnderlying.INT_8, TParsers.YEAR, TypeId.YEAR_ID);
    public static final NoAttrTClass TIMESTAMP = new NoAttrTClass(MBundleID,
            "timestamp", AkCategory.DATE_TIME, FORMAT.TIMESTAMP, 1, 1, 4, PUnderlying.INT_32, TParsers.TIMESTAMP, TypeId.TIMESTAMP_ID);

    public static final List<String> SUPPORTED_LOCALES = new LinkedList<String>();
    
    public static final Map<String, String[]> MONTHS;
    public static final Map<String, String[]> SHORT_MONTHS;
    
    public static final Map<String, String[]> WEEKDAYS;
    public static final Map<String, String[]> SHORT_WEEKDAYS;

    
    public static enum FORMAT implements TClassFormatter {
        DATE {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(dateToString(source.getInt32()));
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append("DATE '");
                out.append(dateToString(source.getInt32()));
                out.append("'");
            }
        }, 
        DATETIME {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(datetimeToString(source.getInt64()));
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append("TIMESTAMP '");
                out.append(datetimeToString(source.getInt64()));
                out.append("'");
            }
        }, 
        TIME {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(timeToString(source.getInt32()));
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append("TIME '");
                out.append(timeToString(source.getInt32()));
                out.append("'");
            }
        }, 
        YEAR {         
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                int year = source.getInt8();
                int yearAbs = Math.abs(year);
                if (yearAbs < 70)
                    year += 2000;
                else if (yearAbs < 100)
                    year += 1900;
                out.append(year);
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                format(instance, source, out);
            }
        }, 
        TIMESTAMP {      
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(timestampToString(source.getInt32(), null));
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append("TIMESTAMP '");
                out.append(datetimeToString(source.getInt64()));
                out.append("'");
            }
        }
    }
    
    static
    {
        // TODO: add all supported LOCALES here
        SUPPORTED_LOCALES.add(Locale.ENGLISH.getLanguage());
        
       Map<String, String[]> months = new HashMap<String, String[]>();
       Map<String, String[]> shortMonths = new HashMap<String, String[]>();
       Map<String, String[]>weekDays = new HashMap<String, String[]>();
       Map<String, String[]>shortWeekdays = new HashMap<String, String[]>();

       for (String locale : SUPPORTED_LOCALES)
       {
           DateFormatSymbols fm = new DateFormatSymbols(new Locale(locale));
           
           months.put(locale, fm.getMonths());
           shortMonths.put(locale, fm.getShortMonths());
           
           weekDays.put(locale, fm.getWeekdays());
           shortWeekdays.put(locale, fm.getShortWeekdays());
       }
       
       MONTHS = Collections.unmodifiableMap(months);
       SHORT_MONTHS = Collections.unmodifiableMap(shortMonths);
       WEEKDAYS = Collections.unmodifiableMap(weekDays);
       SHORT_WEEKDAYS = Collections.unmodifiableMap(shortWeekdays);
    }

    public static String getMonthName(int numericRep, String locale, TExecutionContext context)
    {
        return getVal(numericRep - 1, locale, context, MONTHS, "month", 11, 0);
    }
    
    public static String getShortMonthName(int numericRep, String locale, TExecutionContext context)
    {
        return getVal(numericRep -1, locale, context, SHORT_MONTHS, "month", 11, 0);
    }
    
    public static String getWeekDayName(int numericRep, String locale, TExecutionContext context)
    {
        return getVal(numericRep, locale, context, WEEKDAYS, "weekday", 6, 0);
    }
    
    public static String getShortWeekDayName(int numericRep, String locale, TExecutionContext context)
    {
        return getVal(numericRep, locale, context, SHORT_WEEKDAYS, "weekdays", 6, 0);
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
    
    public static long[] fromJodaDatetime (MutableDateTime date)
    {
        return new long[]
        {
            date.getYear(),
            date.getMonthOfYear(),
            date.getDayOfMonth(),
            date.getHourOfDay(),
            date.getMinuteOfHour(),
            date.getSecondOfMinute()
        };
    }
    
    public static long[] fromJodaDatetime (DateTime date)
    {
        return new long[]
        {
            date.getYear(),
            date.getMonthOfYear(),
            date.getDayOfMonth(),
            date.getHourOfDay(),
            date.getMinuteOfHour(),
            date.getSecondOfMinute()
        };
    }

    public static MutableDateTime toJodaDatetime(long ymd_hms[], String tz)
    {
        return new MutableDateTime((int)ymd_hms[YEAR_INDEX], (int)ymd_hms[MONTH_INDEX], (int)ymd_hms[DAY_INDEX],
                                   (int)ymd_hms[HOUR_INDEX], (int)ymd_hms[MIN_INDEX], (int)ymd_hms[SEC_INDEX], 0,
                                   DateTimeZone.forID(tz));
    }

    public static String dateToString (int date)
    {
        int yr = date / 512;
        int m = date / 32 % 16;
        int d = date % 32;
        
        return String.format("%04d-%02d-%02d", yr, m, d);
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

        if (tks.length != 3)
            throw new InvalidDateFormatException("date", st);
        
        try
        {
            return Integer.parseInt(tks[0]) * 512
                    + Integer.parseInt(tks[1]) * 32
                    + Integer.parseInt(CastUtils.truncateNonDigits(tks[2], context));
        }
        catch (NumberFormatException ex)
        {
            throw new InvalidDateFormatException("date", st);
        }
    }

    /**
     * TODO: This function is ised in CUR_DATE/TIME, could speed up the performance
     * by directly passing the Date(Time) object to this function
     * so it won't have to create one.
     * 
     * @param millis
     * @param tz
     * @return the (MySQL) encoded DATE value
     */
    public static int encodeDate(long millis, String tz)
    {
        DateTime dt = new DateTime(millis, DateTimeZone.forID(tz));
        
        return dt.getYear() * 512
                + dt.getMonthOfYear() * 32
                + dt.getDayOfMonth();
    }

    public static long[] decodeDate(long val)
    {
        return new long[]
        {
            val / 512,
            val / 32 % 16,
            val % 32,
            0,
            0,
            0
        };
    }
    
    public static int encodeDate (long ymd[])
    {
        return (int)(ymd[YEAR_INDEX] * 512 + ymd[MONTH_INDEX] * 32 + ymd[DAY_INDEX]);
    }
    
    public static long[] fromDate(long val)
    {
        return new long[]
        {
            val / DATE_YEAR,
            val / DATE_MONTH % DATE_MONTH,
            val % DATE_MONTH,
            0,
            0,
            0
        };
    }
    
    public static String datetimeToString(long datetime)
    {
        long dt[] = decodeDatetime(datetime);
        
        return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                             dt[YEAR_INDEX],
                             dt[MONTH_INDEX],
                             dt[DAY_INDEX],
                             dt[HOUR_INDEX],
                             dt[MIN_INDEX],
                             dt[SEC_INDEX]);
    }
    
    /**
     * parse the string for DATE, DATETIME or TIME and store the parsed values in ymd
     * @return a integer indicating the type that's been parsed:
     *      DATE_ST :       date string
     *      TIME_ST:        time string
     *      DATETIME_ST:    datetime string
     */
    public static StringType parseDateOrTime(String st, long ymd[])
    {        
        st = st.trim();

        Matcher datetime;
        Matcher time;
        Matcher timeNoday;
        
        String year = "0";
        String month = "0";
        String day = "0";
        String hour = "0";
        String minute = "0";
        String seconds = "0";
        
        datetime = DATE_PATTERN.matcher(st.trim());
        if (datetime.matches())
        {
            StringType ret = StringType.DATE_ST;
            year = datetime.group(DATE_YEAR_GROUP);
            month = datetime.group(DATE_MONTH_GROUP);
            day = datetime.group(DATE_DAY_GROUP);
            
            if (datetime.group(TIME_GROUP) != null)
            {
                ret = StringType.DATETIME_ST;
                hour = datetime.group(TIME_HOUR_GROUP);
                minute = datetime.group(TIME_MINUTE_GROUP);
                seconds = datetime.group(TIME_SECOND_GROUP);
            }
            
            doParse(st,
                    ymd,
                    year, month, day,
                    hour, minute, seconds);
            if (isValidDatetime(ymd))
                return ret;
        }
        else if ((time = TIME_WITH_DAY_PATTERN.matcher(st)).matches())
        {   
            day = time.group(MDatetimes.TIME_WITH_DAY_DAY_GROUP);
            hour = time.group(MDatetimes.TIME_WITH_DAY_HOUR_GROUP);
            minute = time.group(MDatetimes.TIME_WITH_DAY_MIN_GROUP);
            seconds = time.group(MDatetimes.TIME_WITH_DAY_SEC_GROUP);
            
            doParse(st,
                    ymd,
                    year, month, day,
                    hour, minute, seconds);
            
            if (MDatetimes.isValidHrMinSec(ymd, false))
            {
                // adjust DAY to HOUR 
                int sign = 1;
                if (ymd[DAY_INDEX] < 0)
                    ymd[DAY_INDEX] *= (sign = -1);
                ymd[HOUR_INDEX] = sign * (ymd[HOUR_INDEX] += ymd[DAY_INDEX] * 24);
                ymd[DAY_INDEX] = 0;
                return StringType.TIME_ST;
            }
        }
        else if ((timeNoday = TIME_WITHOUT_DAY_PATTERN.matcher(st)).matches())
        {
            hour = timeNoday.group(MDatetimes.TIME_WITHOUT_DAY_HOUR_GROUP);
            minute = timeNoday.group(MDatetimes.TIME_WITHOUT_DAY_MIN_GROUP);
            seconds = timeNoday.group(MDatetimes.TIME_WITHOUT_DAY_SEC_GROUP);
            
            doParse(st,
                    ymd,
                    year, month, day,
                    hour, minute, seconds);
            if (MDatetimes.isValidHrMinSec(ymd, false))
                return StringType.TIME_ST;
        }

        // anything else is an error
        throw new InvalidDateFormatException("datetime", st);
    }

    private static void doParse(String st, // for error message only
                                long ymd[],
                                String year, String month, String day,
                                String hour, String minute, String seconds)
    {
        try
        {
            ymd[YEAR_INDEX] = Long.parseLong(year);
            ymd[MONTH_INDEX] = Long.parseLong(month);
            ymd[DAY_INDEX] = Long.parseLong(day);
            ymd[HOUR_INDEX] = Long.parseLong(hour);
            ymd[MIN_INDEX] = Long.parseLong(minute);
            ymd[SEC_INDEX] = Long.parseLong(seconds);

        }
        catch (NumberFormatException ex)
        {
            throw new InvalidDateFormatException("datetime", st);
        }
    }

    public static long parseDatetime(String st)
    {
        Matcher m = DATE_PATTERN.matcher(st.trim());

            if (!m.matches() || m.group(DATE_GROUP) == null) 
            throw new InvalidDateFormatException("datetime", st);

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
            return Long.parseLong(year) * DATETIME_YEAR_SCALE
                    + Long.parseLong(month) * DATETIME_MONTH_SCALE
                    + Long.parseLong(day) * DATETIME_DAY_SCALE
                    + Long.parseLong(hour) * DATETIME_HOUR_SCALE
                    + Long.parseLong(minute) * DATETIME_MIN_SCALE
                    + Long.parseLong(seconds);
        }
        catch (NumberFormatException ex)
        {
            throw new InvalidDateFormatException("datetime", st);
        }
    }
    
    public static long encodeDatetime(DateTime dt)
    {
        return dt.getYear() * DATETIME_YEAR_SCALE
                + dt.getMonthOfYear() * DATETIME_MONTH_SCALE
                + dt.getDayOfMonth() * DATETIME_DAY_SCALE
                + dt.getHourOfDay() * DATETIME_HOUR_SCALE
                + dt.getMinuteOfHour() * DATETIME_MIN_SCALE
                + dt.getSecondOfMinute();
    }
    
    /**
     * TODO: Same as encodeDate(long, String)'s
     * 
     * @param millis number of millis second from UTC in the specified timezone
     * @param tz
     * @return the (MySQL) encoded DATETIME value
     */
    public static long encodeDatetime(long millis, String tz)
    {
        DateTime dt = new DateTime(millis, DateTimeZone.forID(tz));
        
        return dt.getYear() * DATETIME_YEAR_SCALE
                + dt.getMonthOfYear() * DATETIME_MONTH_SCALE
                + dt.getDayOfMonth() * DATETIME_DAY_SCALE
                + dt.getHourOfDay() * DATETIME_HOUR_SCALE
                + dt.getMinuteOfHour() * DATETIME_MIN_SCALE
                + dt.getSecondOfMinute();
    }
        
    public static long encodeDatetime(long ymdHMS[])
    {
        return ymdHMS[YEAR_INDEX] * DATETIME_YEAR_SCALE
                + ymdHMS[MONTH_INDEX] * DATETIME_MONTH_SCALE
                + ymdHMS[DAY_INDEX] * DATETIME_DAY_SCALE
                + ymdHMS[HOUR_INDEX] * DATETIME_HOUR_SCALE
                + ymdHMS[MIN_INDEX] * DATETIME_MIN_SCALE
                + ymdHMS[SEC_INDEX];
    }

    public static long[] decodeDatetime (long val)
    {
        if (val < 100000000)
            // this is a YYYY-MM-DD int -- need to pad it with 0's for HH-MM-SS
            val *= 1000000;
        return new long[]
        {
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
        int h  = (int)(val / DATETIME_HOUR_SCALE);
        int m = (int)(val / DATETIME_MIN_SCALE) % 100;
        int s = (int)val % 100;
        
        return String.format("%d:%02d:%02d", h, m, s);
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

        
        // hh:mm:ss
        if (string.length() > 8 )
        {
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
        }
        catch (NumberFormatException ex)
        {
            throw new InvalidDateFormatException("time", string);
        }
        
        minutes += seconds / 60;
        seconds %= 60;
        hours += minutes / 60;
        minutes %= 60;

        if (!isValidHrMinSec(hours, minutes, seconds, shortTime))
            throw new InvalidDateFormatException("time", string);
        
        long ret = mul * (hours* DATETIME_HOUR_SCALE + minutes* DATETIME_MIN_SCALE + seconds);
        
        return (int)CastUtils.getInRange(TIME_MAX, TIME_MIN, ret, context);
    }
    public static long[] decodeTime(long val)
    {
        return new long[]
        {
            1970,
            1,
            1,
            val / DATETIME_HOUR_SCALE,
            val / DATETIME_MIN_SCALE % 100,
            val % 100
        };
    }
    
    /**
     * TODO: same as encodeDate(long, String)'s
     * 
     * @param millis: number of millis second from UTC in the sepcified timezone
     * @param tz
     * @return the (MySQL) encoded TIME value
     */
    public static int encodeTime(long millis, String tz)
    {
        DateTime dt = new DateTime(millis, DateTimeZone.forID(tz));

        return (int)(dt.getHourOfDay() * DATETIME_HOUR_SCALE  
                        + dt.getMinuteOfHour() * DATETIME_HOUR_SCALE
                        + dt.getSecondOfMinute());
    }
    
    public static int encodeTime(long val[])
    {
        return (int)(val[HOUR_INDEX] * DATETIME_HOUR_SCALE
                    + val[MIN_INDEX] * DATETIME_MIN_SCALE
                    + val[SEC_INDEX]);
    }
    
    public static int encodeTime(long hr, long min, long sec, TExecutionContext context)
    {
        if (min < 0 || sec < 0)
            throw new InvalidParameterValueException("Invalid time value");
      
        int mul;
        
        if (hr < 0)
            hr *= mul = -1;
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
        catch (IllegalFieldValueException e)
        {
            return 0; // e.g. SELECT UNIX_TIMESTAMP('1920-21-01 00:00:00') -> 0
        }
        catch (NumberFormatException ex)
        {
            throw new InvalidDateFormatException("datetime", ts);
        }
    }

    public static long[] decodeTimestamp(long ts, String tz) 
    {
        DateTime dt = new DateTime(ts * 1000L, DateTimeZone.forID(tz));
        
        return new long[]
        {
            dt.getYear(),
            dt.getMonthOfYear(),
            dt.getDayOfMonth(),
            dt.getHourOfDay(),
            dt.getMinuteOfHour(),
            dt.getSecondOfMinute()
        }; // TODO: fractional seconds
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

    /**
     * @param val array encoding year, month, day, hour, min, sec
     * @param tz
     * @return a unix timestamp (w/o range-checking)
     */
    public static long getTimestamp(long val[], String tz)
    {
        return new DateTime((int)val[YEAR_INDEX], (int)val[MONTH_INDEX], (int)val[DAY_INDEX],
                            (int)val[HOUR_INDEX], (int)val[MONTH_INDEX], (int)val[DAY_INDEX], 0,
                            DateTimeZone.forID(tz)).getMillis() / 1000L;
    }

    public static String timestampToString(long ts, String tz)
    {
        long ymd[] = decodeTimestamp(ts, tz);
        
        return String.format("%04d-%02d-%02d %02d:%02d:%02d", 
                            ymd[YEAR_INDEX], ymd[MONTH_INDEX], ymd[DAY_INDEX],
                            ymd[HOUR_INDEX], ymd[MIN_INDEX], ymd[SEC_INDEX]);
    }

    public static boolean isValidDatetime (long ymdhms[])
    {
        return ymdhms != null && isValidDayMonth(ymdhms) && isValidHrMinSec(ymdhms, true);
    }
    
    public static boolean isValidHrMinSec (long ymdhms[], boolean shortTime)
    {
        return  (shortTime ? ymdhms[HOUR_INDEX] >= 0  : true) // if time portion is from a DATETIME, hour should be non-neg
                && (shortTime ? ymdhms[HOUR_INDEX] < 24 : true) // if time portion is from a DATETIME, hour should be less than 24
                && ymdhms[MIN_INDEX] >= 0 && ymdhms[MIN_INDEX] < 60 
                && ymdhms[SEC_INDEX] >= 0 && ymdhms[SEC_INDEX] < 60;
    }
 
    public static boolean isValidHrMinSec(int hr, int min, int sec, boolean shortTime)
    {
        return hr >= 0 
                && (shortTime ? hr < 24 : true) // if time portion is from a DATETIME, hour should be less than 24
                && min >= 0 && min < 60 
                && sec >= 0 && sec < 60;
    }
    public static boolean isValidDayMonth(int year, int month, int day)
    {
        if (month == 0)
            return false;
        long last = getLastDay(year, month);
        return last > 0 && day <= last;
    }

    public static boolean isValidDayMonth(long ymd[])
    {
        if (ymd[MONTH_INDEX] == 0 || ymd[DAY_INDEX] <= 0)
            return false;
        long last = getLastDay(ymd);
        return last > 0 && ymd[DAY_INDEX] <= last;
    }
        
    public static long getLastDay(int year, int month)
    {
        switch(month)
        {
            case 2:
                return year % 400 == 0 || year % 4 == 0 && year % 100 != 0 ? 29L : 28L;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30L;
            case 3:
            case 1:
            case 5:
            case 7:
            case 8:
            case 10:
            case 0:
            case 12:
                return 31L;
            default:
                return -1;
        }
    }

    public static long getLastDay(long ymd[])
    {
        switch ((int) ymd[1])
        {
            case 2:
                return ymd[0] % 400 == 0 || ymd[0] % 4 == 0 && ymd[0] % 100 != 0 ? 29L : 28L;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30L;
            case 3:
            case 1:
            case 5:
            case 7:
            case 8:
            case 10:
            case 0:
            case 12:
                return 31L;
            default:
                return -1;
        }
    }

    public static final int YEAR_INDEX = 0;
    public static final int MONTH_INDEX = 1;
    public static final int DAY_INDEX = 2;
    public static final int HOUR_INDEX = 3;
    public static final int MIN_INDEX = 4;
    public static final int SEC_INDEX = 5;
    
    private static final int DATE_YEAR = 10000;
    private static final int DATE_MONTH = 100;

    private static final long DATETIME_DATE_SCALE = 1000000L;
    private static final long DATETIME_YEAR_SCALE = 10000L * DATETIME_DATE_SCALE;
    private static final long DATETIME_MONTH_SCALE = 100L * DATETIME_DATE_SCALE;
    private static final long DATETIME_DAY_SCALE = 1L * DATETIME_DATE_SCALE;
    private static final long DATETIME_HOUR_SCALE = 10000L;
    private static final long DATETIME_MIN_SCALE = 100L;
    
    private static final int TIME_HOURS_SCALE = 10000;
    private static final int TIME_MINUTES_SCALE = 100;
    
    private static final int DATE_GROUP = 1;
    private static final int DATE_YEAR_GROUP = 2;
    private static final int DATE_MONTH_GROUP = 3;
    private static final int DATE_DAY_GROUP = 4;
    private static final int TIME_GROUP = 5;
    private static final int TIME_HOUR_GROUP = 6;
    private static final int TIME_MINUTE_GROUP = 7;
    private static final int TIME_SECOND_GROUP = 8;
    private static final int TIME_FRAC_GROUP = 9;
    private static final int TIME_TIMEZONE_GROUP = 10;
    private static final Pattern DATE_PATTERN 
            = Pattern.compile("^((\\d+)-(\\d+)-(\\d+))(\\s+(\\d+):(\\d+):(\\d+)(\\.\\d+)?([+-]\\d+:\\d+)?)?$");
    
    
    private static final int TIME_WITH_DAY_DAY_GROUP = 2;
    private static final int TIME_WITH_DAY_HOUR_GROUP = 3;
    private static final int TIME_WITH_DAY_MIN_GROUP = 4;
    private static final int TIME_WITH_DAY_SEC_GROUP = 5;
    private static final Pattern TIME_WITH_DAY_PATTERN
            = Pattern.compile("^(([-+]?\\d+)\\s+(\\d+):(\\d+):(\\d+)(\\.\\d+)?([+-]\\d+:\\d+)?)?$");

    private static final int TIME_WITHOUT_DAY_HOUR_GROUP = 2;
    private static final int TIME_WITHOUT_DAY_MIN_GROUP = 3;
    private static final int TIME_WITHOUT_DAY_SEC_GROUP = 4;
    private static final Pattern TIME_WITHOUT_DAY_PATTERN
            = Pattern.compile("^(([-+]?\\d+):(\\d+):(\\d+)(\\.\\d+)?([+-]\\d+:\\d+)?)?$");

    // upper and lower limit of TIMESTAMP value
    // as per http://dev.mysql.com/doc/refman/5.5/en/datetime.html
    public static final long TIMESTAMP_MIN = DateTime.parse("1970-01-01T00:00:01Z").getMillis();
    public static final long TIMESTAMP_MAX = DateTime.parse("2038-01-19T03:14:07Z").getMillis();
    public static final long TS_ERROR_VALUE = 0L;
    
    // upper and lower limti of TIME value
    // as per http://dev.mysql.com/doc/refman/5.5/en/time.html
    public static final int TIME_MAX = 8385959;
    public static final int TIME_MIN = -8385959;
    
    
    public static enum StringType
    {
        DATE_ST, DATETIME_ST, TIME_ST;
    }
}

