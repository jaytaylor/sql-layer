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

package com.foundationdb.server.types.texpressions;

import java.text.DateFormatSymbols;
import java.util.HashMap;
import java.util.Locale;
import org.joda.time.DateTimeFieldType;
import org.joda.time.MutableDateTime;


/**
 * Specifiers for <b>str_to_date</b> and <b>date_format</b>
 * See http://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html
 */
public enum DateTimeField
{
    /**
     * abbreviated weekday name: Sun, Sat, Fri, ...
     */
    a
    {
        @Override
        public long [] get(String str)
        {
            return new long[] {abbWeekday.get(str.substring(0, 3).toUpperCase()), 3};
        }

       @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.dayOfWeek().getAsShortText();
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.W;
        }

    },

    /**
     * abbreviated month name: Dec, Nov, Oct, ...
     */
   b
    {
        @Override
        public long [] get(String str)
        {
            return new long [] {abbMonth.get(str.substring(0, 3).toUpperCase()), 3};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.monthOfYear().getAsShortText();
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.m;
        }
    },

    /**
     * month in numeric: 12, 11, 10, ...
     */
    c
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.getMonthOfYear() + "";
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.m;
        }
    },

    /**
     * day of month with suffix: 31st, 30th, 29th, ...
     */
    D
    {
        @Override
        public long [] get(String str)
        {
            int n = 0;
            int limit = 4 < str.length() ? 4 : str.length(); 
            for (n = 0; n < limit && str.charAt(n) >= '0' && str.charAt(n) <= '9'; ++n )
                ; // looking for the first char that is NOT a digit
            return new long[] { Long.parseLong(str.substring(0, n)), n +2 };

        }

        @Override
        public String get(MutableDateTime datetime)
        {
            int d = datetime.getDayOfMonth();

            switch(d%10)
            {
                case 1:  return d + (d == 11 ? "th" :"st");
                case 2:  return d + (d == 12? "th" : "nd");
                case 3:  return d + (d == 13 ? "th" : "rd");
                default: return d + "th";
            }
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.d;
        }
    },

    /**
     * day of month in numeric: 31, 30, 29, ..., 00
     */
    d
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return String.format("%02d", datetime.getDayOfMonth());
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.d;
        }
    },

    /**
     * same as d: day in numeric
     * [0, ..., 59]
     */
    e
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.getDayOfMonth() + "";
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.d;
        }
    },

    /**
     * micro seconds
     */
    f
    {
        @Override
        public long [] get(String str)
        {
            int n = 0;
            while (n < str.length() && Character.isDigit(str.charAt(n))) ++n;
            return new long[] {month.get(str.substring(0, n).toUpperCase()),n};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return "0"; // only second was supplied, => micro = 0
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.f;
        }
    },

    /**
     * hour in 24-hr format
     * [23, 22, ..., 01, 00]
     */
    H
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return String.format("%02d", datetime.getHourOfDay());
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.H;
        }
    },

    /**
     * hour in 12-hr format
     * [12, 11, ...,01]
     */
    h
    {
        @Override
        public long [] get(String str)
        {
            int i = 2 <= str.length() ? 2 : 1;
            // adjust hour to 24hr format
            long h = Long.parseLong(str.substring(0, i));
            if (h > 12) throw new NumberFormatException();
            h %= 12;
            return new long [] {h, i};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return String.format("%02d", datetime.get(DateTimeFieldType.clockhourOfHalfday()));
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.H;
        }
    },

    /**
     * I same as h: hour in 12-hr format
     * [12, 11, ...,01]
     */
    I
    {
        @Override
        public long [] get(String str)
        {
            int i = 2 <= str.length() ? 2 : 1;
            // adjust hour to 24hr format
            long h = Long.parseLong(str.substring(0, i));
            if (h > 12) throw new NumberFormatException();
            h %= 12;
            return new long [] {h, i};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return String.format("%02d", datetime.get(DateTimeFieldType.clockhourOfHalfday()));
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.H;
        }
    },

    /**
     * i: minutes, numeric 00..59
     */
    i
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return String.format("%02d", datetime.getMinuteOfHour());
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.i;
        }
    },

    /**
     * day of year in numeric
     */
    j
    {
        @Override
        public long [] get(String str)
        {
            int i = 0;
            for (; i < 3 && i < str.length() && str.charAt(i) >= '0' && str.charAt(i) <= '9'; ++i)
                ; // looking for the first char that is not a digit
            return new long[] {Long.parseLong(str.substring(0,i)), i};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.getDayOfYear() + "";
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.j;
        }
    },

    /**
     * hour : 24-hr format.
     * Same as H, but do not pre-pend 0 to one-digit hour
     * [23, 22, ...1,0]
     */
    k
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.getHourOfDay() + "";
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.H;
        }
    },

    /**
     * hour: 12-hr format
     * Same as h, but does not pre-pend 0 to a one-digit value
     * [12, 11, ..., 1]
     */
    l
    {
        @Override
        public long [] get(String str)
        {
            int i = 2 <= str.length() ? 2 : 1;
            // adjust hour to 24hr format
            long h = Long.parseLong(str.substring(0, i));
            if (h > 12) throw new NumberFormatException();
            h %= 12;
            return new long [] {h, i};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.get(DateTimeFieldType.clockhourOfHalfday()) + "";
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.H;
        }
    },

    /**
     * month name: December, November, ...
     */
    M
    {
        @Override
        public long [] get(String str)
        {
            int n = 0;
            while (n < str.length() && !Character.isDigit(str.charAt(n))) ++n;
            return new long[] {month.get(str.substring(0, n).toUpperCase()),n};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.monthOfYear().getAsText();
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

         @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.m;
        }
    },

    /**
     * month in numeric: 12, 11, 10, ...
     */
    m
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return String.format("%02d", datetime.getMonthOfYear());
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.m;
        }
    },

    /**
     * specify pm or am
     * to be used with %r (12-hr format time) or %h (12-hr format hour)
     * return NULL if used with %T (24-hr format time) or %H (24-hr format hour)
     */
    p
    {
        @Override
        public long [] get (String str)
        {
            String ap = str.substring(0, 2);
            return new long[] {ap.equalsIgnoreCase("am") ? 0 : ap.equalsIgnoreCase("pm") ? 12 : -1
                , 2};
        }


        @Override
        public String get(MutableDateTime datetime)
        {
            int hr = datetime.getHourOfDay();
            if(hr < 12) return "AM";
            else return "PM";
        }
         @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.p;
        }
    },

    /**
     * Time hh:mm:ss 12hr format
     */
    r
    {
        @Override
        public long [] get(String str)
        {
            int i =  Math.min(8, str.length());
            String time = str.substring(0,i);
            String t[] = time.split("\\:");

            // adjust time to 24hr format
            long hr = Long.parseLong(t[0]);
            if (hr > 12) throw new NumberFormatException();
            hr %= 12;
            long m = Long.parseLong(t[1]);
            long s = Long.parseLong(t[2]);
            return new long [] {10000L * hr + 100 * m + s, i};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            int h = datetime.get(DateTimeFieldType.clockhourOfHalfday());
            int m = datetime.getMinuteOfHour();
            int s = datetime.getSecondOfMinute();
            String halfDay = datetime.get(DateTimeFieldType.halfdayOfDay())  == 0 ? "AM" : "PM";
            
            return String.format("%02d:%02d:%02d %s" ,
                    h,m,s, halfDay);
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.T;
        }
    },

    /**
     * second, same as s
     *  [00, ..., 59]
     */
    S
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return String.format("%02d", datetime.getSecondOfMinute());
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.s;
        }
    },

    /**
     * second
     * [00, ..., 59]
     */
    s
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return String.format("%02d", datetime.getSecondOfMinute());
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.s;
        }
    },

    /**
     * Time: hh:mm:ss in 24hr format.
     */
    T
    {
        @Override
        public long [] get(String str)
        {
            int i = Math.min(8, str.length());
            String time = str.substring(0, i);
            String t[] = time.split("\\:");
            return new long [] {10000L * Long.parseLong(t[0]) + 100 * Long.parseLong(t[1]) + Long.parseLong(t[2]), i};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            int h = datetime.getHourOfDay();
            int m = datetime.getMinuteOfHour();
            int s = datetime.getSecondOfMinute();

            return String.format("%02d:%02d:%02d", h,m,s);
        }

        @Override
        public int getFieldType ()
        {
             return 2;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.T;
        }
    },

    /**
     * week of year [0,...53], Sunday is the first day of week
     */
    U
    {
        @Override
        public long [] get(String str)
        {
            // TO DO: not sure how this actually gets used
            throw new UnsupportedOperationException("%U is not supported in str_to_date");
            /*
            int i;
            return new long [] { Long.parseLong(str.substring(0,i = 2 <= str.length() ? 2 :1)), i};
             *
             */
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            // last parameter: 7 means SUNDAY
            return getWeek(datetime, datetime.getYear(), datetime.getMonthOfYear(), datetime.getDayOfMonth(), 7, true) + "";
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.U;
        }
    },

    /**
     * week of year [0...53], Monday is the first day of week
     */
    u
    {
        @Override
        public long [] get(String str)
        {
            // TO DO: not sure how this actually gets used
            throw new UnsupportedOperationException("%u is not supported in str_to_date");
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            // last parameter: 1 means MONDAY
            return getWeek(datetime, datetime.getYear(), datetime.getMonthOfYear(), datetime.getDayOfMonth(), 1, true) + "";
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.u;
        }
    },

    /**
     * week of year: [1,..,53]: where Sunday is the first day
     * to be used with %X
     */
    V
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return getWeek(datetime, datetime.getYear(), datetime.getMonthOfYear(), datetime.getDayOfMonth(), 7, false) + "";

        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.V;
        }
    },

    /**
     * week of year [1,..,53]: where Monday is the first day
     * to be used with %x
     */
    v
    {
        @Override
        public long [] get(String str)
        {
            return parseLeadingChars(str);
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            // the last parameter: 1 means MONDAY
             return getWeek(datetime, datetime.getYear(), datetime.getMonthOfYear(), datetime.getDayOfMonth(), 1, false) + "";
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.v;
        }
    },

    /**
     * week day name: Sunday, Saturday, ...
     */
    W
    {
        @Override
        public long [] get(String str)
        {
            int n = 0;
            while (n < str.length() && !Character.isDigit(str.charAt(n))) ++n;
            return new long[] {weekDay.get(str.substring(0, n).toUpperCase()),n};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.dayOfWeek().getAsText();
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.W;
        }
    },

    /**
     * week day in numeric: Sunday = 0, .... Saturday = 6
     */
    w
    {
        @Override
        public long [] get(String str)
        {
            return new long[] { Long.parseLong(str.charAt(0) + ""), 1};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.getDayOfWeek() % 7 + "";
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.W;
        }
    },

    /**
     * year for the week, to be used with %V: Sunday is the first day
     */
    X
    {
        @Override
        public long [] get(String str)
        {
            int i = 4 % str.length();
            i = (i == 0 ? 4 :i);
            return new long[] { Long.parseLong(str.substring(0, i)),i };
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            // Last parameter: 7 means SUNDAY
            return getYear(datetime, datetime.getYear(), datetime.getMonthOfYear(), datetime.getDayOfMonth(), 7) + "";
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.X;
        }
    },

    /**
     * year for the week, to be used with %v: Monday is the first day
     */
    x
    {
        @Override
        public long [] get(String str)
        {
            int i = 4 % str.length();
            i = (i == 0 ? 4 :i);
            return new long[] { Long.parseLong(str.substring(0, i)),i };
        }

        @Override
        public String get(MutableDateTime datetime)
        {
             // the last parameter: 1 means MONDAY
             return getYear(datetime, datetime.getYear(), datetime.getMonthOfYear(), datetime.getDayOfMonth(), 1) + "";

        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.x;
        }
    },

    /**
     * year : 2 digits, relative to 2000
     */
    y
    {
        @Override
        public long [] get(String str)
        {
            int i;
            return new long[] {2000 + Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2: 1)), i};
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return String.format("%02d", datetime.getYear() % 100);
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.Y;
        }
    },

    /**
     *  year: 4 digits
     */
    Y
    {
        @Override
        public long [] get(String str)
        {
            int i = 4 % str.length();
            i = (i == 0 ? 4 :i);
            return new long[] { Long.parseLong(str.substring(0, i)),i };
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return datetime.getYear() + "";
        }

        @Override
        public int getFieldType ()
        {
             return 1;
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.Y;
        }
    },

    /**
     * literal %
     */
    percent
    {
        @Override
        public long [] get(String str)
        {
            throw new UnsupportedOperationException("literal % is not supported in str_to_date");
        }

        @Override
        public int getFieldType ()
        {
             throw new UnsupportedOperationException("literal % is not supported for this method");
        }

        @Override
        public String get(MutableDateTime datetime)
        {
            return "%";
        }

        @Override
        public DateTimeField equivalentField ()
        {
            return DateTimeField.percent;
        }
    };



    /**
     * parse str to get value for this DateTimeField.
     * return an array of long where array[0] is the parsed value, and array[1] is n where
     * str.substring(0,n) contains the parsed value. This is used to remove parsed DateTimeField from str.
     */
    abstract long [] get(String str);

    /**
     *
     * @param datetime: given a datetime object
     * @return the value of this field as a string
     */
    abstract String get(MutableDateTime datetime);

    /**
     *
     * @return 1 if the DateTimeField specifies date-related info (i.e., year, month, day, date, week, etc..)
     *         2 if the DateTimeField specifies time-related info (i.e., hour, minute, second,)
     */
    abstract int getFieldType ();

    /**
     *
     * @return the equivalent DateTimeField (that's ultimately used to map values to)
     * For example, %b, %m, and %M are all referred to as %m because the all mean day of month
     *              %y and %Y  => %Y year
     */
    abstract DateTimeField equivalentField ();
    
    
   /**
    * 
    * @param str
    * 
    * - Looks at the first two chars of the string and try to parse that substring
    *      into a LONG
    * - But if the length is only 1, then it only looks at the first char
    * @return  the parsed value along with the length of the substring that was parsed
    *      The index is passed back to the calling function to cut-off the substring
    *      that has been passed
    */ 
    private static long [] parseLeadingChars(String str)
    {
         int i = 2 <= str.length() ? 2 :1;
         return new long[] { Long.parseLong(str.substring(0, i )), i};
    }

    /**
     * to be used in X and x
     * @param cal
     * @param yr
     * @param mo
     * @param da
     * @param firstDay: the first day of week
     * @return the year for this week, could be the same as yr or different
     *        , depending on the first day of year
     */
    private static int getYear(MutableDateTime cal, int yr, int mo, int da, int firstDay)
    {
        if (mo > 1 || da >7 ) return yr;

        cal.setYear(yr);
        cal.setMonthOfYear(1);
        cal.setDayOfMonth(1);
        int firstD = 1;

        while (cal.getDayOfWeek() != firstDay)
            cal.setDayOfMonth(++firstD);

        // reset cal
        cal.setYear(yr);
        cal.setMonthOfYear(mo);
        cal.setDayOfMonth(da);
        
        if (da < firstD) return yr -1;
        else return yr;
    }

    /**
     * to be used in V, v, U and u
     * @param cal
     * @param yr
     * @param mo
     * @param da
     * @param firstDay
     * @param lowestIs0: whether the lowest value could be zero or not
     * @return the week for this date, if the lowest value is not supposed to be zero, then it returns that
     *          the number of the last week in the previous year
     */
    private static int getWeek(MutableDateTime cal, int yr, int mo, int da, int firstDay, boolean lowestIs0)
    {
        cal.setYear(yr);
        cal.setMonthOfYear(1);
        cal.setDayOfMonth(1);
        int firstD = 1;

        while (cal.getDayOfWeek() != firstDay)
            cal.setDayOfMonth(++firstD);

        cal.setYear(yr);
        cal.setMonthOfYear(mo);
        cal.setDayOfMonth(da);

        int dayOfYear = cal.getDayOfYear();
        int result;
        
        if (dayOfYear < firstD) result =  (lowestIs0 ? 0 : getWeek(cal, yr-1, 12, 31, firstDay, lowestIs0));
        else result = (dayOfYear - firstD) / 7 +1;
        
        // reset cal
        cal.setYear(yr);
        cal.setMonthOfYear(mo);
        cal.setDayOfMonth(da);
        return result;
    }

    /**
     * Takes a (Mutable)DateTime object and a mysql format string
     *
     * @param date
     * @param format
     * @return a string formatted accordingly
     */
    public static String getFormatted (MutableDateTime date, String format)
    {
        String[] frmList = format.split("\\%");
        StringBuilder builder = new StringBuilder(frmList[0]);

        for (int n = 1; n < frmList.length; ++n)
        {
            if (frmList[n].length() == 0 )
            {
                builder.append('%');
                if ( n+1 < frmList.length && frmList[n+1].length() == 0)
                    ++n;
            }
            else
            {
                try
                {
                    String s = Character.toString(frmList[n].charAt(0));
                    builder.append(frmList[n].replaceFirst(s, DateTimeField.valueOf(s).get(date)));
                }
                catch (IllegalArgumentException ex) // unknown specifiers are treated as regular chars
                {
                    builder.append(frmList[n]);
                }
            }
        }

        for (int m = format.length() -1; format.charAt(m) == '%'; m -= 2)
            builder.append('%');
        return builder.toString();
    }

    // class static data DateTimeFields
    static protected final HashMap<String, Integer> abbWeekday = new HashMap<>();
    static protected final HashMap<String, Integer> weekDay = new HashMap<>();
    static protected final HashMap<String, Integer> abbMonth = new HashMap<>();
    static protected final HashMap<String, Integer> month = new HashMap<>();
    static
    {
        DateFormatSymbols fm = new DateFormatSymbols(new Locale(System.getProperty("user.language")));
        String mon [] = fm.getMonths(), shortMon[] = fm.getShortMonths(),
               wk[] = fm.getWeekdays(), shortWk[] = fm.getShortWeekdays();

        for (int n = 0; n < 12; ++n)
        {
            month.put(mon[n].toUpperCase(), n+1);
            abbMonth.put(shortMon[n].toUpperCase(), n+1);
        }
        
        for (int n = 1; n < 8; ++n)
        {
            weekDay.put(wk[n].toUpperCase(), n-1);
            abbWeekday.put(shortWk[n].toUpperCase(), n-1);
        }
    }
}
