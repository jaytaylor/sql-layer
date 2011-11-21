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


package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.util.ValueHolder;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StrToDateExpression extends AbstractBinaryExpression
{
    /**
     * Specifiers for str_to_date
     * See http://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html
     */    
    protected static enum Field
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
            public int getFieldType ()
            {
                 return 1;
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
            public int getFieldType ()
            {
                 return 1;
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
                int i;
                return new long[] { Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2: 1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
            }
        },
        
        /**
         * day with suffix: 31st, 30th, 29th, ...
         */
        D 
        {
            @Override
            public long [] get(String str)
            {
                throw new UnsupportedOperationException(" day with suffix is not supported yet");
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
            }
        },
        
        /**
         * day in numeric: 31, 30, 29, ...
         */
        d 
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long[] {Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2: 1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
            }
        },
        
        /**
         * same as d: day in numeric
         */
        e 
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long [] { Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2: 1))};
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
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
                throw new UnsupportedOperationException("micro seconds is not supported yet");
            }
             @Override
            public int getFieldType ()
            {
                 return 2;
            }
        },
        
        /**
         * hour in 24-hr format
         */
        H 
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long[] { Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2 :1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
            }
        },
        
        /**
         * hour in 12-hr format
         */
        h 
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long [] {Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2 : 1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
            }
        },
        
        /**
         * I same as h: hour in 12-hr format
         */
        I 
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long [] { (Long.parseLong(str.substring(0,i = 2 <= str.length() ? 2 : 1)) + 12) % 24, i};
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
            }
        },
        
        /**
         * minute
         */
        i 
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long[] {Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2 :1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
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
                throw new UnsupportedOperationException ("day of year not supported yet");
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
            }
        },
        
        // TODO: k, l same as H, h
        
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
            public int getFieldType ()
            {
                 return 1;
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
                int i;
                return new long[] { Long.parseLong(str.substring(0,i = 2 <= str.length() ? 2 :1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
            }
        },
        
        /**
         * specify pm or am
         */        
        p 
        {
            @Override
            public long [] get (String str)
            {
                throw new UnsupportedOperationException(" am pm is not supported yet");
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
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
                String time = str.substring(0, 8);
                throw new UnsupportedOperationException(" r is not supported yet");
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
            }
        },
   
        /**
         * second
         */
        S // second, same as S
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long[] { Long.parseLong(str.substring(0,i = 2 <= str.length() ? 2 :1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
            }
        },
        
        /**
         * second, same as S
         */
        s // second, same as S
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long[] { Long.parseLong(str.substring(0,i = 2 <= str.length() ? 2 :1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
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
                throw new UnsupportedOperationException(" T is ot supported yet");
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
            }
        },
        
        /**
         * week of year, Sunday is the first day of week
         */
        U 
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long [] { Long.parseLong(str.substring(0,i = 2 <= str.length() ? 2 :1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
            }
        },
        
        /**
         * week of year, Monday is the first day of week
         */
        u 
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long [] { Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2 :1)), i};
            }

            @Override
            public int getFieldType ()
            {
                 return 1;
            }
        },
        
        /**
         * week: 0, ....53: where Sunday is the first day
         * to be used with %X
         */
        V // (0 ...53) where Sunday is the first day, use with X
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long[] { Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2:1)), i};
            }

            @Override
            public int getFieldType ()
            {
                 return 1;
            }
        },
        
        /**
         * week 0, ...53: where Monday is the first day
         * to be used with %x
         */
        v 
        {
            @Override
            public long [] get(String str)
            {
                int i;
                return new long [] { Long.parseLong(str.substring(0, i = 2 <= str.length() ? 2 :1)), i};
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
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
                throw new UnsupportedOperationException ("week day name is not supportd yet");
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
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
            public int getFieldType ()
            {
                 return 1;
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
                throw new UnsupportedOperationException ("year week is not supported yet");
            }

            @Override
            public int getFieldType ()
            {
                 return 1;
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
                throw new UnsupportedOperationException ("year week is not supported yet");
            }

            @Override
            public int getFieldType ()
            {
                 return 1;
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
            public int getFieldType ()
            {
                 return 1;
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
            public int getFieldType ()
            {
                 return 1;
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
                throw new UnsupportedOperationException("percent is not supported yet");
            }

            @Override
            public int getFieldType ()
            {
                 throw new UnsupportedOperationException("percent is not supported yet");
            }
        };
        
        abstract long [] get(String str);
        abstract int getFieldType ();

        static protected HashMap<String, Integer> abbWeekday = new HashMap<String, Integer>();
        static protected HashMap<String, Integer> abbMonth = new HashMap<String, Integer>();
        static protected HashMap<String, Integer> month = new HashMap<String, Integer>();
        static
        {
            abbWeekday.put("MON", 1);
            abbWeekday.put("TUE", 2);
            abbWeekday.put("WED", 3);
            abbWeekday.put("THU", 4);
            abbWeekday.put("FRI", 5);
            abbWeekday.put("SAT", 6);
            abbWeekday.put("SUN", 7);

            abbMonth.put("JAN", 1);
            abbMonth.put("FEB", 2);
            abbMonth.put("MAR", 3);
            abbMonth.put("APR", 4);
            abbMonth.put("MAY", 5);
            abbMonth.put("JUN", 6);
            abbMonth.put("JUL", 7);
            abbMonth.put("AUG", 8);
            abbMonth.put("SEP", 9);
            abbMonth.put("OCT", 10);
            abbMonth.put("NOV", 11);
            abbMonth.put("DEC", 12);

            month.put("JANUARY", 1);
            month.put("FEBRUARY", 2);
            month.put("MARCH", 3);
            month.put("APRIL", 4);
            month.put("MAY", 5);
            month.put("JUNE", 6);
            month.put("JULY", 7);
            month.put("AUGUST", 8);
            month.put("SEPTEMBER", 9);
            month.put("OCTOBER", 10);
            month.put("NOVEMBER", 11);
            month.put("DECEMBER", 12);

        }
    }

    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        private final AkType topType;
        private EnumMap<Field, Long> valuesMap = new EnumMap<Field,Long>(Field.class);
        public InnerEvaluation (AkType type, List<? extends ExpressionEvaluation> childrenEval)
        {
            super(childrenEval);
            topType = type;
        }

        @Override
        public ValueSource eval()
        {
            ObjectExtractor<String> extractor = Extractors.getStringExtractor();
            long l = getDate(extractor.getObject(left()), extractor.getObject(right()));

            if (l < 0) return NullValueSource.only();
            else return new ValueHolder(topType, l);
        }

        /**
         * 
         * @param str
         * @param format
         * @return match str with format to get date/time info, enconde to appropriate type and return long
         */
        private long getDate(String str, String format) 
        {
            // trim leading/trailing in str.
            str = str.trim();
           
            // split format
            String formatList[] = format.split("\\%");

            String sVal = "";
            Field field = null;
            try
            {
                for (int n = 0; n < formatList.length - 2; ++n)
                {
                    String fName = formatList[n + 1].charAt(0) + "";
                    field = Field.valueOf(fName);
                    String del = formatList[n + 1].substring(1);
                    if (del.length() == 0)
                    {
                        long[] num = field.get(str);
                        valuesMap.put(field, num[0]);
                        sVal = str.substring(0, (int) num[1]);
                        str = str.replaceFirst(sVal, "");
                        continue;
                    }

                    if (del.matches("^\\s*"))
                        del = " ";
                    else
                        del = del.trim();

                    Matcher m = Pattern.compile("^.*?(?=" + del + ")").matcher(str);
                    m.find();
                    sVal = m.group();
                    valuesMap.put(field, field.get(sVal)[0]);
                    str = str.replaceFirst(sVal + del, "");
                    str = str.trim();
                }
                field = Field.valueOf(formatList[formatList.length - 1]);
                valuesMap.put(field, field.get(str)[0]);

            }
            catch (IllegalStateException iexc) // str and format do not match
            {
                return -1;
            }
            catch (NullPointerException nex) // str does not contains enough info specified by format
            {
                return -1;
            }
            catch (NumberFormatException nbEx) // str contains bad input, ie. str_to_date("33-12-2009", "%d-%m-%Y")
            {
                return -1;
            }
            
            Long y = 0L;
            Long m = 0L;
            Long d = 0L;
            Long hr = 0L;
            Long min = 0L;
            Long sec = 0L;

            switch (topType)
            {
                // date
                case DATE:
                    if (valuesMap.containsKey(Field.y) && valuesMap.containsKey(Field.Y)
                            || valuesMap.containsKey(Field.m) && valuesMap.containsKey(Field.M)
                            || valuesMap.containsKey(Field.d) && valuesMap.containsKey(Field.D))
                        return -1;
                    // year
                    if ((y = valuesMap.get(Field.y)) == null)
                        y = valuesMap.get(Field.Y);
                    y = (y == null ? 0L : y);
                    // month
                    if ((m = valuesMap.get(Field.m)) == null)
                        m = valuesMap.get(Field.Y);
                    m = (m == null ? 0L : m);

                    if ((d = valuesMap.get(Field.d)) == null)
                        d = valuesMap.get(Field.D);
                    d = (d == null ? 0L : d);

                    // TODO: date specified by week,year and weekday

                    return validYMD(y, m, d) ? y * 512 + m * 32 + d * 16 : -1;

                case TIME:
                    if (valuesMap.containsKey(Field.H) && valuesMap.containsKey(Field.h))
                        return -1;
                    // hour
                    if ((hr = valuesMap.get(Field.H)) == null)
                        hr = valuesMap.get(Field.h);
                    hr = (hr == null ? 0L : hr);
                    // minute
                    if ((min = valuesMap.get(Field.i)) == null)
                        min = 0L;
                    // second
                    if ((sec = valuesMap.get(Field.s)) == null)
                        sec = 0L;

                    // TODO: millis sec

                    return hr * 10000L + min * 100L + sec;

                default:
                    if (valuesMap.containsKey(Field.y) && valuesMap.containsKey(Field.Y)
                            || valuesMap.containsKey(Field.m) && valuesMap.containsKey(Field.M)
                            || valuesMap.containsKey(Field.d) && valuesMap.containsKey(Field.D))
                        return -1;
                    // year
                    if ((y = valuesMap.get(Field.y)) == null)
                        y = valuesMap.get(Field.Y);
                    y = (y == null ? 0L : y);
                    // month
                    if ((m = valuesMap.get(Field.m)) == null)
                        m = valuesMap.get(Field.Y);
                    m = (m == null ? 0L : m);

                    if ((d = valuesMap.get(Field.d)) == null)
                        d = valuesMap.get(Field.D);
                    d = (d == null ? 0L : d);

                    // TODO: date specified by week,year and weekday

                    if (!validYMD(y, m, d))
                        return -1;

                    // --------------- hh:mm:ss
                    if (valuesMap.containsKey(Field.H) && valuesMap.containsKey(Field.h))
                        return -1;
                    // hour
                    if ((hr = valuesMap.get(Field.H)) == null)
                        hr = valuesMap.get(Field.h);
                    hr = (hr == null ? 0L : hr);
                    // minute
                    if ((min = valuesMap.get(Field.i)) == null)
                        min = 0L;
                    // second
                    if ((sec = valuesMap.get(Field.s)) == null)
                        sec = 0L;

                    // yyyyMMddHHmmss
                    return y * 10000000000L + m * 100000000L + d * 1000000L + hr * 10000L + min * 100 + sec;
            }
        }

        private static boolean validYMD(long y, long m, long d)
        {
            if (y < 0 || m < 0 || d < 0) return false;
            switch ((int) m)
            {
                case 2:     return d <= (y % 4 == 0 ? 29L : 28L);
                case 4:
                case 6:
                case 9:
                case 11:    return d <= 30;
                case 3:
                case 1:
                case 5:
                case 7:
                case 8:
                case 10:
                case 12:    return d <= 31;
                default:    return false;
            }
        }

    }
   
    public StrToDateExpression (Expression l, Expression r)
    {
        super(getTopType(l.evaluation(),r.evaluation()),l, r);
    }

    protected static AkType getTopType (ExpressionEvaluation strE, ExpressionEvaluation formatE)
    {
        ValueSource formatS = formatE.eval();
        if (strE.eval().isNull() || formatS.isNull()) return AkType.NULL;

        String format = Extractors.getStringExtractor().getObject(formatS);
        String formatList[] = format.split("\\%");
        int bit = 0;
        for (int n = 1; n < formatList.length; ++n)
            bit |= Field.valueOf(formatList[n].charAt(0) + "").getFieldType();
        switch(bit)
        {
            case 1:     return AkType.DATE;
            case 2:     return AkType.TIME;
            default:    return AkType.DATETIME;
        }
    }

    @Override
    protected boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("STR_TO_DATE()");
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        if (valueType() == AkType.NULL) return LiteralExpression.forNull().evaluation();
        return new InnerEvaluation(valueType(),childrenEvaluations());
    }
}
