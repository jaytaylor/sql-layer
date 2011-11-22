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

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.util.ValueHolder;
import java.util.Calendar;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class StrToDateExpression extends AbstractBinaryExpression
{
    public static final ExpressionComposer COMPOSER = new BinaryComposer ()
    {
        @Override
        protected Expression compose(Expression first, Expression second)
        {
            return new StrToDateExpression(first, second);
        }

        @Override
        protected ExpressionType composeType(ExpressionType first, ExpressionType second)
        {
            //TODO: str_to_date  return type coudl be a DATE, TIME or DATETIME depending on the format specifiers
            // which can only be known at evaluation time....
            // thus this method returns a LONG, since LONG could be casted to all of the types mentioned above (???)
            if (first.getType() == AkType.NULL || second.getType() == AkType.NULL ) return ExpressionTypes.NULL;
            else return ExpressionTypes.LONG;
        }

        @Override
        public void argumentTypes(List<AkType> argumentTypes)
        {
            int size = argumentTypes.size();
            if (size != 2)  throw new WrongExpressionArityException(2, size);

            for (int n = 0; n < size; ++n)
                argumentTypes.set(n, AkType.VARCHAR);
        }

    };

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

            @Override
            public Field equivalentField ()
            {
                return Field.W;
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

            @Override
            public Field equivalentField ()
            {
                return Field.m;
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

            @Override
            public Field equivalentField ()
            {
                return Field.m;
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
                throw new UnsupportedOperationException(" day with suffix is not supported yet");
            }

             @Override
            public int getFieldType ()
            {
                 return 1;
            }

            @Override
            public Field equivalentField ()
            {
                return Field.d;
            }
        },
        
        /**
         * day of month in numeric: 31, 30, 29, ...
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

            @Override
            public Field equivalentField ()
            {
                return Field.d;
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

            @Override
            public Field equivalentField ()
            {
                return Field.d;
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
            public int getFieldType ()
            {
                 return 2;
            }

            @Override
            public Field equivalentField ()
            {
                return Field.f;
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

            @Override
            public Field equivalentField ()
            {
                return Field.H;
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
                int i = 2 <= str.length() ? 2 : 1;
                // adjust hour to 24hr format
                long h = Long.parseLong(str.substring(0, i));
                if (h > 12) throw new NumberFormatException();
                h %= 12;
                return new long [] {h, i};
            }

            @Override
            public int getFieldType ()
            {
                 return 2;
            }

            @Override
            public Field equivalentField ()
            {
                return Field.H;
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

            @Override
            public Field equivalentField ()
            {
                return Field.H;
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

            @Override
            public Field equivalentField ()
            {
                return Field.i;
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

            @Override
            public Field equivalentField ()
            {
                return Field.j;
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

             @Override
            public Field equivalentField ()
            {
                return Field.m;
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

            @Override
            public Field equivalentField ()
            {
                return Field.m;
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
                String ap = str.substring(0, 2);
                return new long[] {ap.equalsIgnoreCase("am") ? 0 : ap.equalsIgnoreCase("pm") ? 12 : -1
                    , 2};
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
            }

            @Override
            public Field equivalentField ()
            {
                return Field.p;
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
                String t[] = time.split("\\:");

                // adjust time to 24hr format
                long hr = Long.parseLong(t[0]);
                if (hr > 12) throw new NumberFormatException();
                hr %= 12;
                long m = Long.parseLong(t[1]);
                long s = Long.parseLong(t[2]);
                return new long [] {10000L * hr + 100 * m + s, 8};
            }

             @Override
            public int getFieldType ()
            {
                 return 2;
            }

            @Override
            public Field equivalentField ()
            {
                return Field.T;
            }
        },
   
        /**
         * second, same as s
         */
        S 
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

            @Override
            public Field equivalentField ()
            {
                return Field.s;
            }
        },
        
        /**
         * second,
         */
        s 
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

            @Override
            public Field equivalentField ()
            {
                return Field.s;
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
                String time = str.substring(0, 8);
                String t[] = time.split("\\:");
                return new long [] {10000L * Long.parseLong(t[0]) + 100 * Long.parseLong(t[1]) + Long.parseLong(t[2]), 8};
            }

            @Override
            public int getFieldType ()
            {
                 return 2;
            }

            @Override
            public Field equivalentField ()
            {
                return Field.T;
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

            @Override
            public Field equivalentField ()
            {
                return Field.U;
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

            @Override
            public Field equivalentField ()
            {
                return Field.u;
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

            @Override
            public Field equivalentField ()
            {
                return Field.V;
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

            @Override
            public Field equivalentField ()
            {
                return Field.v;
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

            @Override
            public Field equivalentField ()
            {
                return Field.W;
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

            @Override
            public Field equivalentField ()
            {
                return Field.W;
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
            public int getFieldType ()
            {
                 return 1;
            }

            @Override
            public Field equivalentField ()
            {
                return Field.X;
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
            public int getFieldType ()
            {
                 return 1;
            }

            @Override
            public Field equivalentField ()
            {
                return Field.x;
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

            @Override
            public Field equivalentField ()
            {
                return Field.Y;
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

            @Override
            public Field equivalentField ()
            {
                return Field.Y;
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

            @Override
            public Field equivalentField ()
            {
                return Field.percent;
            }
        };

        /**
         * parse str to get value for this field.
         * return an array of long where array[0] is the parsed value, and array[1] is n where
         * str.substring(0,n) contains the parsed value. This is used to remove parsed field from str.
         */
        abstract long [] get(String str);

        /**
         *
         * @return 1 if the field specifies date-related info (i.e., year, month, day, date, week, etc..)
         *         2 if the field specifies time-related info (i.e., hour, minute, second,)
         */
        abstract int getFieldType ();

        /**
         * 
         * @return the equivalent field (that's ultimately used to map values to)
         * For example, %b, %m, and %M are all referred to as %m because the all mean day of month
         *              %y and %Y  => %Y year
         */
        abstract Field equivalentField ();

        // class static data fields
        static protected HashMap<String, Integer> abbWeekday = new HashMap<String, Integer>();
        static protected HashMap<String, Integer> abbMonth = new HashMap<String, Integer>();
        static protected HashMap<String, Integer> month = new HashMap<String, Integer>();
        static
        {
            abbWeekday.put("SUN", 0);
            abbWeekday.put("MON", 1);
            abbWeekday.put("TUE", 2);
            abbWeekday.put("WED", 3);
            abbWeekday.put("THU", 4);
            abbWeekday.put("FRI", 5);
            abbWeekday.put("SAT", 6);

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
        private boolean has24Hr;
        public InnerEvaluation (AkType type, List<? extends ExpressionEvaluation> childrenEval)
        {
            super(childrenEval);
            topType = type;
        }

        @Override
        public ValueSource eval()
        {
            if (left().isNull() || right().isNull()) return NullValueSource.only();
            
            ObjectExtractor<String> extractor = Extractors.getStringExtractor();
            long l = getValue(extractor.getObject(left()), extractor.getObject(right()));

            if (l < 0) return NullValueSource.only();
            else return new ValueHolder(topType, l);
        }

        private long getValue (String str, String format)
        {
            if (parseString(str, format)) return getLong();
            else return -1;
        }
        
        private boolean parseString (String str, String format)
        {
            // trim unnecessary spaces in str.
            str = str.trim();           
            // split format
            String formatList[] = format.split("\\%");
            String sVal = "";
            Field field = null;
            has24Hr = false;
            try
            {
                for (int n = 1; n < formatList.length - 1; ++n)
                {
                    String fName = formatList[n].charAt(0) + "";
                    field = Field.valueOf(fName);
                    has24Hr |= field.equals(Field.H) || field.equals(Field.T);
                    String del = formatList[n].substring(1);
                    if (del.length() == 0) // no delimeter
                    {
                        long[] num = field.get(str);
                        if (valuesMap.containsKey(field.equivalentField())) return false; // duplicate field
                        valuesMap.put(field.equivalentField(), num[0]);
                        sVal = str.substring(0, (int) num[1]);
                        str = str.replaceFirst(sVal, "");
                        continue;
                    }

                    if (del.matches("^\\s*")) del = " "; // delimeter is only space(s)
                    else del = del.trim(); // delimeter is non-space, then trim all leading/trailing spaces

                    Matcher m = Pattern.compile("^.*?(?=" + del + ")").matcher(str);
                    m.find();
                    sVal = m.group();
                    if (valuesMap.containsKey(field.equivalentField())) return false;
                    valuesMap.put(field.equivalentField(), field.get(sVal)[0]);
                    str = str.replaceFirst(sVal + del, "");
                    str = str.trim();
                }
                field = Field.valueOf(formatList[formatList.length - 1]);
                if (valuesMap.containsKey(field.equivalentField())) return false;
                valuesMap.put(field.equivalentField(), field.get(str)[0]);
            }
            catch (IllegalStateException iexc) // str and format do not match
            {
                return false;
            }
            catch (NullPointerException nex) // format specifier not found
            {
                return false;
            }
            catch (NumberFormatException nbEx) // str contains bad input, ie. str_to_date("33-abc-2009", "%d-%m-%Y")
            {
                return false;
            }
            catch (ArrayIndexOutOfBoundsException oexc) // str does not contains enough info specified by format
            {
                return false;
            }
            catch (ArithmeticException aexc) // trying to put 0hr to a 12hr format
            {
                return false;
            }
            return true; // parse sucessfully
        }
        
        private long getLong ()
        {
            switch (topType) 
            {
                case DATE:  return findDate();
                case TIME:  return findTime();
                default:    return findDateTime();

            }
        }
        
        private long findDate ()
        {
             Long y = 0L;
             Long m = 0L;
             Long d = 0L;
             
              // year
              if ((y = valuesMap.get(Field.Y.equivalentField())) == null) y = 0L;

              // month
              if ((m = valuesMap.get(Field.m.equivalentField())) == null) m = 0L;
                    
              // day
              if ((d = valuesMap.get(Field.d.equivalentField())) == null) d = 0L;
              
              // get date specified by week,year and weekday if year, month or day field is not available
              if ( y*m*d == 0)
              {
                  Long yr = valuesMap.get(Field.x);
                  Long wk;
                  Long dWeek;
                  Calendar cal = Calendar.getInstance();
                 
                  if (yr == null)
                  {
                      if ((yr = valuesMap.get(Field.X)) == null || (wk = valuesMap.get(Field.V)) == null
                              || (dWeek = valuesMap.get(Field.W)) == null)
                          return validYMD(y, m, d) ? y * 512 + m * 32 + d : -1;
                       cal.setMinimalDaysInFirstWeek(7);
                       cal.setFirstDayOfWeek(Calendar.SUNDAY);
                   }
                  else
                  {
                      if ((wk = valuesMap.get(Field.v)) == null
                              || (dWeek = valuesMap.get(Field.W)) == null)
                          return validYMD(y, m, d) ? y * 512 + m * 32 + d : -1;
                      cal.setMinimalDaysInFirstWeek(1);
                      cal.setFirstDayOfWeek(Calendar.MONDAY); 
                  }
                  cal.set(Calendar.YEAR, yr.intValue()); 
                  cal.set(Calendar.WEEK_OF_YEAR, wk.intValue() ); // week in calendar start at 1
                  cal.set(Calendar.DAY_OF_WEEK, dWeek.intValue() +1);

                  y = (long) cal.get(Calendar.YEAR);
                  m = (long) cal.get(Calendar.MONTH) +1; // month in Calendar is 0-based
                  d = (long) cal.get(Calendar.DAY_OF_MONTH);
              }
              return validYMD(y, m, d) ? y * 512 + m * 32 + d : -1;
        }
       
        private long findTime ()
        {
            Long hr = 0L;
            Long min = 0L;
            Long sec = 0L;
            
            Long t = valuesMap.get(Field.T);
            Long ap = valuesMap.get(Field.p);
            if (ap != null && (ap < 0L || has24Hr)) return -1;
                    
            if (t != null)
            {
                hr = t / 10000L + (ap != null ? ap : 0L);
                min = t / 100 % 100;
                sec = t % 100;
                return validHMS(hr,min,sec) ? hr * 10000L + min * 100L + sec : -1;
             }

            // hour
            if ((hr = valuesMap.get(Field.h.equivalentField())) == null) hr = 0L;
            if (ap != null && hr > 0L) hr += ap;

            // minute
            if ((min = valuesMap.get(Field.i.equivalentField())) == null) min = 0L;
            // second
            if ((sec = valuesMap.get(Field.s.equivalentField())) == null) sec = 0L;

            // TODO: millis sec (???)

            return validHMS(hr,min,sec) ? hr * 10000L + min * 100L + sec : -1;

        }
        
        private long findDateTime ()
        {
            Long y = 0L;
            Long m = 0L;
            Long d = 0L;
            
            Long hr = 0L;
            Long min = 0L;
            Long sec = 0L;
             
            // year
            if ((y = valuesMap.get(Field.Y.equivalentField())) == null) y = 0L;
            
            // month
            if ((m = valuesMap.get(Field.m.equivalentField())) == null) m = 0L;
            
            // day
            if ((d = valuesMap.get(Field.d.equivalentField())) == null) d = 0L;

            // get date specified by week,year and weekday if year, month or day field is not available
            if (y * m * d == 0) 
            {
                Long yr = valuesMap.get(Field.x);
                Long wk = 1L;
                Long dWeek = 1L;
                Calendar cal = Calendar.getInstance();
               
                if (yr == null)
                {
                    if ((yr = valuesMap.get(Field.X)) != null 
                            && (wk = valuesMap.get(Field.V)) != null
                            && (dWeek = valuesMap.get(Field.W)) != null)
                    {
                        cal.setMinimalDaysInFirstWeek(7);
                        cal.setFirstDayOfWeek(Calendar.SUNDAY);
                        cal.set(Calendar.YEAR, yr.intValue());
                        cal.set(Calendar.WEEK_OF_YEAR, wk.intValue());
                        cal.set(Calendar.DAY_OF_WEEK, dWeek.intValue() + 1);
                        y = (long) cal.get(Calendar.YEAR);
                        m = (long) cal.get(Calendar.MONTH) + 1;
                        d = (long) cal.get(Calendar.DAY_OF_MONTH);
                    }
   
                } 
                else 
                {
                    if ((wk = valuesMap.get(Field.v)) != null
                            && (dWeek = valuesMap.get(Field.W)) != null) 
                    {
                        cal.setMinimalDaysInFirstWeek(1);
                        cal.setFirstDayOfWeek(Calendar.MONDAY);
                        cal.set(Calendar.YEAR, yr.intValue());
                        cal.set(Calendar.WEEK_OF_YEAR, wk.intValue());
                        cal.set(Calendar.DAY_OF_WEEK, dWeek.intValue() + 1);
                        y = (long) cal.get(Calendar.YEAR);
                        m = (long) cal.get(Calendar.MONTH) + 1;
                        d = (long) cal.get(Calendar.DAY_OF_MONTH);
                    }
                }
            }

            if (!validYMD(y, m, d)) return -1;

            // --------------- hh:mm:ss
            Long ti = valuesMap.get(Field.T);
            Long am = valuesMap.get(Field.p);
               if (am != null && (am < 0L || has24Hr)) return -1;

               if (ti != null)
               {
                   hr = ti / 10000L + (am != null ? am : 0L);
                   min = ti / 100 % 100;
                   sec = ti % 100;
                   return validHMS(hr, min, sec) ?y * 10000000000L + m * 100000000L + d * 1000000L + hr * 10000L + min * 100 + sec : -1;
               }

                    
               // hour
               if ((hr = valuesMap.get(Field.h.equivalentField())) == null) hr = 0L;
               if (am != null && hr > 0L) hr = hr + am;
               
               // minute
               if ((min = valuesMap.get(Field.i.equivalentField())) == null) min = 0L;
                 
               // second 
               if ((sec = valuesMap.get(Field.s.equivalentField())) == null) sec = 0L;
               
               // TODO: millis sec
                       
               // yyyyMMddHHmmss
               return validHMS(hr, min, sec) ? y * 10000000000L + m * 100000000L + d * 1000000L + hr * 10000L + min * 100 + sec : -1;
        }
        
     

        private static boolean validYMD(long y, long m, long d)
        {
            if (y < 0 || m < 0 || d < 0) return false;
            switch ((int) m)
            {
                case 0:     return d <= 31;
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

        private static boolean validHMS (long h, long m, long sec)
        {
            return !(h < 0 || h > 23 || m < 0 || m > 59 || sec < 0 || sec > 59);
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
        try
        {
            for (int n = 1; n < formatList.length; ++n)
                bit |= Field.valueOf(formatList[n].charAt(0) + "").getFieldType();
            switch(bit)
            {
                case 1:     return AkType.DATE;
                case 2:     return AkType.TIME;
                default:    return AkType.DATETIME;
            }
        }
        catch (IllegalArgumentException  ex)
        {
            return AkType.NULL;
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
