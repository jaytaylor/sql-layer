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

package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.InvalidParameterValueException;
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
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import java.util.Calendar;
import java.util.EnumMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public class StrToDateExpression extends AbstractBinaryExpression
{
    @Scalar("str_to_date")
    public static final ExpressionComposer COMPOSER = new BinaryComposer ()
    {
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new StrToDateExpression(first, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int size = argumentTypes.size();
            if (size != 2) throw new WrongExpressionArityException(2, size);
            argumentTypes.setType(0, AkType.VARCHAR);
            argumentTypes.setType(1, AkType.VARCHAR);

            return ExpressionTypes.DATETIME;
        }
    };

    @Override
    public String name()
    {
        return "STR_TO_DATE";
    }

    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        private  int topType;
        private EnumMap<DateTimeField, Long> valuesMap = new EnumMap<DateTimeField,Long>(DateTimeField.class);
        private boolean has24Hr;       
        
        public InnerEvaluation (AkType type, List<? extends ExpressionEvaluation> childrenEval)
        {
            super(childrenEval);
        }        

        @Override
        public ValueSource eval()
        {
            if (left().isNull() || right().isNull()) return NullValueSource.only();
            
            ObjectExtractor<String> extractor = Extractors.getStringExtractor();
            long l = getValue(extractor.getObject(left()), extractor.getObject(right()));

            if (l < 0)
                return NullValueSource.only();
            else 
            {
                valueHolder().putDateTime(l);
                return valueHolder();
            }
        }

        private long getValue (String str, String format)
        { 
            if (str.trim().isEmpty() || format.trim().isEmpty())
            {
                QueryContext qc = queryContext();
                if (qc != null)
                    qc.warnClient(new InvalidParameterValueException("Empty string(s)"));
                return 0;
            }
            
            if (parseString(str, format)) return getLong();
            else return -1;
        }

        /**
         * parse the date string, and stores the values of each field (year, month, day, etc) in valuesMap
         * 
         * @param str
         * @param format
         * @return true if parsing was done successfully
         *         false otherwise
         */
        private boolean parseString (String str, String format)
        {
            // split format
            String formatList[] = format.split("\\%");

            // remove [matched] leading string
            str = str.replaceFirst(formatList[0], "");

            // trim unnecessary spaces in str.
            str = str.trim();
            String sVal = "";
            DateTimeField field = null;
            topType = 0;
            has24Hr = false;
            try
            {
                for (int n = 1; n < formatList.length - 1; ++n)
                {
                    String fName = formatList[n].charAt(0) + "";
                    field = DateTimeField.valueOf(fName);
                    has24Hr |= field.equals(DateTimeField.H) || field.equals(DateTimeField.T);
                    topType |= field.getFieldType();
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
                field = DateTimeField.valueOf(formatList[formatList.length - 1].charAt(0) + "");
                topType |= field.getFieldType();
                if (valuesMap.containsKey(field.equivalentField())) return false;
                valuesMap.put(field.equivalentField(), field.get(str)[0]);
            }
            catch (IllegalStateException iexc) // str and format do not match
            {
                QueryContext context = queryContext();
                if (context != null)
                    context.warnClient(new InvalidParameterValueException("Date String and Format do not match"));
                return false;
            }
            catch (IllegalArgumentException nex) // format specifier not found, or str contains bad input (NumberFormatException)
            {
                QueryContext context = queryContext();
                if (context != null)
                    context.warnClient(new InvalidParameterValueException("Invalid Input value " + nex.getLocalizedMessage()));
                return false;
            }
            catch (ArrayIndexOutOfBoundsException oexc) // str does not contains enough info specified by format
            {
                QueryContext context = queryContext();
                if (context != null)
                    context.warnClient(new InvalidParameterValueException("Date String and Format do not match"));
                return false;
            }
            catch (ArithmeticException aexc) // trying to put 0hr to a 12hr format
            {
                QueryContext context = queryContext();
                 if (context != null)
                    context.warnClient(new InvalidParameterValueException("Cannot specified 0 hour in a 12h-format string"));
                return false;
            }
            return true; // parse sucessfully
        }
        
        private long getLong ()
        {
            switch (topType) 
            {
                case 1:  return findDate() * 1000000L; // datetime without time
                case 2:  return findTime(); // datetime without date
                default:    return findDateTime(); // full datetime

            }
        }
        
        private long findDate ()
        {
             Long y = 0L;
             Long m = 0L;
             Long d = 0L;
             
              // year
              if ((y = valuesMap.get(DateTimeField.Y.equivalentField())) == null) y = 0L;

              // month
              if ((m = valuesMap.get(DateTimeField.m.equivalentField())) == null) m = 0L;
                    
              // day
              if ((d = valuesMap.get(DateTimeField.d.equivalentField())) == null) d = 0L;
              
              // get date specified by day of year, year, if year, month or day is not available
              if ( m * d == 0 && y != 0)
              {
                  Long dayOfYear = valuesMap.get(DateTimeField.j);
                  if (dayOfYear != null && dayOfYear.intValue() >= 0)
                  {
                      MutableDateTime date = new MutableDateTime(DateTimeZone.getDefault());
                      date.setYear(y.intValue());
                      date.setDayOfYear(dayOfYear.intValue());

                      y = (long)date.getYear();
                      m = (long)date.getMonthOfYear();
                      d = (long)date.getDayOfMonth();
                  }
              }
              
              // get date specified by week,year and weekday if year, month or day field is still not available
              if ( y*m*d == 0)
              {
                  Long yr = valuesMap.get(DateTimeField.x);
                  Long wk;
                  Long dWeek;
                  Calendar cal = Calendar.getInstance();
                 
                  if (yr == null)
                  {
                      if ((yr = valuesMap.get(DateTimeField.X)) == null || (wk = valuesMap.get(DateTimeField.V)) == null
                              || (dWeek = valuesMap.get(DateTimeField.W)) == null)
                          return validYMD(y, m, d) ? y * 10000L + m * 100 + d : -1;
                       cal.setMinimalDaysInFirstWeek(7);
                       cal.setFirstDayOfWeek(Calendar.SUNDAY);
                   }
                  else
                  {
                      if ((wk = valuesMap.get(DateTimeField.v)) == null
                              || (dWeek = valuesMap.get(DateTimeField.W)) == null)
                          return validYMD(y, m, d) ? y * 10000L + m * 100 + d: -1;
                      cal.setMinimalDaysInFirstWeek(4);
                      cal.setFirstDayOfWeek(Calendar.MONDAY); 
                  }
                  cal.set(Calendar.YEAR, yr.intValue()); 
                  cal.set(Calendar.WEEK_OF_YEAR, wk.intValue() ); // week in calendar start at 1
                  cal.set(Calendar.DAY_OF_WEEK, dWeek.intValue() +1);

                  y = (long) cal.get(Calendar.YEAR);
                  m = (long) cal.get(Calendar.MONTH) +1; // month in Calendar is 0-based
                  d = (long) cal.get(Calendar.DAY_OF_MONTH);
              }
              return validYMD(y, m, d) ? y * 10000L + m * 100 + d : -1;
        }
       
        private long findTime ()
        {
            Long hr = 0L;
            Long min = 0L;
            Long sec = 0L;
            
            Long t = valuesMap.get(DateTimeField.T);
            Long ap = valuesMap.get(DateTimeField.p);
            if (ap != null && (ap < 0L || has24Hr)) return -1;
                    
            if (t != null)
            {
                hr = t / 10000L + (ap != null ? ap : 0L);
                min = t / 100 % 100;
                sec = t % 100;
                return validHMS(hr,min,sec) ? hr * 10000L + min * 100L + sec : -1;
             }

            // hour
            if ((hr = valuesMap.get(DateTimeField.h.equivalentField())) == null) hr = 0L;
            if (ap != null && hr > 0L) hr += ap;

            // minute
            if ((min = valuesMap.get(DateTimeField.i.equivalentField())) == null) min = 0L;
            
            // second
            if ((sec = valuesMap.get(DateTimeField.s.equivalentField())) == null) sec = 0L;

            // TODO: millis sec (???)

            return validHMS(hr,min,sec) ? hr * 10000L + min * 100L + sec : -1;
        }
        
        private long findDateTime ()
        {            
            long date = findDate();  
            if (date < 0) return -1;
            
            long time = findTime();
            if (time < 0) return -1;
            
            return date * 1000000L + time;       
        }             

        private static boolean validYMD(long y, long m, long d)
        {
            if (y < 0 || m < 0 || d < 0) return false;
            switch ((int) m)
            {
                case 0:     return d <= 31;
                case 2:     return d <= (y % 400 == 0 || y % 4 == 0 && y != 100? 29L : 28L);
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
        super(AkType.DATETIME,l, r);
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("STR_TO_DATE");
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        if (valueType() == AkType.NULL) return LiteralExpression.forNull().evaluation();
        return new InnerEvaluation(valueType(),childrenEvaluations());
    }
}

