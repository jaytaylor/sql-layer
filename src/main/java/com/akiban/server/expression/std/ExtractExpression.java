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
import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.util.ConversionUtil;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.StandardException;
import java.text.DateFormatSymbols;
import java.util.EnumSet;
import java.util.Locale;
import org.joda.time.IllegalFieldValueException;
import org.joda.time.MutableDateTime;

public class ExtractExpression extends AbstractUnaryExpression
{
    @Scalar ("dayofyear")
    public static final ExpressionComposer DAY_YEAR_COMPOSER = new InternalComposer(TargetExtractType.DAY_OF_YEAR);
    
    @Scalar ("date")
    public static final ExpressionComposer DATE_COMPOSER = new InternalComposer(TargetExtractType.DATE);

    @Scalar ("datetime")
    public static final ExpressionComposer DATETIME_COMPOSER = new InternalComposer(TargetExtractType.DATETIME);
    
    @Scalar ({"day", "dayofmonth"})
    public static final ExpressionComposer DAY_COMPOSER = new InternalComposer(TargetExtractType.DAY);
    
    /**
     * extract the HOUR from a DATETIME/TIME/TIMESTAMP expression.
     * (24-hr format)
     */
    @Scalar ({"hour", "hour_of_day"})
    public static final ExpressionComposer HOUR_COMPOSER = new InternalComposer(TargetExtractType.HOUR);

    @Scalar ("minute")
    public static final ExpressionComposer MINUTE_COMPOSER = new InternalComposer(TargetExtractType.MINUTE);

    @Scalar ("month")
    public static final ExpressionComposer MONTH_COMPOSER = new InternalComposer(TargetExtractType.MONTH);

    @Scalar ("second")
    public static final ExpressionComposer SECOND_COMPOSER = new InternalComposer(TargetExtractType.SECOND);

    @Scalar ("time")
    public static final ExpressionComposer TIME_COMPOSER = new InternalComposer(TargetExtractType.TIME);

    @Scalar ("timestamp")
    public static final ExpressionComposer TIMESTAMP_COMPOSER = new InternalComposer(TargetExtractType.TIMESTAMP);

    @Scalar ("year")
    public static final ExpressionComposer YEAR_COMPOSER = new InternalComposer(TargetExtractType.YEAR);

    @Scalar ("monthname")
    public static final ExpressionComposer MONTH_NAME_COMPOSER = new InternalComposer(TargetExtractType.MONTH_NAME);
    
    @Scalar ("last_day")
    public static final ExpressionComposer LAST_DAY_COMPOSER = new InternalComposer(TargetExtractType.LAST_DAY);
    
    @Scalar ("quarter")
    public static final ExpressionComposer QUARTER_COMPOSER = new InternalComposer (TargetExtractType.QUARTER);
    
    protected static enum TargetExtractType
    {
        DATE(AkType.DATE)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                                Extractors.getLongExtractor(type).getLong(source));
                LongExtractor extractor = Extractors.getLongExtractor(AkType.DATE);
                switch (type)
                {
                    case DATE:      return source.getDate();
                    case DATETIME:  return validDayMonth(ymd) ? extractor.getEncoded(ymd) : null;
                    case TIMESTAMP: return extractor.getEncoded(ymd);
                    default:        return null;
                }
            }
        },
             
        DATETIME(AkType.DATETIME)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                                Extractors.getLongExtractor(type).getLong(source));
                LongExtractor extractor = Extractors.getLongExtractor(AkType.DATETIME);
                
                switch (type)
                {
                    case DATE:      return validDayMonth(ymd) ? extractor.getEncoded(ymd) : null;         
                    case DATETIME:  return validDayMonth(ymd) ? source.getDateTime() : null;
                    case TIMESTAMP: return extractor.getEncoded(ymd);
                    default:        return null;
                }
            }
            
        },

        DAY_OF_YEAR (AkType.INT)
        {
            @Override
            public Long extract (ValueSource source)
            {
                MutableDateTime datetime = ConversionUtil.getDateTimeConverter().get(source);                
                return (long)datetime.getDayOfYear();
            }
        },

        DAY(AkType.INT)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                        Extractors.getLongExtractor(type).getLong(source));
                
                switch (type)
                {
                    case DATE:
                    case DATETIME:  
                    case TIMESTAMP: return validDayMonth(ymd) ? ymd[2] : null;
                    default: /*year or time*/       return null;
                }
            }
        },
        
        HOUR(AkType.INT)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd_hms[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                        Extractors.getLongExtractor(type).getLong(source));
                switch(type)
                {
                    case TIME:      long hr = ymd_hms[3]; // hour in TIME doesnt have to be less than 24. It could be anything
                                    ymd_hms[3] = 1;       // but the checking method has no way to know, 
                                                          // so we put a legal value in hr field (which is a bit clumsy)
                                                          
                                    return validHrMinSec(ymd_hms) ? hr : null;
                    case DATETIME:  return validDayMonth(ymd_hms) && validHrMinSec(ymd_hms) ? ymd_hms[3] : null;
                    case TIMESTAMP: return ymd_hms[3];
                    default:        return null;
                }
            }
        },

        LAST_DAY(AkType.DATE)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                        Extractors.getLongExtractor(type).getLong(source));
   
                switch (type)
                {
                    case DATE:      
                    case DATETIME:  
                    case TIMESTAMP: Long last = getLastDay(ymd);
                                    if (last == null || ymd[1] * ymd[2] == 0 || ymd[2] > last) 
                                        return null;
                                    ymd[2] = last;
                                    return Extractors.getLongExtractor(AkType.DATE).getEncoded(ymd);
                    default: /*year or time*/       return null;
                }
            }
        },
        
        MINUTE(AkType.INT)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd_hms[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                        Extractors.getLongExtractor(type).getLong(source));
                switch(type)
                {
                    case TIME:      return validHrMinSec(ymd_hms) ? ymd_hms[4] : null;
                    case DATETIME:  return validDayMonth(ymd_hms) && validHrMinSec(ymd_hms) ? ymd_hms[4] : null;
                    case TIMESTAMP: return ymd_hms[4];
                    default:        return null;
                }
            }
        },
        
        MONTH(AkType.INT)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                        Extractors.getLongExtractor(type).getLong(source));
                
                switch (type)
                {
                    case DATE:
                    case DATETIME:  
                    case TIMESTAMP: return validDayMonth(ymd) ? ymd[1] : null;
                    default: /*year or time*/       return null;
                }
            }
            
        },
        
        MONTH_NAME(AkType.VARCHAR)
        {
            @Override
            public Long extract (ValueSource source)
            {
                return MONTH.extract(source);
            }            
        },
        
        QUARTER (AkType.INT)
        {
            @Override
            public Long extract (ValueSource source)
            {
                Long month = MONTH.extract(source);                
                if (month == null) return null;
                else if (month < 4) return 1L;
                else if (month < 7) return 2L;
                else if (month < 10) return 3L;
                else return 4L;
            }
        },
        
        SECOND(AkType.INT)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd_hms[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                        Extractors.getLongExtractor(type).getLong(source));
                switch(type)
                {
                    case TIME:      return validHrMinSec(ymd_hms) ? ymd_hms[5] : null;
                    case DATETIME:  return validDayMonth(ymd_hms) && validHrMinSec(ymd_hms) ? ymd_hms[5] : null;
                    case TIMESTAMP: return ymd_hms[5];
                    default:        return null;
                }
            }
        },
        
        TIME(AkType.TIME)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                                Extractors.getLongExtractor(type).getLong(source));
                LongExtractor extractor = Extractors.getLongExtractor(AkType.TIME);
                switch (type)
                {
                    case TIME:      return source.getTime();
                    case DATETIME:  
                    case TIMESTAMP: return validDayMonth(ymd) && validHrMinSec(ymd) ? extractor.getEncoded(ymd) : null;
                    default:        return null;
                }
            }
            
        },
        
        TIMESTAMP(AkType.TIMESTAMP)
        {
            @Override
            public Long extract (ValueSource source)
            {   
                switch (source.getConversionType())
                {
                    case DATE:      
                    case DATETIME:  return ConversionUtil.getDateTimeConverter().get(source).getMillis() / 1000L;
                    case TIMESTAMP: return source.getTimestamp();                        
                    default:        return null;
                }
            }
            
        },
        
        YEAR(AkType.INT)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long ymd_hms[] = Extractors.getLongExtractor(type).getYearMonthDayHourMinuteSecond(
                        Extractors.getLongExtractor(type).getLong(source));
                switch(type)
                {
                    case DATE:      return validDayMonth(ymd_hms) ? ymd_hms[0] : null;
                    case YEAR:      return ymd_hms[0];
                    case DATETIME:  
                    case TIMESTAMP: return validDayMonth(ymd_hms) && validHrMinSec(ymd_hms) ? ymd_hms[0] : null;
                    default:        return null;
                }
              
            }            
        };
        
        public final AkType underlying;
        
        private TargetExtractType (AkType type)
        {
            underlying = type;
        }

        abstract Long extract (ValueSource source);

        private static boolean validHrMinSec (long hms[])
        {
            return hms[3] >= 0 && hms[3] < 24 && hms[4] >= 0 && hms[4] < 60 && hms[5] >= 0 && hms[5] < 60;
        }

        private static Long getLastDay (long ymd[])
        {
            switch ((int)ymd[1])
            {
                case 2:     return ymd[0] % 400 == 0 || ymd[0] % 4 == 0 && ymd[0] % 100 != 0 ? 29L : 28L;
                case 4:
                case 6:
                case 9:
                case 11:    return 30L;
                case 3:
                case 1:
                case 5:
                case 7:
                case 8:
                case 10:
                case 0:
                case 12:    return 31L;                        
                default:    return null;
            }
        }
        
        private static boolean validDayMonth (long ymd[])
        {
            Long last = getLastDay(ymd);
            return last != null && ymd[2] <= last;
        }
    }
    
    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final TargetExtractType type;
        private QueryContext context;
        public InnerEvaluation (ExpressionEvaluation ev, TargetExtractType type)
        {
            super(ev);
            this.type = type;
        }
        
        @Override
        public void of(QueryContext context) {
            super.of(context);
            this.context = context;
        }

        private static final EnumSet<AkType> DATES = EnumSet.of( AkType.DATE, AkType.DATETIME, AkType.TIMESTAMP, AkType.YEAR);
        private static final EnumSet<AkType> TIMES = EnumSet.of(AkType.TIME, AkType.TIMESTAMP, AkType.DATETIME);
       
        // We could use collection in DateTimeField once the branch is in. 
        private static final String MONTHS[] = new DateFormatSymbols(new Locale(System.getProperty("user.language"))).getMonths();

        private AkType t;
        @Override
        public ValueSource eval() 
        {
            ValueSource source = operand();  
            if (source.isNull()) return NullValueSource.only();
            
            Long raw = null;
            try 
            {
                switch (type)
                {
                    case DAY_OF_YEAR:
                    case DATE:     
                    case DAY:
                    case TIMESTAMP:
                    case DATETIME:
                    case YEAR:
                    case MONTH:
                    case MONTH_NAME:
                    case LAST_DAY:
                    case QUARTER:   if (DATES.contains(source.getConversionType()))
                                        raw = type.extract(source);
                                    else
                                        for ( AkType ty : DATES)
                                        {
                                            raw = tryGetLong(t = ty);
                                            if (raw != null) 
                                            {
                                                raw = type.extract(new ValueHolder(t,raw.longValue()));
                                                break;
                                            }
                                        }
                                    break;                    
                    case SECOND: 
                    case TIME:
                    case HOUR:      
                    case MINUTE:    if (TIMES.contains(source.getConversionType()))
                                        raw = type.extract(source);
                                    else if (DATES.contains(source.getConversionType()))
                                        raw = 0L;
                                    else
                                        for (AkType t: TIMES)
                                        {
                                            raw = tryGetLong(t);
                                            if (raw != null)
                                            {
                                                raw = type.extract(new ValueHolder(t,raw.longValue()));
                                                break;
                                            }
                                        }
                }
           
                if (raw != null)
                {
                    if (type == TargetExtractType.MONTH_NAME)
                        valueHolder().putString(MONTHS[raw.intValue()-1]);
                    else
                        valueHolder().putRaw(type.underlying, raw.longValue());
                    return valueHolder();
                }
                else
                {
                    if (context != null)
                        context.warnClient(new InconvertibleTypesException(source.getConversionType(), AkType.DATETIME));
                    return NullValueSource.only();
                }
            }
            catch (InvalidDateFormatException ex)
            {
                if (context != null)
                    context.warnClient(ex);
                return NullValueSource.only();
            }
            catch (InvalidParameterValueException ex)
            {
                if (context != null)
                    context.warnClient(ex);
                return NullValueSource.only();
            }
            catch (IllegalFieldValueException ex)
            {
                if (context != null)
                    context.warnClient(new InvalidParameterValueException(ex.getMessage()));
                return NullValueSource.only();
            }
        }

        private Long tryGetLong(AkType targetType)
        {
            try
            {
                long l;
                AkType argType = operand().getConversionType();
                switch (argType)
                {
                    case TEXT:
                    case VARCHAR:  String st = Extractors.getStringExtractor().getObject(operand());
                                   l = Extractors.getLongExtractor(t = (targetType == AkType.DATE && st.length() > 10 ? AkType.DATETIME : targetType)).
                                           getLong(st);
                                   break;
                        
                    default:       long raw = Extractors.getLongExtractor(targetType).getLong(operand());
                                   if (targetType == AkType.TIMESTAMP) return null;
                                   else if (targetType == AkType.DATE )
                                   {
                                       t = AkType.DATETIME;
                                       if (raw <= 1000000000) return raw * 1000000L; // less than 10 digits
                                       else return raw;                             //, then fill the time portion with zeroes
                                   }
                                   l = Extractors.getLongExtractor(targetType).getLong(Extractors.getLongExtractor(targetType).asString(raw)); break;
                }
                return l;
            }
            catch (InconvertibleTypesException ex)
            {
                return null;
            }
            catch (InvalidDateFormatException ex)
            {
                return null;
            }           
        }
        
    }
     
    private static class InternalComposer extends UnaryComposer
    {
        private final TargetExtractType type;
        
        public InternalComposer (TargetExtractType type)
        {
            this.type = type;
        }
        

        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) 
        {
            return new ExtractExpression(argument, type);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            return ExpressionTypes.newType(type.underlying, argumentTypes.get(0).getPrecision(), argumentTypes.get(0).getScale());
        }        
    }
            
    private final TargetExtractType extractType;
    
    protected ExtractExpression (Expression e, TargetExtractType type)
    {
        super(type.underlying, e);
        extractType = type;
    }

    @Override
    public String name() 
    {
        return extractType.name();
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        if (operand().valueType() == AkType.NULL) return LiteralExpression.forNull().evaluation();        
        return new InnerEvaluation(operandEvaluation(), extractType);
    }

}
