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

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import java.util.Calendar;
import java.util.EnumSet;

public class ExtractExpression extends AbstractUnaryExpression
{
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
    @Scalar ({"hour", "hourofday"})
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

    protected static enum TargetExtractType
    {
        DATE(AkType.DATE)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong; 
                switch (type)
                {
                    case DATE:      return source.getDate();
                        
                    case DATETIME:  rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    long year = rawLong / 10000000000L;
                                    long month = rawLong / 100000000L % 100;
                                    long day = rawLong / 1000000L % 100;
                                    return vallidDayMonth(year, month,day) ? day + month*32 + year*512 : null;
                        
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    long y = calendar.get(Calendar.YEAR);
                                    long m = calendar.get(Calendar.MONTH) +1; // month in calendar is 0-based
                                    long d = calendar.get(Calendar.DAY_OF_MONTH);
                                    return d + m*32 + y*512;
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
                long rawLong;
                long y = 1970;
                long m = 1;
                long d = 1;
                long hr = 0;
                long min = 0;
                long sec = 0;
                switch (type)
                {
                    case DATE:      rawLong = source.getDate();
                                    y = rawLong / 512;
                                    m = rawLong / 32 % 16;
                                    d = rawLong % 32;
                                    return vallidDayMonth(y,m,d) ? y * 10000000000L + m * 100000000L + d * 1000000L : null;                        
                    case TIME:      return null;                        
                    case DATETIME:  rawLong = source.getDateTime();
                                    y = rawLong / 10000000000L;
                                    m = rawLong / 100000000L % 100;
                                    d = rawLong / 1000000L % 100;
                                    return vallidDayMonth(y,m,d) ? rawLong : null;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    y = calendar.get(Calendar.YEAR);
                                    m = calendar.get(Calendar.MONTH) +1; // month in calendar is 0-based
                                    d = calendar.get(Calendar.DAY_OF_MONTH);
                                    hr = calendar.get(Calendar.HOUR_OF_DAY);
                                    min = calendar.get(Calendar.MINUTE);
                                    sec = calendar.get(Calendar.SECOND);
                                    return y * 10000000000L + m * 100000000L + d * 1000000L + hr * 10000 + min * 100 + sec;
                    default:        return null;
                }
            }
            
        },
        DAY(AkType.LONG)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong ;
                switch (type)
                {
                    case DATE:      rawLong = source.getDate();
                                    long y = rawLong / 512;
                                    long m = rawLong / 32 % 16;
                                    long d = rawLong % 32;
                                    return vallidDayMonth (y,m,d)? d : null;
                    case DATETIME:  rawLong = source.getDateTime();
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    return vallidDayMonth(yr,mo,da) ? da : null;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return (long)calendar.get(Calendar.DAY_OF_MONTH);                    
                    default: /*year*/       return null;
                }
            }
        },
        
        HOUR(AkType.LONG)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long hr, min, sec;
                long rawLong;
                switch(type)
                {
                    case TIME:      rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    sec = rawLong % 100;
                                    min = rawLong / 100 % 100;
                                    hr = rawLong / 10000;
                                    return vallidHrMinSec(1, min,sec) ? hr : null; 
                    case DATETIME:  rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    sec = rawLong % 100;
                                    min = rawLong / 100 % 100;
                                    hr = rawLong / 10000L % 100;
                                    return vallidDayMonth(yr, mo,da) && vallidHrMinSec(hr, min,sec) ? hr : null;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return (long)calendar.get(Calendar.HOUR_OF_DAY);                    
                    default:        return null;
                }
            }
        },
        
        MINUTE(AkType.LONG)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong;
                long hr, min, sec;
                switch (type)
                {
                    case TIME:      rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    sec = rawLong % 100;
                                    min = rawLong / 100 % 100;               
                                    return vallidHrMinSec(0, min,sec) ? min : null;
                    case DATETIME:  rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    sec = rawLong % 100;
                                    min = rawLong / 100 % 100;
                                    hr = rawLong / 10000 % 100;
                                    return vallidDayMonth(yr, mo,da) && vallidHrMinSec(hr, min,sec) ? min : null;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return (long)calendar.get(Calendar.MINUTE);                             
                    default:        return null;
                }
                
            }
        },
        
        MONTH(AkType.LONG)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong ; 
                switch (type)
                {
                    case DATE:      rawLong = source.getDate();
                                    long y = rawLong / 512;
                                    long m = rawLong / 32 % 16;
                                    long d = rawLong % 32;
                                    return vallidDayMonth (y,m,d)? m : null;
                    case DATETIME:  rawLong = source.getDateTime();
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    return vallidDayMonth(yr,mo,da) ? mo : null;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return calendar.get(Calendar.MONTH) +1L;                    
                    default: /*year*/       return null;
                }
            }
            
        },
        
        SECOND(AkType.LONG)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong;
                switch (type)
                {
                    case TIME:      rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source)) % 100;
                                    return rawLong >= 0 && rawLong < 60 ? rawLong : null;
                    case DATETIME:  rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    long hr = rawLong / 10000 % 100;
                                    long min = rawLong / 100 % 100;
                                    long sec = rawLong % 100;
                                    return vallidDayMonth(yr,mo,da) && vallidHrMinSec(hr, min,sec) ? sec : null;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return (long)calendar.get(Calendar.SECOND);                             
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
                long rawLong;
                switch (type)
                {
                    case TIME:      return source.getTime();
                    case DATETIME:  rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    long hr = rawLong / 10000 % 100;
                                    long min = rawLong / 100 % 100;
                                    long sec = rawLong % 100;
                                    return vallidDayMonth(yr,mo,da) && vallidHrMinSec(hr, min,sec) ? rawLong % 1000000L : null;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    long h =  calendar.get(Calendar.HOUR_OF_DAY);
                                    long m = calendar.get(Calendar.MINUTE);
                                    long s = calendar.get(Calendar.SECOND);
                                    return h*10000 + m*100 + s;  
                    default:        return null;
                }
                
            }
            
        },
        
        TIMESTAMP(AkType.TIMESTAMP)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong;
                long y = 1970;
                long m = 1;
                long d = 1;
                long hr = 0;
                long min = 0;
                long sec = 0;
                switch (type)
                {
                    case DATE:      rawLong = source.getDate();
                                    y = rawLong / 512;
                                    m = rawLong / 32 % 16;
                                    d = rawLong % 32;
                                    if (!vallidDayMonth(y,m,d)) return null;
                                    Calendar cal = Calendar.getInstance();
                                    cal.set((int)y, (int)m -1, (int)d,0,0,0);
                                    return cal.getTimeInMillis() / 1000L;                        
                    case TIME:      return null;                        
                    case DATETIME:  rawLong = source.getDateTime();
                                    y = rawLong / 10000000000L;
                                    m = rawLong / 100000000L % 100;
                                    d = rawLong / 1000000L % 100;
                                    hr = rawLong /10000L % 100;
                                    min = rawLong / 100L % 100;
                                    sec = rawLong % 100;
                                    if (!vallidDayMonth(y,m,d) || !vallidHrMinSec(hr, min,sec)) return null;
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.set((int)y, (int)m -1, (int)d, (int)hr, (int)min, (int)sec);
                                    return calendar.getTimeInMillis() / 1000;                        
                    case TIMESTAMP: return source.getTimestamp();                        
                    default:        return null;
                }
            }
            
        },
        
        YEAR(AkType.YEAR)
        {
            @Override
            public Long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong ; 
                switch (type)
                {
                    case DATE:      rawLong = source.getDate();
                                    long y = rawLong / 512;
                                    long m = rawLong / 32 % 16;
                                    long d = rawLong % 32;
                                    return vallidDayMonth (y,m,d)? (y == 0 ? 0 : y - 1900) : null;
                    case DATETIME:  rawLong = source.getDateTime();
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    long hr = rawLong / 10000 % 100;
                                    long min = rawLong / 100 % 100;
                                    long sec = rawLong % 100;
                                    return vallidDayMonth(yr,mo,da) && vallidHrMinSec(hr,min,sec) ? (yr == 0 ? 0 : yr - 1900) : null;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return calendar.get(Calendar.YEAR) -1900L;                    
                    default: /*year*/       return source.getYear();
                }
            }
            
        };
        
        public final AkType underlying;
        
        private TargetExtractType (AkType type)
        {
            underlying = type;
        }

        abstract Long extract (ValueSource source);

        private static boolean vallidHrMinSec (long hr, long min, long sec)
        {
            return hr >= 0 && hr < 24 && min >= 0 && min < 60 && sec >= 0 && sec < 60;
        }

        private static boolean vallidDayMonth (long y, long m, long d)
        {
            switch ((int)m)
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
                case 0:
                case 12:    return d <= 31;                        
                default:    return false;
            }
        }
    }
    
    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final TargetExtractType type;
        public InnerEvaluation (ExpressionEvaluation ev, TargetExtractType type)
        {
            super(ev);
            this.type = type;
        }
        
        private static final EnumSet<AkType> DATES = EnumSet.of( AkType.DATE, AkType.DATETIME, AkType.TIMESTAMP, AkType.YEAR);
        private static final EnumSet<AkType> TIMES = EnumSet.of(AkType.TIME, AkType.TIMESTAMP, AkType.DATETIME);

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
                    case DATE:     
                    case DAY:
                    case TIMESTAMP:
                    case DATETIME:
                    case YEAR:
                    case MONTH:
                                    if (DATES.contains(source.getConversionType()))
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
                    valueHolder().putRaw(type.underlying, raw.longValue());
                    return valueHolder();
                }
                else return NullValueSource.only();
            }
            catch (InvalidDateFormatException ex)
            {
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
                    case VARCHAR: l = Extractors.getLongExtractor(targetType).getLong(operand().getString()); break;
                    case LONG:     long raw = operand().getLong();
                                   if (targetType == AkType.TIMESTAMP) return null;
                                   else if (targetType == AkType.DATE )
                                   {
                                       t = AkType.DATETIME;
                                       if (raw <= 1000000000) return raw * 1000000L; // less than 10 digits
                                       else return raw;                             //, then fill the time portion with zeroes
                                   }
                                   l = Extractors.getLongExtractor(targetType).getLong(Extractors.getLongExtractor(targetType).asString(raw)); break;
                    default:
                     l = Extractors.getLongExtractor(targetType).getLong(operand());
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
        protected Expression compose(Expression argument) 
        {
            return new ExtractExpression(argument, type);
        }

        @Override
        protected AkType argumentType(AkType givenType) 
        {
            return givenType;
        }

        @Override
        protected ExpressionType composeType(ExpressionType argumentType) 
        {
            return ExpressionTypes.newType(type.underlying, 0,0);
        }
        
    }
            
    private final TargetExtractType extractType;
    
    protected ExtractExpression (Expression e, TargetExtractType type)
    {
        super(type.underlying, e);
        extractType = type;
    }

    @Override
    protected String name() 
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
