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
            public long extract (ValueSource source)
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
                                    return vallidDayMonth(year, month,day) ? day + month*32 + year*512 : -1;
                        
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    long y = calendar.get(Calendar.YEAR);
                                    long m = calendar.get(Calendar.MONTH) +1; // month in calendar is 0-based
                                    long d = calendar.get(Calendar.DAY_OF_MONTH);
                                    return d + m*32 + y*512;
                    case LONG:      rawLong = source.getLong();
                                    long yr = rawLong /10000;
                                    long mo = rawLong / 100 % 100;
                                    long da = rawLong % 100;
                                    if(!TargetExtractType.vallidDayMonth(yr, mo, da) || (yr +mo + da == rawLong))
                                        return -1;
                                    else return yr * 512 + mo * 32 + da;
                    default:        return -1;
                }
            }
        },
             
        DATETIME(AkType.DATETIME)
        {
            @Override
            public long extract (ValueSource source)
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
                                    return vallidDayMonth(y,m,d) ? y * 10000000000L + m * 100000000L + d * 1000000L : -1;                        
                    case TIME:      return -1;                        
                    case DATETIME:  rawLong = source.getDateTime();
                                    y = rawLong / 10000000000L;
                                    m = rawLong / 100000000L % 100;
                                    d = rawLong / 1000000L % 100;
                                    return vallidDayMonth(y,m,d) ? rawLong : -1;
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
                    default:        return -1;
                }
            }
            
        },
        DAY(AkType.LONG)
        {
            @Override
            public long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong ;
                switch (type)
                {
                    case DATE:      rawLong = source.getDate();
                                    long y = rawLong / 512;
                                    long m = rawLong / 32 % 16;
                                    long d = rawLong % 32;
                                    return vallidDayMonth (y,m,d)? d : -1;
                    case DATETIME:  rawLong = source.getDateTime();
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    return vallidDayMonth(yr,mo,da) ? da : -1;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return calendar.get(Calendar.DAY_OF_MONTH);                    
                    default: /*year*/       return -1;
                }
            }
        },
        
        HOUR(AkType.LONG)
        {
            @Override
            public long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong;
                switch(type)
                {
                    case TIME:      rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    return rawLong / 10000L;
                    case DATETIME:  rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    long hr = rawLong / 10000L % 100;
                                    return vallidDayMonth(yr, mo,da) ? hr : -1;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return calendar.get(Calendar.HOUR_OF_DAY);                    
                    default:        return -1;
                }
            }
        },
        
        MINUTE(AkType.LONG)
        {
            @Override
            public long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong;
                switch (type)
                {
                    case TIME:      rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    return rawLong / 100 % 100;
                    case DATETIME:  rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    long min = rawLong / 100 % 100;
                                    return vallidDayMonth(yr, mo,da) ? min : -1;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return calendar.get(Calendar.MINUTE);                             
                    default:        return -1;
                }
                
            }
        },
        
        MONTH(AkType.LONG)
        {
            @Override
            public long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong ; 
                switch (type)
                {
                    case DATE:      rawLong = source.getDate();
                                    long y = rawLong / 512;
                                    long m = rawLong / 32 % 16;
                                    long d = rawLong % 32;
                                    return vallidDayMonth (y,m,d)? m : -1;
                    case DATETIME:  rawLong = source.getDateTime();
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    return vallidDayMonth(yr,mo,da) ? mo : -1;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return calendar.get(Calendar.MONTH) +1;
                    
                    default: /*year*/       return -1;
                }
            }
            
        },
        
        SECOND(AkType.LONG)
        {
            @Override
            public long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong;
                switch (type)
                {
                    case TIME:      rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    return rawLong % 100;
                    case DATETIME:  rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    long sec = rawLong % 100;
                                    return vallidDayMonth(yr,mo,da) ? sec : -1;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return calendar.get(Calendar.SECOND);                             
                    default:        return -1;
                }
                
            }
        },
        
        TIME(AkType.TIME)
        {
            @Override
            public long extract (ValueSource source)
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
                                    return vallidDayMonth(yr,mo,da) ? rawLong % 1000000L : -1;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    long h =  calendar.get(Calendar.HOUR_OF_DAY);
                                    long m = calendar.get(Calendar.MINUTE);
                                    long s = calendar.get(Calendar.SECOND);
                                    return h*10000 + m*100 + s;  
                    default:        return -1;
                }
                
            }
            
        },
        
        TIMESTAMP(AkType.TIMESTAMP)
        {
            @Override
            public long extract (ValueSource source)
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
                                    if (!vallidDayMonth(y,m,d)) return -1;
                                    Calendar cal = Calendar.getInstance();
                                    cal.set((int)y, (int)m -1, (int)d,0,0,0);
                                    return cal.getTimeInMillis() / 1000L;                        
                    case TIME:      return -1;                        
                    case DATETIME:  rawLong = source.getDateTime();
                                    y = rawLong / 10000000000L;
                                    m = rawLong / 100000000L % 100;
                                    d = rawLong / 1000000L % 100;
                                    hr = rawLong /10000L % 100;
                                    min = rawLong / 100L % 100;
                                    sec = rawLong % 100;
                                    if (!vallidDayMonth(y,m,d)) return -1;
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.set((int)y, (int)m -1, (int)d, (int)hr, (int)min, (int)sec);
                                    return calendar.getTimeInMillis() / 1000;                        
                    case TIMESTAMP: return source.getTimestamp();                        
                    default:        return -1;
                }
            }
            
        },
        
        YEAR(AkType.YEAR)
        {
            @Override
            public long extract (ValueSource source)
            {
                AkType type = source.getConversionType();
                long rawLong ; 
                switch (type)
                {
                    case DATE:      rawLong = source.getDate();
                                    long y = rawLong / 512;
                                    long m = rawLong / 32 % 16;
                                    long d = rawLong % 32;
                                    return vallidDayMonth (y,m,d)? (y == 0 ? 0 : y - 1900) : -1;
                    case DATETIME:  rawLong = source.getDateTime();
                                    long yr = rawLong / 10000000000L;
                                    long mo = rawLong / 100000000L % 100;
                                    long da = rawLong / 1000000L % 100;
                                    return vallidDayMonth(yr,mo,da) ? (yr == 0 ? 0 : yr - 1900) : -1;
                    case TIMESTAMP: rawLong = Math.abs(Extractors.getLongExtractor(type).getLong(source));
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(rawLong * 1000);
                                    return calendar.get(Calendar.YEAR) -1900;                    
                    default: /*year*/       return source.getYear();
                }
            }
            
        };
        
        public final AkType underlying;
        
        private TargetExtractType (AkType type)
        {
            underlying = type;
        }

        abstract long extract (ValueSource source);
        
        protected static boolean vallidDayMonth (long y, long m, long d)
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
        
        private static final EnumSet<AkType> DATES = EnumSet.of(AkType.DATE, AkType.DATETIME, AkType.TIMESTAMP, AkType.YEAR);
        private static final EnumSet<AkType> TIMES = EnumSet.of(AkType.TIME, AkType.TIMESTAMP, AkType.DATETIME);
       
        @Override
        public ValueSource eval() 
        {
            ValueSource source = operand();  
            if (source.isNull()) return NullValueSource.only();
            
            long raw = -1;
            try 
            {
                switch (type)
                {
                    case DATE:      if (DATES.contains(source.getConversionType()) || source.getConversionType() == AkType.LONG)
                                        raw = type.extract(source);
                                    else
                                        for (AkType t : DATES)
                                        {
                                            raw = tryGetLong(t);
                                            if (raw != -1) 
                                            {
                                                raw = type.extract(new ValueHolder(t,raw));
                                                break;
                                            }
                                        }
                                    break;
                    case DAY:
                    case TIMESTAMP:
                    case DATETIME:
                    case YEAR:
                    case MONTH:     if (DATES.contains(source.getConversionType()))
                                        raw = type.extract(source);
                                    else
                                        for (AkType t : DATES)
                                        {
                                            raw = tryGetLong(t);
                                            if (raw != -1) 
                                            {
                                                raw = type.extract(new ValueHolder(t,raw));
                                                break;
                                            }
                                        }
                                    break;                    
                    case SECOND: 
                    case TIME:
                    case HOUR:      
                    case MINUTE:    if (TIMES.contains(source.getConversionType()))
                                        raw = type.extract(source);
                                    else
                                        for (AkType t: TIMES)
                                        {
                                            raw = tryGetLong(t);
                                            if (raw != -1)
                                            {
                                                raw = type.extract(new ValueHolder(t,raw));
                                                break;
                                            }
                                        }
                }
           
                if (raw != -1)
                {
                    valueHolder().putRaw(type.underlying, raw);
                    return valueHolder();
                }
                else return NullValueSource.only();
            }
            catch (InvalidDateFormatException ex)
            {
                return NullValueSource.only();
            }
        }
        
        private long tryGetLong(AkType targetType)
        {
            try
            {
                long l;
                AkType argType = operand().getConversionType();
                switch (argType)
                {
                    case TEXT:
                    case VARCHAR: l = Extractors.getLongExtractor(targetType).getLong(operand().getString()); break;
                    case LONG:     if (targetType == AkType.TIMESTAMP || targetType == AkType.DATE) return -1;
//                                   else if (targetType == AkType.DATE )
//                                   {
//                                       long raw = operand().getLong();
//                                       long yr = raw /10000;
//                                       long mo = raw / 100 % 100;
//                                       long day = raw % 100;
//                                       if(!TargetExtractType.vallidDayMonth(yr, mo, day) || (yr +mo + day == raw))
//                                           return -2; //throw new InvalidDateFormatException("date", raw + "");
//                                       else return
//                                               yr * 512 + mo * 32 + day;
//                                   }
                                   l = Extractors.getLongExtractor(targetType).getLong(Extractors.getLongExtractor(targetType).asString(operand().getLong())); break; 
                    default:
                     l = Extractors.getLongExtractor(targetType).getLong(operand());
                }
               
                return l;
            }
            catch (InconvertibleTypesException ex)
            {
                return -1;
            }
            catch (InvalidDateFormatException ex)
            {
                return -1;
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
        return extractType +" ()";
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        if (operand().valueType() == AkType.NULL) return LiteralExpression.forNull().evaluation();        
        return new InnerEvaluation(operandEvaluation(), extractType);
    }

}
