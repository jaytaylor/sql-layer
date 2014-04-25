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

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.InvalidDateFormatException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;

import static com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.*;

@SuppressWarnings("unused")
public class MDateTimeDiff
{
    public static final TScalar INSTANCES[] = new TScalar[]
    {
        new DateTimeDiff(ArgType.DATE, ArgType.DATE, "DATEDIFF", false, false)
        {
            @Override
            int compute(long val0[], long val1[], TExecutionContext context)
            {
                return (int)(millisDiff(val0, val1) / MILLIS_PER_DAY);
            }
            
            @Override
            public TOverloadResult resultType()
            {
                return TOverloadResult.fixed(MNumeric.INT, 7);
            }
        },
        new DateTimeDiff(ArgType.TIME, ArgType.TIME, "TIMEDIFF")
        {
            @Override
            int compute(long val0[], long val1[], TExecutionContext context)
            {
                return substractTime(val0, val1, context);
            }
         
            @Override
            public int[] getPriorities()
            {
                return new int[] {0};
            }
        },
        new DateTimeDiff(ArgType.TIME, ArgType.TIME_VARCHAR, "TIMEDIFF", true, false)
        {
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return substractTime(arg0, arg1, context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {1};
            }
        },
        new DateTimeDiff(ArgType.TIME_VARCHAR, ArgType.TIME, "TIMEDIFF", false, true)
        {
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return substractTime(arg0, arg1, context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {2};
            }
        },
        new DateTimeDiff(ArgType.VARCHAR, ArgType.VARCHAR, "TIMEDIFF")
        {
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
            {
                long ymd0[], ymd1[];
                StringType t0[] = new StringType[1];
                StringType t1[] = new StringType[1];

                ymd0 = ArgType.VARCHAR.getYMDHMS(inputs.get(0), t0, context);
                ymd1 = ArgType.VARCHAR.getYMDHMS(inputs.get(1), t1, context);
                
                if (t0[0] == StringType.UNPARSABLE && t1[0] == StringType.TIME_ST)
                {
                    ymd0 = new long[] {1970, 1, 1, 0, 0,0};
                    output.putInt32(substractTime(ymd0, ymd1, context));
                }
                else if (t1[0] == StringType.UNPARSABLE && t0[0] == StringType.TIME_ST)
                {
                    ymd1 = new long[] {1970, 1, 1, 0, 0,0};
                    output.putInt32(substractTime(ymd0, ymd1, context));
                }
                else if (ymd0 == null || ymd1 == null 
                         || t0[0] != t1[0] 
                         || !MDateAndTime.isValidType(t0[0]) || !MDateAndTime.isValidType(t1[0]))
                    output.putNull();
                else
                    output.putInt32(t0[0] == StringType.TIME_ST
                                        ? substractTime(ymd0, ymd1, context)
                                        : millisToTime(millisDiff(ymd0, ymd1), context));
            }
            
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                throw new AkibanInternalException("Not Used");
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {0};
            }
        },
        new DateTimeDiff(ArgType.DATETIME, ArgType.TIMESTAMP, "TIMEDIFF")
        {
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return millisToTime(millisDiff(arg0, arg1), context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {0};
            }
        },
        new DateTimeDiff(ArgType.TIMESTAMP, ArgType.DATETIME, "TIMEDIFF")
        {
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return millisToTime(millisDiff(arg0, arg1), context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {0};
            }
        },
        new DateTimeDiff(ArgType.DATETIME, ArgType.VARCHAR, "TIMEDIFF", true, false)
        {   
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return millisToTime(millisDiff(arg0, arg1), context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {1};
            }
        },
        new DateTimeDiff(ArgType.VARCHAR, ArgType.DATETIME, "TIMEDIFF", false, true)
        {
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return millisToTime(millisDiff(arg0, arg1), context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {2};
            }
        },
        new DateTimeDiff(ArgType.TIMESTAMP, ArgType.VARCHAR, "TIMEDIFF", true, false)
        {
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return millisToTime(millisDiff(arg0, arg1), context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {1};
            }
        },
        new DateTimeDiff(ArgType.VARCHAR, ArgType.TIMESTAMP, "TIMEDIFF", false, true)
        {
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return millisToTime(millisDiff(arg0, arg1), context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {2};
            }
        },

        // UNSUPPORTED cases
        new RejectedCase(ArgType.DATE, ArgType.DATETIME, "TIMEDIFF"),
        new RejectedCase(ArgType.DATE, ArgType.TIME, "TIMEDIFF"),
        new RejectedCase(ArgType.DATE, ArgType.DATETIME, "TIMEDIFF"),
        new RejectedCase(ArgType.DATE, ArgType.VARCHAR, "TIMEDIFF"),
       
        new RejectedCase(ArgType.DATETIME, ArgType.DATE, "TIMEDIFF"),
        new RejectedCase(ArgType.DATETIME, ArgType.TIME, "TIMEDIFF"),      
        
        new RejectedCase(ArgType.TIMESTAMP, ArgType.DATE, "TIMEDIFF"),
        new RejectedCase(ArgType.TIMESTAMP, ArgType.TIME, "TIMEDIFF"),
        
        // Anything else should be casted to TIME
        
        new DateTimeDiff(ArgType.TIME, ArgType.TIME, "TIMEDIFF", false, false)
        {
            @Override
            int compute(long val0[], long val1[], TExecutionContext context)
            {
                return substractTime(val0, val1, context);
            }
         
            @Override
            public int[] getPriorities()
            {
                return new int[] {3};
            }
        },
            
    };


    // ------------------- static members --------------------------------------
    private static enum ArgType
    {
        DATE(MDateAndTime.DATE)
        {
            @Override
            long[] getYMDHMS(ValueSource source, StringType[] type, TExecutionContext context)
            {
                int date = source.getInt32();
                long ymd[] = MDateAndTime.decodeDate(date);
                type[0] = StringType.DATE_ST;
                if (MDateAndTime.isValidDateTime_Zeros(ymd))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("DATE", MDateAndTime.dateToString(date)));
                    return null;
                }
            }
        },
        TIME(MDateAndTime.TIME)
        {
            @Override
            long[] getYMDHMS(ValueSource source, StringType[] type, TExecutionContext context)
            {
                int time = source.getInt32();
                long ymd[] = MDateAndTime.decodeTime(time);
                type[0] = StringType.TIME_ST;
                if (MDateAndTime.isValidHrMinSec(ymd, false, false))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("TIME", MDateAndTime.timeToString(time)));
                    return null;
                }
            }
        },
        DATETIME(MDateAndTime.DATETIME)
        {
            @Override
            long[] getYMDHMS(ValueSource source, StringType[] type, TExecutionContext context)
            {
                long datetime = source.getInt64();
                long ymd[] = MDateAndTime.decodeDateTime(datetime);
                type[0] = StringType.DATETIME_ST;
                if (MDateAndTime.isValidDateTime_Zeros(ymd))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("DATETIME", MDateAndTime.dateTimeToString(datetime)));
                    return null;
                }
            }
        },
        TIMESTAMP(MDateAndTime.TIMESTAMP)
        {
            @Override
            long[] getYMDHMS(ValueSource source, StringType[] type, TExecutionContext context)
            {
                int ts = source.getInt32();
                long ymd[] = MDateAndTime.decodeTimestamp(ts, context.getCurrentTimezone());
                type[0] = StringType.DATETIME_ST;
                if (MDateAndTime.isValidDateTime_Zeros(ymd))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("TIMESTAMP", MDateAndTime.dateTimeToString(ts)));
                    return null;
                }
            }
        },
        VARCHAR(MString.VARCHAR)
        {
            @Override
            long[] getYMDHMS(ValueSource source, StringType[] type, TExecutionContext context)
            {
                String st = source.getString();
                long hms[] = new long[6];
                type[0] = MDateAndTime.parseDateOrTime(st, hms);
                switch(type[0])
                {
                    case DATE_ST:
                    case TIME_ST:
                    case DATETIME_ST:
                        return hms;
                    default:
                        type[0] = StringType.UNPARSABLE;
                        context.warnClient(new InvalidDateFormatException("datetime", st));
                        return null;
                }
            }
        },
        TIME_VARCHAR(MString.VARCHAR)
        {
            @Override
            long[] getYMDHMS(ValueSource source, StringType[] type, TExecutionContext context)
            {
                String st = source.getString();
                long hms[] = new long[6];
                type[0] = MDateAndTime.parseDateOrTime(st, hms);
                switch(type[0])
                {
                    case DATE_ST:
                    case DATETIME_ST:
                        return null;
                    case TIME_ST:
                        return hms;
                    case INVALID_DATE_ST:
                    case INVALID_DATETIME_ST:
                    case INVALID_TIME_ST:
                        context.warnClient(new InvalidDateFormatException("datetime", st));
                        return null;
                    default:
                        // if failed to parse as TIME, return 0 (rathern than NULL)
                        // because that's how MySQL does it
                        context.warnClient(new InvalidDateFormatException("time", st));
                        type[0] = StringType.TIME_ST;
                        toZero(hms);
                        return hms;
                }
            }
        }
        ;
        
        abstract long[] getYMDHMS(ValueSource source, StringType[] type, TExecutionContext context);
        final TClass type;
        private ArgType(TClass type)
        {
            this.type = type;
        }
        
        protected static void toZero(long hms[])
        {
            for (int n = MDateAndTime.HOUR_INDEX; n < hms.length; ++n)
                    hms[n] = 0;
        }
    }
    
    private static class RejectedCase extends DateTimeDiff
    {
        RejectedCase(ArgType left, ArgType right, String name)
        {
            super(left, right, name);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
        {
            output.putNull();
        }

        @Override
        int compute(long[] arg0, long[] arg1, TExecutionContext context)
        {
            throw new AkibanInternalException("Not Used.");
        }
        
        @Override
        public int[] getPriorities()
        {
            return new int[] {0};
        }
    }
    
    private abstract static class DateTimeDiff extends TScalarBase
    {
        abstract int compute(long arg0[], long arg1[], TExecutionContext context);
        private final ArgType arg0Type;
        private final boolean exact0;
        
        private final ArgType arg1Type;
        private final boolean exact1;
        
        private final String name;
        
        DateTimeDiff(ArgType arg0, ArgType arg1, String name)
        {
            this(arg0, arg1, name, true, true);
        }
        
        DateTimeDiff(ArgType arg0, ArgType arg1, String name, boolean e0, boolean e1)
        {
            arg0Type = arg0;
            exact0 = e0;
            
            arg1Type = arg1;
            exact1 = e1;
            
            this.name = name;
        }

        @Override
        protected void buildInputSets(TInputSetBuilder builder)
        {
            builder.setExact(exact0).covers(arg0Type.type, 0).setExact(exact1).covers(arg1Type.type, 1);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
        {
            long ymd0[], ymd1[];
            StringType t0[] = new StringType[1];
            StringType t1[] = new StringType[1];

            if ((ymd0 = arg0Type.getYMDHMS(inputs.get(0), t0, context)) == null
                    || (ymd1 = arg1Type.getYMDHMS(inputs.get(1), t1, context)) == null
                    || t0[0] != t1[0])
                output.putNull();
            else
                output.putInt32(compute(ymd0, ymd1, context));
        }
        
        @Override
        public String displayName()
        {
            return name;
        }

        @Override
        public TOverloadResult resultType()
        {
            return TOverloadResult.fixed(TIME);
        }
    }
    
    private static int substractTime(long val0[], long val1[], TExecutionContext context)
    {
        return millisToTime(hmsToMillis(val0) - hmsToMillis(val1), context);
    }
    
    private static long millisDiff(long val0[], long val1[])
    {
        return MDateAndTime.toJodaDateTime(val0, "UTC").getMillis()
                                 - MDateAndTime.toJodaDateTime(val1, "UTC").getMillis();
    }
    
    private static int millisToTime(long millis, TExecutionContext context)
    {
        int hr = (int) (millis / MILLIS_PER_HOUR);
        millis -= hr * MILLIS_PER_HOUR;

        int min = (int) (millis / MILLIS_PER_MIN);
        millis -= min * MILLIS_PER_MIN;

        int sec = (int) (millis / MILLIS_PER_SEC);

        return MDateAndTime.encodeTime(hr, min, sec, context);
    }
    
    private static long hmsToMillis(long hms[])
    {
        int n = HOUR_INDEX;
        int sign = 1;
        
        while (n < hms.length && hms[n] >= 0)
            ++n;
        
        if (n < hms.length)
            hms[n] = hms[n] * (sign = -1);
        
        return sign * (hms[HOUR_INDEX] * MILLIS_PER_HOUR
                + hms[MIN_INDEX] * MILLIS_PER_MIN
                + hms[SEC_INDEX] * MILLIS_PER_SEC);
    }
    
    private static final long MILLIS_PER_SEC = 1000L;
    private static final long MILLIS_PER_MIN = 60 * 1000L;
    private static final long MILLIS_PER_HOUR = 3600 * 1000L;
    private static final long MILLIS_PER_DAY = 24 * 3600L * 1000L;
    
}
