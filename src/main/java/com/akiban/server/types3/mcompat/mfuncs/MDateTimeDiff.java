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

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

import static com.akiban.server.types3.mcompat.mtypes.MDatetimes.*;

public class MDateTimeDiff
{
    public static final TOverload INSTANCES[] = new TOverload[]
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
                return TOverloadResult.fixed(MNumeric.INT.instance(7));
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
        new DateTimeDiff(ArgType.TIME, ArgType.VARCHAR, "TIMEDIFF")
        {
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return substractTime(arg0, arg1, context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {0};
            }
        },
        new DateTimeDiff(ArgType.VARCHAR, ArgType.TIME, "TIMEDIFF")
        {
            @Override
            int compute(long[] arg0, long[] arg1, TExecutionContext context)
            {
                return substractTime(arg0, arg1, context);
            }
            
            @Override
            public int[] getPriorities()
            {
                return new int[] {0};
            }
        },
        new DateTimeDiff(ArgType.VARCHAR, ArgType.VARCHAR, "TIMEDIFF")
        {
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
            {
                long ymd0[], ymd1[];
                StringType t0[] = new StringType[1];
                StringType t1[] = new StringType[1];

                if ((ymd0 = ArgType.VARCHAR.getYMDHMS(inputs.get(0), t0, context)) == null
                    || (ymd1 = ArgType.VARCHAR.getYMDHMS(inputs.get(1), t1, context)) == null
                    || t0[0] != t1[0])
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
        new DateTimeDiff(ArgType.DATETIME, ArgType.VARCHAR, "TIMEDIFF")
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
        new DateTimeDiff(ArgType.VARCHAR, ArgType.DATETIME, "TIMEDIFF")
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
        new DateTimeDiff(ArgType.TIMESTAMP, ArgType.VARCHAR, "TIMEDIFF")
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
        new DateTimeDiff(ArgType.VARCHAR, ArgType.TIMESTAMP, "TIMEDIFF")
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
                return new int[] {1};
            }
        },
            
    };


    // ------------------- static members --------------------------------------
    private static enum ArgType
    {
        DATE(MDatetimes.DATE)
        {
            @Override
            long[] getYMDHMS(PValueSource source, StringType[] type, TExecutionContext context)
            {
                int date = source.getInt32();
                long ymd[] = MDatetimes.decodeDate(date);
                type[0] = StringType.DATE_ST;
                if (MDatetimes.isValidDayMonth(ymd))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("DATE", MDatetimes.dateToString(date)));
                    return null;
                }
            }
        },
        TIME(MDatetimes.TIME)
        {
            @Override
            long[] getYMDHMS(PValueSource source, StringType[] type, TExecutionContext context)
            {
                int time = source.getInt32();
                long ymd[] = MDatetimes.decodeTime(time);
                type[0] = StringType.TIME_ST;
                if (MDatetimes.isValidHrMinSec(ymd, false))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("TIME", MDatetimes.timeToString(time)));
                    return null;
                }
            }
        },
        DATETIME(MDatetimes.DATETIME)
        {
            @Override
            long[] getYMDHMS(PValueSource source, StringType[] type, TExecutionContext context)
            {
                long datetime = source.getInt64();
                long ymd[] = MDatetimes.decodeDatetime(datetime);
                type[0] = StringType.DATETIME_ST;
                if (MDatetimes.isValidDatetime(ymd))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("DATETIME", MDatetimes.datetimeToString(datetime)));
                    return null;
                }
            }
        },
        TIMESTAMP(MDatetimes.TIMESTAMP)
        {
            @Override
            long[] getYMDHMS(PValueSource source, StringType[] type, TExecutionContext context)
            {
                int ts = source.getInt32();
                long ymd[] = MDatetimes.decodeTimestamp(ts, context.getCurrentTimezone());
                type[0] = StringType.DATETIME_ST;
                if (MDatetimes.isValidDatetime(ymd))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("TIMESTAMP", MDatetimes.datetimeToString(ts)));
                    return null;
                }
            }
        },
        VARCHAR(MString.VARCHAR)
        {
            @Override
            long[] getYMDHMS(PValueSource source, StringType[] type, TExecutionContext context)
            {
                long hms[] = new long[6];
                try
                {
                    type[0] = MDatetimes.parseDateOrTime(source.getString(), hms);
                    return hms;
                }
                catch (InvalidDateFormatException e)
                {
                    context.warnClient(e);
                    return null;
                }
            }
        }
        ;
        
        abstract long[] getYMDHMS(PValueSource source, StringType[] type, TExecutionContext context);
        final TClass type;
        private ArgType(TClass type)
        {
            this.type = type;
        }
    }
    
    private static class RejectedCase extends DateTimeDiff
    {
        RejectedCase(ArgType left, ArgType right, String name)
        {
            super(left, right, name);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
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
    
    private abstract static class DateTimeDiff extends TOverloadBase
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
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
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
        return MDatetimes.toJodaDatetime(val0, "UTC").getMillis() 
                                 - MDatetimes.toJodaDatetime(val1, "UTC").getMillis();
    }
    
    private static int millisToTime(long millis, TExecutionContext context)
    {
        int hr = (int) (millis / MILLIS_PER_HOUR);
        millis -= hr * MILLIS_PER_HOUR;

        int min = (int) (millis / MILLIS_PER_MIN);
        millis -= min * MILLIS_PER_MIN;

        int sec = (int) (millis / MILLIS_PER_SEC);

        return MDatetimes.encodeTime(hr, min, sec, context);
    }
    
    private static long hmsToMillis(long hms[])
    {
        return hms[HOUR_INDEX] * MILLIS_PER_HOUR
                + hms[MIN_INDEX] * MILLIS_PER_MIN
                + hms[SEC_INDEX] * MILLIS_PER_SEC;
    }
    
    private static final long MILLIS_PER_SEC = 1000L;
    private static final long MILLIS_PER_MIN = 60 * 1000L;
    private static final long MILLIS_PER_HOUR = 3600 * 1000L;
    private static final long MILLIS_PER_DAY = 24 * 3600L * 1000L;
    
}
