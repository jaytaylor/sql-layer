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

import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

import static com.akiban.server.types3.mcompat.mtypes.MDatetimes.*;

public abstract class MDateTimeDiff extends TOverloadBase
{
    public static final TOverload INSTANCES[] = new TOverload[]
    {
        new MDateTimeDiff("DATEDIFF", MDatetimes.DATE)
        {
            @Override
            long[] toYMDHMS(PValueSource source, TExecutionContext context)
            {
                int date = source.getInt32();
                long ymd[] = MDatetimes.decodeDate(date);
                if (MDatetimes.isValidDayMonth(ymd))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("DATE", MDatetimes.dateToString(date)));
                    return null;
                }
            }

            @Override
            int compute(long val0[], long val1[], TExecutionContext context)
            {
                return (int)((MDatetimes.toJodaDatetime(val0, "UTC").getMillis() 
                                 - MDatetimes.toJodaDatetime(val1, "UTC").getMillis())
                             / MILLIS_PER_DAY);
            }
        },
        new MDateTimeDiff("TIMEDIFF", MDatetimes.TIME)
        {
            @Override
            public TOverloadResult resultType()
            {
                return TOverloadResult.fixed(TIME.instance());
            }
            
            @Override
            long[] toYMDHMS(PValueSource source, TExecutionContext context)
            {
                int time = source.getInt32();
                long ymd[] = MDatetimes.decodeTime(time);
                if (MDatetimes.isValidHrMinSec(ymd, false))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("TIME", MDatetimes.timeToString(time)));
                    return null;
                }
            }

            @Override
            int compute(long val0[], long val1[], TExecutionContext context)
            {
                long millis = hmsToMillis(val0) - hmsToMillis(val1);
                
                // convert back to hour, minute, second
                
                int hr = (int) (millis / MILLIS_PER_HOUR);
                millis -= hr * MILLIS_PER_HOUR;
                
                int min = (int) (millis / MILLIS_PER_MIN);
                millis -= min * MILLIS_PER_MIN;
                
                int sec = (int) (millis / MILLIS_PER_SEC);
                
                return MDatetimes.encodeTime(hr, min, sec, null);
                
                            
            }
            
            private long hmsToMillis(long hms[])
            {
                return hms[HOUR_INDEX] * 3600
                        + hms[MIN_INDEX] * 60
                        + hms[SEC_INDEX] * 1000;
            }
        },
    };
    
    abstract long[] toYMDHMS(PValueSource source, TExecutionContext context);
    abstract int compute(long val0[], long val1[], TExecutionContext context);

    private final String name;
    private final TClass inputType;
    private MDateTimeDiff(String name, TClass inputType)
    {
        this.name = name;
        this.inputType = inputType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(inputType, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        long ymd0[], ymd1[];
        
        if ((ymd0 = toYMDHMS(inputs.get(0), context)) == null
                || (ymd1 = toYMDHMS(inputs.get(1), context)) == null)
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
        return TOverloadResult.fixed(MNumeric.INT.instance(7));
    }
    
    private static final long MILLIS_PER_SEC = 1000L;
    private static final long MILLIS_PER_MIN = 60 * 1000L;
    private static final long MILLIS_PER_HOUR = 3600 * 100L;
    private static final long MILLIS_PER_DAY = 24 * 3600L * 1000L;
    
}
