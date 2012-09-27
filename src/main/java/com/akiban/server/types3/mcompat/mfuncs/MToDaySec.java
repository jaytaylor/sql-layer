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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public abstract class MToDaySec extends TOverloadBase
{
    public static final TOverload TO_DAYS = new MToDaySec(MDatetimes.DATETIME, "TO_DAYS")
    {
        @Override
        protected int computeDaySec(long[] ymd)
        {
            long now = MDatetimes.toJodaDatetime(ymd, "UTC").getMillis();
            return (int)((now - START) / MILLIS_PER_DAY);
        }
    };

    public static final TOverload TO_SECS = new MToDaySec(MDatetimes.DATETIME, "TO_SECONDS")
    {
        @Override
        protected int computeDaySec(long[] ymd)
        {
            long now = MDatetimes.toJodaDatetime(ymd, "UTC").getMillis();
            return (int)((now - START) / MILLIS_PER_SEC);
        }    
    };
    
    public static final TOverload TIME_TO_SEC = new MToDaySec(MDatetimes.TIME, "TIME_TO_SEC")
    {
        @Override
        public TOverloadResult resultType()
        {
            return TOverloadResult.fixed(MNumeric.INT.instance(10));
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            long hms[] = MDatetimes.decodeTime(inputs.get(0).getInt32());
            
            if (!MDatetimes.isValidHrMinSec(hms, false))
            {
                context.warnClient(new InvalidDateFormatException("time",
                                                                  MDatetimes.timeToString((int)inputs.get(0).getInt32())));
                output.putNull();
            }
            else
            {
                output.putInt32((int)(hms[MDatetimes.HOUR_INDEX] * SEC_PER_HOUR
                                       + hms[MDatetimes.MIN_INDEX] * SEC_PER_MIN
                                       + hms[MDatetimes.SEC_INDEX]));
            }
        }

        @Override
        protected int computeDaySec(long[] ymd)
        {
            throw new AssertionError("Should not be used");
        }   
    };
    
    private static final int SEC_PER_HOUR = 3600;
    private static final int SEC_PER_MIN = 60;
    private static final int MILLIS_PER_SEC = 1000;
    private static final int MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
    private static final long START = new DateTime(0, 1, 1, 
                                                   0, 0, 0, 0,
                                                   DateTimeZone.UTC).getMillis();
    

    private final TClass inputType;
    private final String name;

    abstract protected int computeDaySec(long ymd[]);

    private MToDaySec(TClass inputType, String name)
    {
        this.inputType = inputType;
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(inputType, 0);
    }
    
    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        long ymd[] = MDatetimes.decodeDatetime(inputs.get(0).getInt64());
        if (!MDatetimes.isValidDatetime(ymd))
        {
            context.warnClient(new InvalidDateFormatException("DATETIME",
                                                              MDatetimes.datetimeToString(inputs.get(0).getInt64())));

            output.putNull();
        }
        else
            output.putInt32(computeDaySec(ymd));
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT.instance(6));
    }
}
