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

import com.akiban.server.types3.*;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import com.akiban.sql.parser.TernaryOperatorNode;

public abstract class MTimestampDiff extends TOverloadBase
{
    public static TOverload[] create()
    {
        return new TOverload[]
        {
            new MTimestampDiff(MDatetimes.DATE)
            {
                @Override
                protected long[] getYMD(PValueSource source, TExecutionContext context)
                {
                    return MDatetimes.decodeDate(source.getInt32());
                }
                
                @Override
                protected long tryGetUnix(PValueSource source, TExecutionContext context)
                {
                    return MDatetimes.getTimestamp(MDatetimes.decodeDate(source.getInt32()),
                                                   context.getCurrentTimezone());
                }
            },
            new MTimestampDiff(MDatetimes.DATETIME)
            {
                @Override
                protected long[] getYMD(PValueSource source, TExecutionContext context)
                {
                    return MDatetimes.decodeDatetime(source.getInt64());
                }

                @Override
                protected long tryGetUnix(PValueSource source, TExecutionContext context)
                {
                    return MDatetimes.getTimestamp(MDatetimes.decodeDatetime(source.getInt64()),
                                                   context.getCurrentTimezone());
                }
            },
            new MTimestampDiff(MDatetimes.TIMESTAMP)
            {
                @Override
                protected long[] getYMD(PValueSource source, TExecutionContext context)
                {
                    return MDatetimes.decodeTimestamp(source.getInt32(), context.getCurrentTimezone());
                }

                @Override
                protected long tryGetUnix(PValueSource source, TExecutionContext context)
                {
                    return source.getInt32();
                }
            }
        };
    }

    private static final long[] MILLIS_DIV = new long[6];
    private static final int[] MONTH_DIV = {12, 4, 1};

    private static final int MILLIS_BASE = TernaryOperatorNode.WEEK_INTERVAL;
    private static final int MONTH_BASE = TernaryOperatorNode.YEAR_INTERVAL;
    static
    {
        int mul[] ={7, 24, 60, 60, 1000};

        MILLIS_DIV[5] = 1;
        for (int n = 4; n >= 0; --n)
            MILLIS_DIV[n] = MILLIS_DIV[n + 1] * mul[n];
    }

    private final TClass dateType;

    protected abstract long[] getYMD(PValueSource source, TExecutionContext context);
    protected abstract long tryGetUnix(PValueSource source, TExecutionContext context);

    private MTimestampDiff(TClass dtType)
    {
        dateType = dtType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.INT, 0).covers(dateType, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        int type = inputs.get(0).getInt32();
        
        PValueSource date1 = inputs.get(1);
        PValueSource date2 = inputs.get(2);

        switch(type)
        {
            case TernaryOperatorNode.YEAR_INTERVAL:
            case TernaryOperatorNode.QUARTER_INTERVAL:
            case TernaryOperatorNode.MONTH_INTERVAL:
                output.putInt64(doMonthSubtraction(getYMD(date2, context), getYMD(date1, context))
                                / MONTH_DIV[type - MONTH_BASE]);
                break;
            case TernaryOperatorNode.WEEK_INTERVAL:
            case TernaryOperatorNode.DAY_INTERVAL:
            case TernaryOperatorNode.HOUR_INTERVAL:
            case TernaryOperatorNode.MINUTE_INTERVAL:
            case TernaryOperatorNode.SECOND_INTERVAL:
            case TernaryOperatorNode.FRAC_SECOND_INTERVAL:
                output.putInt64((tryGetUnix(date2, context) - tryGetUnix(date1, context))
                                / MILLIS_DIV[type - MILLIS_BASE]);
                break;
            default:
                        throw new UnsupportedOperationException("Unknown INTERVAL_TYPE: " + type);
        }
    }

    @Override
    public String overloadName()
    {
        return "TIMESTAMPDIFF";
    }
    
    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.BIGINT.instance());
    }

    private static long doMonthSubtraction (long d1[], long d2[])
    {
        long ret = (d1[0] - d2[0]) * 12 + d1[1] - d2[1];

        // adjust the day difference
        if (ret > 0 && d1[2] < d2[2]) --ret;
        else if (ret < 0 && d1[2] > d2[2]) ++ret;

        return ret;
    }
}
