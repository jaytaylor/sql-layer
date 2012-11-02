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

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import com.akiban.sql.parser.TernaryOperatorNode;

public class MTimestampDiff extends TScalarBase
{
    public static final TScalar instance = new MTimestampDiff();
    
    private MTimestampDiff()
    {
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.INT, 0).covers(MDatetimes.TIMESTAMP, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        int unit = inputs.get(0).getInt32();
        
        PValueSource date1 = inputs.get(1);
        PValueSource date2 = inputs.get(2);
        
        switch(unit)
        {
            case TernaryOperatorNode.YEAR_INTERVAL:
            case TernaryOperatorNode.QUARTER_INTERVAL:
            case TernaryOperatorNode.MONTH_INTERVAL:
                doMonthSubtraction(getYMD(date2),
                                   getYMD(date1),
                                   MONTH_DIV[unit - MONTH_BASE],
                                   output);
                break;
            case TernaryOperatorNode.WEEK_INTERVAL:
            case TernaryOperatorNode.DAY_INTERVAL:
            case TernaryOperatorNode.HOUR_INTERVAL:
            case TernaryOperatorNode.MINUTE_INTERVAL:
            case TernaryOperatorNode.SECOND_INTERVAL:
            case TernaryOperatorNode.FRAC_SECOND_INTERVAL:
                long unix1 = getUnix(date1);
                long unix2 = getUnix(date2);
                output.putInt64((unix2 - unix1) / MILLIS_DIV[unit - MILLIS_BASE]);
                break;
            default:
                throw new UnsupportedOperationException("Unknown UNIT: " + unit);
        
        }
    }

    @Override
    public String displayName()
    {
        return "TIMESTAMPDIFF";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.BIGINT, 21);
    }

    private long [] getYMD(PValueSource source)
    {
        return MDatetimes.decodeTimestamp(source.getInt32(), "UTC"/*context.getCurrentTimezone()*/);
    }

    private long getUnix(PValueSource source)
    {
        return source.getInt32() * 1000L; // unix
    }
    
    // -------- static members --------------
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
    
    private static void doMonthSubtraction (long d1[], long d2[], long divisor, PValueTarget out)
    {
        if (d1 == null || d2 == null)
        {
            out.putNull();
            return;
        }
        long ret = (d1[0] - d2[0]) * 12 + d1[1] - d2[1];

        // adjust the day difference
        if (ret > 0 && d1[2] < d2[2]) --ret;
        else if (ret < 0 && d1[2] > d2[2]) ++ret;

        out.putInt64(ret / divisor);
    }

}
