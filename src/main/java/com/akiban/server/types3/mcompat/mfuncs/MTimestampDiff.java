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

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.common.UnitValue;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

public class MTimestampDiff extends TOverloadBase {
    
    public static final TOverload INSTANCE = new MTimestampDiff();

    private static final long[] MILLIS_DIV = new long[6];
    private static final long[] MONTH_DIV = {12L, 4L, 1L};
    private static final int MILLIS_BASE = UnitValue.WEEK;
    private static final int MONTH_BASE = UnitValue.YEAR;

    private MTimestampDiff() {
        int mul[] = {7, 24, 60, 60, 1000};

        MILLIS_DIV[5] = 1;
        for (int n = 4; n >= 0; --n) {
            MILLIS_DIV[n] = MILLIS_DIV[n + 1] * mul[n];
        }
    }
        
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MNumeric.INT, 0);
        builder.covers(MDatetimes.DATETIME, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        int val = inputs.get(0).getInt32();
        long datetime0 = inputs.get(1).getInt64();
        long datetime1 = inputs.get(2).getInt64();
        
        switch (val) {
            case UnitValue.YEAR: 
            case UnitValue.QUARTER:
            case UnitValue.MONTH: 
                long[] date0 = MDatetimes.decodeDatetime(datetime0);
                long[] date1 = MDatetimes.decodeDatetime(datetime1);
                
                output.putInt64(doSubtract(date0, date1) / MONTH_DIV[val - MONTH_BASE]);
                break;
            case UnitValue.WEEK:
            case UnitValue.DAY:
            case UnitValue.HOUR:
            case UnitValue.MINUTE:
            case UnitValue.SECOND:
                output.putInt64((datetime0 - datetime1) / MILLIS_DIV[val - MILLIS_BASE]);
                break;
        }
    }

    @Override
    public String overloadName() {
        return "TIMESTAMPDIFF";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.BIGINT.instance());
    }
    
    private static long doSubtract(long d1[], long d2[]) {
        if (!MDatetimes.isValidDatetime(d1)
                || !MDatetimes.isValidDatetime(d2)){
            throw new InvalidParameterValueException("Invalid date/time values");
        }

        long ret = (d1[0] - d2[0]) * 12 + d1[1] - d2[1];

        // adjust the day difference
        if (ret > 0 && d1[2] < d2[2]) {
            --ret;
        } else if (ret < 0 && d1[2] > d2[2]) {
            ++ret;
        }

        return ret;
    }
}
