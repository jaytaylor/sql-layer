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
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class MPeriodArith extends TScalarBase {

    private final String name;
    
    public static final TScalar[] INSTANCES = {
        new MPeriodArith("PERIOD_ADD") {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                int period = inputs.get(0).getInt32();
                int offsetMonths = inputs.get(1).getInt32();

                // COMPATIBILITY: MySQL currently has undefined behavior for negative numbers
                // Our behavior follows our B.C. year numbering (-199402 + 1 = -199401)
                // Java's mod returns negative numbers: -1994 % 100 = -94
                int periodInMonths = fromPeriod(period);
                int totalMonths = periodInMonths + offsetMonths;

                // Handle the case where the period changes sign (e.g. -YYMM to YYMM)
                // as a result of adding. Since 0000 months is not really a date,
                // this leads to an off by one error
                if (Long.signum(periodInMonths) * Long.signum(totalMonths) == -1) {
                    totalMonths -= Long.signum(totalMonths) * 2;
                }

                int result = toPeriod(totalMonths);
                output.putInt32(result);
            }
        },
        new MPeriodArith("PERIOD_DIFF") {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                // COMPATIBILITY: MySQL currently has undefined behavior for negative numbers
                // Our behavior follows our B.C. year numbering (-199402 + 1 = -199401)
                int periodLeft = inputs.get(0).getInt32();
                int periodRight = inputs.get(1).getInt32();

                int result = fromPeriod(periodLeft) - fromPeriod(periodRight);
                output.putInt32(result);
            }
        }
    };
            
    private MPeriodArith(String name) {
        this.name = name;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MNumeric.INT, 0, 1);
    }

    @Override
    public String displayName() {
        return name;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.INT.instance());
    }

    // Helper functions
    // Takes a period and returns the number of months from year 0
    protected static int fromPeriod(int period)
    {
        int periodSign = Long.signum(period);
        
        int rawMonth = period % 100;
        int rawYear = period / 100;

        int absValYear = Math.abs(rawYear);
        if (absValYear < 70)
            rawYear += periodSign * 2000;
        else if (absValYear < 100)
            rawYear += periodSign * 1900; 
        
        return (rawYear * 12 + rawMonth - (1 * periodSign));
    }
    
    // Create a YYYYMM format from a number of months
    protected static int toPeriod(int monthCount) {
        int year = monthCount / 12;
        int month = (monthCount % 12) + 1 * Long.signum(monthCount);
        return (year * 100) + month;
    }
    
    // End helper functions ***************************************************
}
