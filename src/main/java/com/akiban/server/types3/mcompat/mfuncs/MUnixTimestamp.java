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
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class MUnixTimestamp extends TScalarBase {
    
    public static final TScalar[] INSTANCES =
    {
        new MUnixTimestamp()
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDatetimes.TIMESTAMP, 0);
            }
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
            {
                output.putInt32(inputs.get(0).getInt32());
            }            
        },
        new MUnixTimestamp() {
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MDatetimes.DATETIME, 0);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                long[] datetime = MDatetimes.decodeDatetime(inputs.get(0).getInt64());
                output.putInt32((int) MDatetimes.encodeTimestamp(datetime, context.getCurrentTimezone(), context));
            }
        },
        new MUnixTimestamp() {

            @Override
            protected void buildInputSets(TInputSetBuilder builder) 
            {
                // Does nothing (takes 0 input)
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
            {
                output.putInt32((int)MDatetimes.encodeTimetamp(context.getCurrentDate(), context));
            }
        }
    };

    @Override
    public String displayName() 
    {
        return "UNIX_TIMESTAMP";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.INT.instance());
    }
}
