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
        return TOverloadResult.fixed(MNumeric.INT);
    }
}
