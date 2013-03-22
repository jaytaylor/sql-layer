
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class MSecToTime extends TScalarBase
{
    public static final TScalar INSTANCE = new MSecToTime();
    
    private MSecToTime() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder) 
    {
        builder.covers(MNumeric.BIGINT, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) 
    {
        long time = inputs.get(0).getInt64();
        int mul;
        
        if (time < 0)
            time *= mul = -1;
        else
            mul = 1;

        long hour = time / 3600 * mul;
        long min = time / 60;
        long sec = time % 60;

        output.putInt32(MDatetimes.encodeTime(hour, min, sec, context));
    }

    @Override
    public String displayName() {
        return "SEC_TO_TIME";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MDatetimes.TIME);
    }
}
