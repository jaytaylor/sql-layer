
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.*;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class MLog extends TScalarBase
{
    private final TClass argType;
    public MLog(TClass argType)
    {
        this.argType = argType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(argType, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        double base = inputs.get(0).getDouble();
        double value = inputs.get(1).getDouble();
        if (0 < value && 0 < base && 1 != base)
            output.putDouble(Math.log(value)/Math.log(base));
        else
            output.putNull();
    }

    @Override
    public String displayName()
    {
        return "LOG";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(argType);
    }
}
