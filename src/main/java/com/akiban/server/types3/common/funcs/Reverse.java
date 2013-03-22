
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class Reverse extends TScalarBase
{
    private final TClass stringType;
    public Reverse (TClass stringType)
    {
        this.stringType = stringType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.pickingCovers(stringType, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        output.putString(new StringBuilder(inputs.get(0).getString()).reverse().toString(), null);
    }

    @Override
    public String displayName()
    {
        return "REVERSE";
    }

    @Override
    public TOverloadResult resultType()
    { 
        return TOverloadResult.picking();  
    }
}
