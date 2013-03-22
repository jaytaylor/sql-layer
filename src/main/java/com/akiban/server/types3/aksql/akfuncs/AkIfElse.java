
package com.akiban.server.types3.aksql.akfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class AkIfElse extends TScalarBase
{
    public static final TScalar INSTANCE = new AkIfElse();
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(AkBool.INSTANCE, 0).pickingCovers(null, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {

        int whichSource = inputs.get(0).getBoolean() ? 1 : 2;
        PValueSource source = inputs.get(whichSource);
        PValueTargets.copyFrom(source,  output);
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return inputIndex == 0;
    }

    @Override
    public String displayName()
    {
        return "IF";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.picking();
    }

    private AkIfElse() {}
}
