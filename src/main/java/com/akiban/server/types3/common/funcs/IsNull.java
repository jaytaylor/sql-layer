
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class IsNull extends TScalarBase
{
    public static TScalar create(TClass boolType)
    {
        return new IsNull(boolType);
    }

    private final TClass boolType;
    
    private IsNull(TClass boolType)
    {
        this.boolType = boolType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(null, 0);
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) 
    {
        output.putBool(inputs.get(0).isNull());
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        assert false : "should have called evaluate";
    }

    @Override
    public String displayName()
    {
        return "IsNull";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(boolType);
    }
}
