
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class IsTrueFalseUnknown extends TScalarBase
{
    public static TScalar[] create (TClass boolType)
    {
        return new TScalar[]
        {
            new IsTrueFalseUnknown(boolType, "isTrue")
            {
                @Override
                protected void evaluate(PValueSource source, PValueTarget target)
                {
                    target.putBool(source.getBoolean(false));
                }
            },
            new IsTrueFalseUnknown(boolType, "isFalse")
            {
                @Override
                protected void evaluate(PValueSource source, PValueTarget target)
                {
                    target.putBool(!source.getBoolean(true));
                }
            },
            new IsTrueFalseUnknown(boolType, "isUnknown")
            {
                @Override
                protected void evaluate(PValueSource source, PValueTarget target)
                {
                    target.putBool(source.isNull());
                }
            }
        };
    }
   
    protected abstract void evaluate(PValueSource source, PValueTarget target);
    
    private final TClass boolType;
    private final String name;
    
    private IsTrueFalseUnknown(TClass boolType, String name)
    {
        this.boolType = boolType;
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(boolType, 0);
    }

     
    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) 
    {
        evaluate(inputs.get(0), output);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        // DOES NOTHING
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(boolType);
    }
}
