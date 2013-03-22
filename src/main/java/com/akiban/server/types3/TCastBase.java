
package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;

public abstract class TCastBase implements TCast
{
        
    private final TClass sourceClass;
    private final TClass targetClass;
    private final Constantness constness;

    protected TCastBase(TClass sourceClass, TClass targetClass)
    {
        this(sourceClass, targetClass, Constantness.UNKNOWN);
    }

    protected TCastBase(TClass sourceClass, TClass targetClass, Constantness constness)
    {
        this.sourceClass = sourceClass;
        this.targetClass = targetClass;
        this.constness = constness;
    }

    @Override
    public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
        if (source.isNull())
            target.putNull();
        else
            doEvaluate(context, source, target);
    }

    @Override
    public TInstance preferredTarget(TPreptimeValue source) {
        return targetClass().instance(source.isNullable()); // you may want to override this, especially for varchars
    }

    protected abstract void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target);

    @Override
    public Constantness constness()
    {
        return constness;
    }

    @Override
    public TClass sourceClass()
    {
        return sourceClass;
    }

    @Override
    public TClass targetClass()
    {
        return targetClass;
    }

    @Override
    public String toString() {
        return sourceClass + "->" + targetClass;
    }
}
