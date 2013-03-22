
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.*;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

import java.util.Random;

public abstract class Rand extends TScalarBase {

    private static final int RAND_INDEX = 0; // the cached Random Object's index

    public static TScalar[] create(TClass inputType, TClass resultType)
    {
        return new TScalar[]
        {
            new Rand(inputType, resultType)
            {
                @Override
                protected Random getRandom(LazyList<? extends PValueSource> inputs)
                {
                    return new Random();
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    // does nothing. Takes 0 arg
                }
            },
            new Rand(inputType, resultType)
            {
                @Override
                protected Random getRandom(LazyList<? extends PValueSource> inputs)
                {
                    PValueSource input = inputs.get(0);
                    if (input.isNull())
                        return new Random();
                    else
                        return new Random(inputs.get(0).getInt64());
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    builder.covers(inputType, 0);
                }
            }
        };
    }
    
    protected abstract  Random getRandom (LazyList<? extends PValueSource> inputs);
    protected final TClass inputType;
    protected final TClass resultType;

    protected Rand(TClass inputType, TClass resultType) {
        if (inputType.underlyingType() != PUnderlying.INT_64
                || resultType.underlyingType() != PUnderlying.DOUBLE)
            throw new IllegalArgumentException("Wrong types");
        this.inputType = inputType;
        this.resultType = resultType;
    }

    @Override
    protected boolean nullContaminates(int inputIndex)
    {
        return false;
    }
    
    @Override
    protected boolean neverConstant()
    {
        return true;
    }
    
    @Override
    public String displayName() {
        return "RAND";
    }

    @Override
    public String[] registeredNames() {
        return new String[] { "rand", "random" };
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(resultType);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget out)
    {
        Random rand;
        if (context.hasExectimeObject(RAND_INDEX))
            rand = (Random) context.exectimeObjectAt(RAND_INDEX);
        else
            context.putExectimeObject(RAND_INDEX, rand = getRandom(inputs));

        out.putDouble(rand.nextDouble());
    }
}
