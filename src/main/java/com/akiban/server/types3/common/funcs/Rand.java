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

package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.*;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public abstract class Rand extends TScalarBase {

    private static final int RAND_INDEX = 0; // the cached Random Object's index
    private static final int DELTA = 100;
    private static final AtomicLong counter = new AtomicLong(DELTA);

    public static TScalar[] create(TClass inputType, TClass resultType)
    {
        return new TScalar[]
        {
            new Rand(inputType, resultType)
            {
                @Override
                protected Random getRandom(LazyList<? extends PValueSource> inputs)
                {
                    return new Random(System.currentTimeMillis() + counter.getAndAdd(DELTA));
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
                        return new Random(System.currentTimeMillis() + counter.getAndAdd(DELTA));
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
