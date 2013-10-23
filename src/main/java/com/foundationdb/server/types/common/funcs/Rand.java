/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

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
                protected Random getRandom(LazyList<? extends ValueSource> inputs)
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
                protected Random getRandom(LazyList<? extends ValueSource> inputs)
                {
                    ValueSource input = inputs.get(0);
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
    
    protected abstract  Random getRandom (LazyList<? extends ValueSource> inputs);
    protected final TClass inputType;
    protected final TClass resultType;

    protected Rand(TClass inputType, TClass resultType) {
        if (inputType.underlyingType() != UnderlyingType.INT_64
                || resultType.underlyingType() != UnderlyingType.DOUBLE)
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
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget out)
    {
        Random rand;
        if (context.hasExectimeObject(RAND_INDEX))
            rand = (Random) context.exectimeObjectAt(RAND_INDEX);
        else
            context.putExectimeObject(RAND_INDEX, rand = getRandom(inputs));

        out.putDouble(rand.nextDouble());
    }
}
