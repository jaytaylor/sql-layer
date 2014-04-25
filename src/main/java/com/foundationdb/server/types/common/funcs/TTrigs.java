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

import com.foundationdb.server.error.OverflowException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class TTrigs extends TScalarBase
{
    public static TTrigs[] create(TClass argType)
    {
        TrigType values[] = TrigType.values();
        TTrigs ret[] = new TTrigs[values.length];
        for (int n = 0; n < ret.length; ++n)
        {
            ret[n] = new TTrigs(values[n], argType);
        }
        return ret;
    }

    private static final int TWO_ARGS[] = new int[]{0, 1};
    private static final int ONE_ARG[] = new int[]{0};
    static enum TrigType
    {
        SIN()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                return Math.sin(inputs.get(0).getDouble());
            }
        }, 
        COS()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                return Math.cos(inputs.get(0).getDouble());
            }
        },  
        TAN()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                double var = inputs.get(0).getDouble();
                if (Double.compare(Math.cos(var), 0) == 0)
                    throw new OverflowException();
                return Math.tan(inputs.get(0).getDouble());
            }
        }, 
        COT()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                double var = inputs.get(0).getDouble();
                double sin = Math.sin(var);
                if (Double.compare(sin, 0) == 0)
                    throw new OverflowException();
                return Math.cos(var) / sin;
            }
        }, 
        ASIN()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                return Math.asin(inputs.get(0).getDouble());
            }
        }, 
        ACOS()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                return Math.acos(inputs.get(0).getDouble());
            }
        }, 
        ACOT()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                double var = inputs.get(0).getDouble();
                if (Double.compare(var, 0) == 0)
                    return Math.PI / 2;
                return Math.atan(1 / var);
            }
        }, 
        ATAN(ONE_ARG)
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                return Math.atan(inputs.get(0).getDouble());
            }
        },
        ATAN2(TWO_ARGS)
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                return Math.atan2(inputs.get(0).getDouble(),inputs.get(1).getDouble());
            }

            @Override
            public String[] allNames() {
                return new String[] { ATAN.name(), name() };
            }
        }, 
        COSH()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                return Math.cosh(inputs.get(0).getDouble());
            }
        },
        SINH()  
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                return Math.sinh(inputs.get(0).getDouble());
            }
        },
        TANH()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                return Math.tanh(inputs.get(0).getDouble());
            }
        },
        COTH()
        {
            @Override
            double evaluate(LazyList<? extends ValueSource> inputs)
            {
                double var = inputs.get(0).getDouble();
                if (Double.compare(var, 0) == 0)
                    throw new OverflowException();
                return Math.cosh(var) / Math.sinh(var);
            }
        };

        abstract double evaluate(LazyList<? extends ValueSource> inputs);

        public String[] allNames() {
            return new String[] { name() };
        }

        private TrigType() {
            this(ONE_ARG);
        }

        private TrigType(int covering[]) {
            this.covering = covering;
        }

        public final int[] covering;
    }

    private final TrigType trigType;
    private final TClass argType;
    
    private TTrigs(TrigType trigType, TClass argType)
    {
        this.trigType = trigType;
        this.argType = argType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(argType, trigType.covering);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        output.putDouble(trigType.evaluate(inputs));
    }

    @Override
    public String displayName()
    {
        return trigType.name();
    }

    @Override
    public String[] registeredNames() {
        return trigType.allNames();
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(argType);
    }
}
