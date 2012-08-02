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

import com.akiban.server.error.OverflowException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

public class TTrigs extends TOverloadBase
{
    public static TTrigs[] create(TInstance ins)
    {
        TrigType values[] = TrigType.values();
        TTrigs ret[] = new TTrigs[values.length];
        
        for (int n = 0; n < ret.length; ++n)
            ret[n] = new TTrigs(values[n], ins);
        return ret;
    }

    private static final int TWO_ARGS[] = new int[]{0, 1};
    private static final int ONE_ARG[] = new int[]{0};
    static enum TrigType
    {
        SIN()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.sin(inputs.get(0).getDouble());
            }
        }, 
        COS()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.cos(inputs.get(0).getDouble());
            }
        },  
        TAN()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
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
            double evaluate(LazyList<? extends PValueSource> inputs)
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
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.asin(inputs.get(0).getDouble());
            }
        }, 
        ACOS()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.acos(inputs.get(0).getDouble());
            }
        }, 
        ACOT()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                double var = inputs.get(0).getDouble();
                if (Double.compare(var, 0) == 0)
                    return Math.PI / 2;
                return Math.atan(1 / var);
            }
        }, 
        ATAN(TWO_ARGS)
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.atan2(inputs.get(0).getDouble(),inputs.get(1).getDouble());
            }
        },
        ATAN2(TWO_ARGS)
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.atan2(inputs.get(0).getDouble(),inputs.get(1).getDouble());
            }
        }, 
        COSH()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.cosh(inputs.get(0).getDouble());
            }
        },
        SINH()  
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.sinh(inputs.get(0).getDouble());
            }
        },
        TANH()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.tanh(inputs.get(0).getDouble());
            }
        },
        COTH()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                double var = inputs.get(0).getDouble();
                if (Double.compare(var, 0) == 0)
                    throw new OverflowException();
                return Math.cosh(var) / Math.sinh(var);
            }
        };
        
        abstract double evaluate(LazyList<? extends PValueSource> inputs);
        private TrigType(int c[])
        {
            covering = c;
        }
        
        private TrigType()
        {
            covering = ONE_ARG;
        }
        public final int covering[];
    }

    private final TrigType trigType;
    private final TInstance argType;
    
    TTrigs (TrigType trigType, TInstance argType)
    {
        this.trigType = trigType;
        this.argType = argType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(argType.typeClass(), trigType.covering);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        output.putDouble(trigType.evaluate(inputs));
    }

    @Override
    public String displayName()
    {
        return trigType.name();
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(argType);
    }
}
