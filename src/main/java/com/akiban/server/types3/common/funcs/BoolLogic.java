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

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

public class BoolLogic extends TOverloadBase
{
    public static final TOverload INSTANCES[] = new TOverload[Op.values().length];
    static
    {
        Op op[] = Op.values();
        for (int n = 0; n <  op.length; ++n)
            INSTANCES[n] = new BoolLogic(op[n]);
    }

    private static enum Op
    {
        AND(new int[]{0, 1})
        {
            @Override
            boolean evaluate(LazyList<? extends PValueSource> inputs)
            {
                return inputs.get(0).getBoolean() && inputs.get(1).getBoolean();
            }
        }, 
        OR(AND.coverage)
        {
            @Override
            boolean evaluate(LazyList<? extends PValueSource> inputs)
            {
                return inputs.get(0).getBoolean() || inputs.get(1).getBoolean();
            }
        }, 
        XOR(AND.coverage)
        {
            @Override
            boolean evaluate(LazyList<? extends PValueSource> inputs)
            {
                return inputs.get(0).getBoolean() ^ inputs.get(1).getBoolean();
            }
        }, 
        NOT(new int[]{0})
        {
            @Override
            boolean evaluate(LazyList<? extends PValueSource> inputs)
            {
                return !inputs.get(0).getBoolean();
            }
        };
        
        public final int[] coverage;
        private Op(int[] c)
        {
            coverage = c;
        }
        
        abstract boolean evaluate(LazyList<? extends PValueSource> inputs);
    }
    
    private final Op op;
    
    BoolLogic (Op op)
    {
        this.op = op;
    }
    
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        output.putBool(op.evaluate(inputs));
    }
    
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(AkBool.INSTANCE, op.coverage);
    }

    @Override
    public String displayName()
    {
        return op.name();
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(AkBool.INSTANCE);
    }
    
}
