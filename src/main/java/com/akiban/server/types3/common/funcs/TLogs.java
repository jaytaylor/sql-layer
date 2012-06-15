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
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

public class TLogs extends TOverloadBase
{
    static final double ln2 = Math.log(2);
    
    public static TLogs[] create(TInstance ins)
    {
        LogType values[] = LogType.values();
        TLogs ret[] = new TLogs[values.length];
        
        for (int n = 0; n < ret.length; ++n)
            ret[n] = new TLogs(values[n], ins);
        return ret;
    }

    private static final int TWO_ARGS[] = new int[]{0, 1};
    private static final int ONE_ARG[] = new int[]{0};
    static enum LogType
    {
        LN()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.log(inputs.get(0).getDouble());
            }
        },
        LOG10()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.log10(inputs.get(0).getDouble());
            }
        },  
        LOG2()
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                return Math.log(inputs.get(0).getDouble())/ln2;
            }
        }, 
        LOG(TWO_ARGS)
        {
            @Override
            double evaluate(LazyList<? extends PValueSource> inputs)
            {
                if (inputs.size() == 2)
                    return Math.log(inputs.get(1).getDouble())/Math.log(inputs.get(0).getDouble());
                else
                    return Math.log(inputs.get(0).getDouble());
            }
            
            @Override
            boolean isValid(LazyList<? extends PValueSource> inputs)
            {
                if (inputs.size() == 2)
                    return Math.min(inputs.get(1).getDouble(), inputs.get(0).getDouble()-1) > 0;
                else
                    return inputs.get(0).getDouble() > 0;
            }
            
        };
        
        abstract double evaluate(LazyList<? extends PValueSource> inputs);
        private LogType(int c[])
        {
            covering = c;
        }
        
        private LogType()
        {
            covering = ONE_ARG;
        }
        public final int covering[];
        
        boolean isValid(LazyList<? extends PValueSource> inputs)
        {
            return inputs.get(0).getDouble() > 0;
        }
    }

    private final LogType logType;
    private final TInstance argType;
    
    TLogs (LogType logType, TInstance argType)
    {
        this.logType = logType;
        this.argType = argType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.pickingCovers(argType.typeClass(), logType.covering);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        if (logType.isValid(inputs))
            output.putDouble(logType.evaluate(inputs));
        else
            output.putNull();
    }

    @Override
    public String overloadName()
    {
        return logType.name();
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(argType);
    }
}
