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

import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.common.types.TString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

public class InetAton extends TOverloadBase
{
    private static final long FACTORS[] = {16777216L,  65536, 256};        
    
    private final TClass argType;
    private final TInstance returnType;
    
    public InetAton(TClass tclass, TInstance returnType)
    {
        assert tclass instanceof TString : "expecting a string class";
        this.argType = tclass;
        
        // TODO: assert returnType instaceof BIGINT ...
        this.returnType = returnType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(argType, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String tks[] = (inputs.get(0).getString()).split("\\.");
        if (tks.length > 4)
            output.putNull();
        else
            try
            {
                int last = tks.length - 1;
                short val = Short.parseShort(tks[last]);
                long ret = val;
                
                if (ret < 0 || ret > 255) output.putNull();
                else if (tks.length == 1) output.putInt64(ret);
                else
                {
                    for (int i = 0; i < last; ++i)
                        if ((val = Short.parseShort(tks[i])) < 0 || val > 255)
                        {
                            output.putNull();
                            return;
                        }
                        else
                            ret += val * FACTORS[i];
                    output.putInt64(ret);
                }
            }
            catch (NumberFormatException e)
            {
                context.warnClient(new InvalidCharToNumException(e.getMessage()));
                output.putNull();
            }
    }

    @Override
    public String displayName()
    {
        return "INET_ATON";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(returnType);
    }
}
