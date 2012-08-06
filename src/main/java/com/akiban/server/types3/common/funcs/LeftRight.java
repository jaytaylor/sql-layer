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
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;

public abstract class LeftRight extends TOverloadBase
{
    public static TOverload getLeft(TClass stringType, TClass intType)
    {
        return new LeftRight(stringType, intType, "LEFT")
        {

            @Override
            String getSubstring(String st, int length)
            {
                return st.substring(0, length);
            }
            
        };
    }

    public static TOverload getRight(TClass stringType, TClass intType)
    {
        return new LeftRight(stringType, intType, "RIGHT")
        {
            @Override
            String getSubstring(String st, int length)
            {
                return st.substring(st.length() - length, length);
            }
        };
    }
    
    abstract String getSubstring(String st, int length);
    
    private final TClass stringType;
    private final TClass intType;
    private final String name;
    private LeftRight (TClass stringType, TClass intType, String name)
    {
        this.stringType = stringType;
        this.intType = intType;
        this.name = name;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(stringType, 0).covers(intType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String st = inputs.get(0).getString();
        int len = inputs.get(1).getInt32();

        // adjust the length
        len = len < 0 
                ? 0
                : len > st.length() ? st.length() : len;

        output.putString(getSubstring(st, len), null);
    }
    
    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TPreptimeValue len = inputs.get(1);
                
                // if second argument is not available or is null
                if (len.value() == null || len.value().isNull())
                {
                    TPreptimeValue st = inputs.get(0);
                    
                    // if the string is also not available
                    // the return the precision of the string's type
                    if (st.value() == null || st.value().isNull())
                        return st.instance();
                    else // if the string is available, return its length
                        return stringType.instance((st.value().getString()).length());
                }
                else
                    return stringType.instance(len.value().getInt32());
            }
            
        });
    }
    
}
