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
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;

public abstract class Pad extends TOverloadBase
{
    public static TOverload[] create(TClass stringType, TClass intType)
    {
        return new TOverload[]
        {
            new Pad(stringType, intType, "LPAD") // prepend
            {
                @Override
                String doPadding(String st, int times, String toAdd)
                {
                    StringBuilder prefix = new StringBuilder();                    
                    int delta = times - st.length();
                    int limit = delta / toAdd.length();
                    int remain = delta % toAdd.length();

                    while (limit-- > 0)
                        prefix.append(toAdd);
                    for (int n = 0; n < remain; ++n)
                        prefix.append(toAdd.charAt(n));
                    
                    return prefix.append(st).toString();
                }   
            },
            new Pad(stringType, intType, "RPAD") // append
            {
                @Override
                String doPadding(String st, int times, String toAdd)
                {
                    StringBuilder ret = new StringBuilder(st);
                    int delta = times - st.length();
                    int limit = delta / toAdd.length();
                    int remain = delta % toAdd.length();
                    
                    while (limit-- > 0)
                        ret.append(toAdd);
                    for (int n = 0; n < remain; ++n)
                        ret.append(toAdd.charAt(n));

                    return ret.toString();
                }
            }
        };
    }

    abstract String doPadding(String st, int times, String toAdd);
    
    private final TClass stringType;
    private final TClass intType;
    private final String name;
    
    Pad(TClass stringType, TClass intType, String name)
    {
        this.stringType = stringType;
        this.intType = intType;
        this.name = name;
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String st = (String) inputs.get(0).getObject();
        int length = inputs.get(1).getInt32();
        String toAdd = (String) inputs.get(2).getObject();
        
        if (length < 0)
            output.putNull();
        else if (length <= st.length())
            output.putObject(st.substring(0, length));
        else if (toAdd.isEmpty())
            output.putNull();
        else
            output.putObject(doPadding(st, length, toAdd));
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(stringType, 0, 2).covers(intType, 1);
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
                PValueSource len = inputs.get(1).value();
                
                // if the argument isn't availabe
                // return LONGTEXT 
                if (len == null)
                    throw new UnsupportedOperationException("LONGTEXT type is not available");
                else if (len.isNull())
                    return stringType.instance(0);
                else
                    return stringType.instance(len.getInt32());
            }
        });
    }
    
}
