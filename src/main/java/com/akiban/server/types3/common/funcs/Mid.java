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
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

import java.util.List;

public class Mid extends TScalarBase
{
    private final TClass strType;
    private final TClass int32Type;
    
    public Mid (TClass strType, TClass int32Type)
    {
        this.strType = strType;
        this.int32Type = int32Type;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(strType, 0).covers(int32Type, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String st = inputs.get(0).getString();
        if (st.isEmpty())
        {
            output.putString("", null);
            return;
        }

        // starting index
        int from = inputs.get(1).getInt32();
        if (from == 0)
        {
            output.putString("", null);
            return;
        }

        // if index is negative, start from the end, and adjust
            // index by 1 since index in sql starts at 1 NOT 0
        from += (from < 0?  st.length()  : -1);

        // if from is still neg, return empty string
        if (from < 0)
        {
            output.putString("", null);
            return;
        } 

        // turn the LENGTH operand into 
        // _to_ index
        int to = from + inputs.get(2).getInt32() - 1;

        // if to <= from => return empty
        if (to < from || from >= st.length())
        {
            output.putString("", null);
            return;
        }            
        to = (to > st.length() -1 ? st.length() -1 : to);

        output.putString(st.substring(from, to + 1), null);
    }

    @Override
    public String displayName()
    {
        return "MID";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TPreptimeValue length = inputs.get(2);
                
                // if third argument (_length_) is not available
                // return string with default length
                if (length == null || length.value() == null || length.value().isNull())
                    return strType.instance(anyContaminatingNulls(inputs));

                // evalue the constant
                return strType.instance(length.value().getInt32(), anyContaminatingNulls(inputs));
            }
        });
    }
    
}
