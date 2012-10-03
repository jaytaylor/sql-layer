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
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

import java.util.List;

public abstract class Substring extends TScalarBase
{
    public static TScalar[] create(TClass strType, TClass intType)
    {
        return new TScalar[]
        {
            new Substring(strType, intType, new int[] {1}) // 2 args: SUBSTR(<STRING>, <OFFSET>)
            {
                @Override
                protected int getLength(LazyList<? extends PValueSource> inputs)
                {
                    // length is this string's lenght
                    return inputs.get(0).getString().length();
                }   
            },
            new Substring(strType, intType, new int[] {1, 2}) // 3 args: SUBSTR(<STRING>, <OFFSET>, <LENGTH>)
            {
                @Override
                protected int getLength(LazyList<? extends PValueSource> inputs)
                {
                    return inputs.get(2).getInt32();
                }   
            },
            
        };
    }
    
    protected abstract int getLength (LazyList<? extends PValueSource> inputs);
    
    private final TClass strType;
    private final TClass intType;
    private final int covering[];
    
    private Substring(TClass strType, TClass intType, int covering[])
    {
        this.strType = strType;
        this.intType = intType;
        this.covering = covering;
    }

    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(strType, 0).covers(intType, covering);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        output.putString(getSubstr(inputs.get(0).getString(),
                                   inputs.get(1).getInt32(),
                                   getLength(inputs)),
                         null);
    }

    @Override
    public String displayName()
    {
        return "SUBSTRING";
    }

    @Override
    public String[] registeredNames()
    {
        return new String[] {"SUBSTRING", "SUBSTR"};
    }
    
    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                int strLength = inputs.get(0).instance().attribute(StringAttribute.LENGTH);

                // SUBSTR (<STRING> , <OFFSET>[, <LENGTH>]
                
                // check if <LENGTH> is available
                int length = strLength;
                PValueSource lenArg;
                if (inputs.size() == 3 && (lenArg = inputs.get(2).value()) != null
                                       && !lenArg.isNull())
                    length = lenArg.getInt32();
                
                return MString.VARCHAR.instance(length > strLength ? strLength : length);
            }
        });
    }
    
    private static String getSubstr(String st, int from, int length)
    {
        // if str is empty or <from> and <length> is outside of reasonable index
        // 
        // Note negative index is acceptable for <from>, but its absolute value has
        // to be within [1, str.length] (mysql index starts at 1)
        if (st.isEmpty() || from == 0 || Math.abs(from) > st.length() || length <= 0)
            return "";
        
        // if from is negative, start from the end,
        // and adjust the index by 1
        from += (from < 0 ? st.length() : -1);
       
        // TO operand
        int to = from + length;
        to = (to <= st.length() ? to : st.length());
        
        return st.substring(from, to);
    }
}
