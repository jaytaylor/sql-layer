
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
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

import java.util.List;

public abstract class LeftRight extends TScalarBase
{
    public static TScalar getLeft(TClass stringType, TClass intType)
    {
        return new LeftRight(stringType, intType, "LEFT", "getLeft")
        {

            @Override
            String getSubstring(String st, int length)
            {
                return st.substring(0, length);
            }
            
        };
    }

    public static TScalar getRight(TClass stringType, TClass intType)
    {
        return new LeftRight(stringType, intType, "RIGHT", "getRight")
        {
            @Override
            String getSubstring(String st, int length)
            {
                return st.substring(st.length() - length, st.length());
            }
        };
    }
    
    abstract String getSubstring(String st, int length);
    
    private final TClass stringType;
    private final TClass intType;
    private final String name;
    private final String registeredName;
    
    private LeftRight (TClass stringType, TClass intType, String name, String regname)
    {
        this.stringType = stringType;
        this.intType = intType;
        this.name = name;
        this.registeredName = regname;
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
    public String[] registeredNames()
    {
        return new String[] {registeredName};
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
                        return st.instance().withNullable(false);
                    else // if the string is available, return its length
                        return stringType.instance((st.value().getString()).length(), anyContaminatingNulls(inputs));
                }
                else
                    return stringType.instance(len.value().getInt32(), anyContaminatingNulls(inputs));
            }
            
        });
    }
    
}
