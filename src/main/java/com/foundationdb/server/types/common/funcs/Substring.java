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

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

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
                protected int getLength(LazyList<? extends ValueSource> inputs)
                {
                    // length is this string's lenght
                    return inputs.get(0).getString().length();
                }   
            },
            new Substring(strType, intType, new int[] {1, 2}) // 3 args: SUBSTR(<STRING>, <OFFSET>, <LENGTH>)
            {
                @Override
                protected int getLength(LazyList<? extends ValueSource> inputs)
                {
                    return inputs.get(2).getInt32();
                }   
            },
            
        };
    }
    
    protected abstract int getLength (LazyList<? extends ValueSource> inputs);
    
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
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
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
                int strLength = inputs.get(0).instance().attribute(StringAttribute.MAX_LENGTH);

                // SUBSTR (<STRING> , <OFFSET>[, <LENGTH>]
                
                // check if <LENGTH> is available
                int length = strLength;
                ValueSource lenArg;
                if (inputs.size() == 3 && (lenArg = inputs.get(2).value()) != null
                                       && !lenArg.isNull())
                    length = lenArg.getInt32();
                
                return MString.VARCHAR.instance(length > strLength ? strLength : length, anyContaminatingNulls(inputs));
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
