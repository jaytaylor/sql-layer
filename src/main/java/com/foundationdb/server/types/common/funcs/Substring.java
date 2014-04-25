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
                protected Integer getLength(LazyList<? extends ValueSource> inputs)
                {
                    return null;
                }   
            },
            new Substring(strType, intType, new int[] {1, 2}) // 3 args: SUBSTR(<STRING>, <OFFSET>, <LENGTH>)
            {
                @Override
                protected Integer getLength(LazyList<? extends ValueSource> inputs)
                {
                    return inputs.get(2).getInt32();
                }   
            },
            
        };
    }
    
    protected abstract Integer getLength (LazyList<? extends ValueSource> inputs);
    
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
        return new String[] {"SUBSTRING", "SUBSTR", "MID"};
    }
    
    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                int strLength = inputs.get(0).type().attribute(StringAttribute.MAX_LENGTH);
                // usage: SUBSTR (<STRING> , <OFFSET> [, <LENGTH>] )
                int length = strLength;
                ValueSource lenArg;
                // check if <LENGTH> is available
                if (inputs.size() == 3 && (lenArg = inputs.get(2).value()) != null
                                       && !lenArg.isNull()) {
                    length = lenArg.getInt32();
                }
                return strType.instance(length > strLength ? strLength : length, anyContaminatingNulls(inputs));
            }
        });
    }
    
    private static String getSubstr(String st, int from, Integer length) {
        // if str is empty or <from> and <length> is outside of reasonable index
        // 
        // Note negative index is acceptable for <from>, but its absolute value has
        // to be within [1, str.length] (mysql index starts at 1)
        if (st.isEmpty() || from == 0 || (length != null && length <= 0)) {
            return "";
        }
        try {
            if (from < 0) {
                from = st.offsetByCodePoints(st.length(), from);
            } else {
                from = st.offsetByCodePoints(0, from - 1);
            }
        }
        catch (IndexOutOfBoundsException ex) {
            return "";
        }
        if (length == null) {
            return st.substring(from);
        }
        int to;
        try {
            to = st.offsetByCodePoints(from, length);
        } catch (IndexOutOfBoundsException ex) {
            to = st.length();
        }
        return st.substring(from, to);
    }
}
