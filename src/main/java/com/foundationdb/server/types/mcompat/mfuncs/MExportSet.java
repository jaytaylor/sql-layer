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

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.math.BigInteger;
import java.util.List;

public abstract class  MExportSet extends TScalarBase
{
    public static final TScalar INSTANCES[]
            = createOverloads(MNumeric.INT, MString.VARCHAR, MNumeric.BIGINT_UNSIGNED);
    
    private static final int DEFAULT_LENGTH = 64;
    private static final String DEFAULT_DELIM = ",";
    
    public static TScalar[] createOverloads(final TClass intType, final TClass stringType, final TClass uBigintType)
    {
        return new TScalar[]
        {
            new MExportSet(stringType) // 3 args case
            {
                @Override
                protected String getDelimeter(LazyList<? extends ValueSource> inputs)
                {
                    return DEFAULT_DELIM;
                }

                @Override
                protected int getLength(LazyList<? extends ValueSource> inputs)
                {
                    return DEFAULT_LENGTH;
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    builder.covers(uBigintType, 0).covers(stringType, 1, 2);
                }
                
            },
            new MExportSet(stringType) // 4 args case
            {

                @Override
                protected String getDelimeter(LazyList<? extends ValueSource> inputs)
                {
                    return inputs.get(3).getString();
                }

                @Override
                protected int getLength(LazyList<? extends ValueSource> inputs)
                {
                    return DEFAULT_LENGTH;
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    builder.covers(uBigintType, 0).covers(stringType, 1, 2, 3);
                }
                
            },
            new MExportSet(stringType) // 5 arg case
            {

                @Override
                protected String getDelimeter(LazyList<? extends ValueSource> inputs)
                {
                    return inputs.get(3).getString();
                }

                @Override
                protected int getLength(LazyList<? extends ValueSource> inputs)
                {
                    int length = inputs.get(4).getInt32();
                    if( length < 0) {
                        return DEFAULT_LENGTH;
                    } else {
                        return Math.min(DEFAULT_LENGTH, inputs.get(4).getInt32());
                    }
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    builder.covers(uBigintType, 0).covers(stringType, 1, 2, 3).covers(intType, 4);
                }
            }
        };
    }
            
    private static String computeSet(long num, String bits[], String delim, int length)
    {
        String digits = Long.toBinaryString(num);
        int count = 0;
        StringBuilder ret = new StringBuilder();
        
        // return value is in little-endian format
        for (int n = digits.length() - 1; n >= 0 && count < length; --n, ++count)
            ret.append(bits[digits.charAt(n) - '0']).append(delim);
        
        // fill the rest with 'off'
        for (; count < length; ++count)
            ret.append(bits[0]).append(delim);
        if (!delim.isEmpty()) // delete the last delimiter
            return ret.substring(0, ret.length() - delim.length());
        return ret.toString();
    }

    protected abstract String getDelimeter(LazyList<? extends ValueSource> inputs);
    protected abstract int getLength(LazyList<? extends ValueSource> inputs);

    private final TClass stringType;
    private MExportSet(TClass stringType)
    {
        this.stringType = stringType;
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        String s = computeSet(
                inputs.get(0).getInt64(),
                new String[]{inputs.get(2).getString(), inputs.get(1).getString()},
                getDelimeter(inputs),
                getLength(inputs));
        output.putString(s, null);
    }
    
    @Override
    public String displayName()
    {
        return "EXPORT_SET";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TPreptimeValue on = inputs.get(1);
                TPreptimeValue off = inputs.get(2);

                boolean nullable = anyContaminatingNulls(inputs);
                
                if (on == null 
                        || off == null
                        || on.value().isNull()
                        || off.value().isNull()
                   )
                    return stringType.instance(255, nullable); // if not literal, the length would just be 255
                
                // get the digits length
                int digitLength = Math.max((on.value().getString()).length(), 
                                            (off.value().getString()).length());
                int length = DEFAULT_LENGTH; // number of digits
                int delimLength = DEFAULT_DELIM.length();
                
                switch(inputs.size())
                {
                    case 5:     
                        if (inputs.get(4) != null && !inputs.get(4).value().isNull())
                            length = inputs.get(4).value().getInt32();  // fall thru
                    case 4:
                        if (inputs.get(3) != null && !inputs.get(3).value().isNull())
                            delimLength = (inputs.get(3).value().getString()).length();
                }
                // There would only be [length - 1] number of delimiter characters
                // in the string. But we'd give it enough space for [length]
                return stringType.instance(length * (digitLength + delimLength), nullable);
            }
            
        });
    }
}
