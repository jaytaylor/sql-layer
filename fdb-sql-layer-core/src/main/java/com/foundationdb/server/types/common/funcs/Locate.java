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
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public abstract class Locate extends TScalarBase
{
    public static TScalar create2ArgOverload(final TClass stringType, final TClass intType, String name)
    {
        return new Locate(intType, name)
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(stringType, 0).covers(stringType, 1);
            }
        };
    }
    
    public static TScalar create3ArgOverload(final TClass stringType, final TClass intType, String name)
    {
        return new Locate(intType, name)
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(stringType, 0).covers(stringType, 1).covers(intType, 2);
            }
        };
    }
    
    private final TClass intType;
    private final String name;
    
    Locate(TClass intType, String name)
    {
        this.intType = intType;
        this.name = name;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        String str = inputs.get(1).getString();
        String substr = inputs.get(0).getString();

        int index = 0;
        if (inputs.size() == 3)
        {
            index = inputs.get(2).getInt32() - 1; // mysql uses 1-based indexing
            // invalid index => return 0 as the result
            if (index < 0 || index > str.length())
            {
                output.putInt32(0);
                return;
            }
        }
        output.putInt32(1 + str.indexOf(substr, index));
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(intType);
    }
}

