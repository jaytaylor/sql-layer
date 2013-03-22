/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

public abstract class UpperLower extends TScalarBase
{
    public static TScalar[] create(TClass stringType)
    {
        return new TScalar[]
        {
            new UpperLower(stringType, "UPPER")
            {
                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
                {
                    output.putString((inputs.get(0).getString()).toUpperCase(), null);
                }

                @Override
                public String[] registeredNames()
                {
                    return new String[]{"ucase", "upper"};
                }
            },
            new UpperLower(stringType, "LOWER")
            {
                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
                {
                    output.putString((inputs.get(0).getString()).toLowerCase(), null);
                }

                @Override
                public String[] registeredNames()
                {
                    return new String[]{"lcase", "lower"};
                }
            }
        };
    }

    private final TClass stringType;
    private final String name;
    private UpperLower(TClass stringType, String name)
    {
        this.stringType = stringType;
        this.name = name;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.pickingCovers(stringType, 0);
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.picking();
    }
}
