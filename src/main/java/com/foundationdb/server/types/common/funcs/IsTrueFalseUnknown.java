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

public abstract class IsTrueFalseUnknown extends TScalarBase
{
    public static TScalar[] create (TClass boolType)
    {
        return new TScalar[]
        {
            new IsTrueFalseUnknown(boolType, "isTrue")
            {
                @Override
                protected void evaluate(ValueSource source, ValueTarget target)
                {
                    target.putBool(source.getBoolean(false));
                }
            },
            new IsTrueFalseUnknown(boolType, "isFalse")
            {
                @Override
                protected void evaluate(ValueSource source, ValueTarget target)
                {
                    target.putBool(!source.getBoolean(true));
                }
            },
            new IsTrueFalseUnknown(boolType, "isUnknown")
            {
                @Override
                protected void evaluate(ValueSource source, ValueTarget target)
                {
                    target.putBool(source.isNull());
                }
            }
        };
    }
   
    protected abstract void evaluate(ValueSource source, ValueTarget target);
    
    private final TClass boolType;
    private final String name;
    
    private IsTrueFalseUnknown(TClass boolType, String name)
    {
        this.boolType = boolType;
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(boolType, 0);
    }

     
    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        evaluate(inputs.get(0), output);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        // DOES NOTHING
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(boolType);
    }
}
