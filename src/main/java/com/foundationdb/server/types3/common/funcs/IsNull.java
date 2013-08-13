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
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class IsNull extends TScalarBase
{
    public static TScalar create(TClass boolType)
    {
        return new IsNull(boolType);
    }

    private final TClass boolType;
    
    private IsNull(TClass boolType)
    {
        this.boolType = boolType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(null, 0);
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) 
    {
        output.putBool(inputs.get(0).isNull());
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        assert false : "should have called evaluate";
    }

    @Override
    public String displayName()
    {
        return "IsNull";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(boolType);
    }
}
