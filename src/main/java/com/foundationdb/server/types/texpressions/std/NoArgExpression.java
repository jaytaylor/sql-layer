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

package com.foundationdb.server.types.texpressions.std;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public abstract class NoArgExpression extends TScalarBase
{
    public abstract void evaluate(TExecutionContext context, ValueTarget target);

    public boolean constantPerPreparation()
    {
        return constPerPrep;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        // does nothing. (no input)
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        assert inputs.size() == 0 : "unexpected input";
        evaluate(context, output);
    }

    @Override
    protected boolean neverConstant() {
        return true;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(resultTClass(), resultAttrs());
    }

    protected int[] resultAttrs() {
        return NO_ATTRS;
    }

    protected abstract TClass resultTClass();

    public NoArgExpression(String name, boolean constPerPrep)
    {
        this.name = name;
        this.constPerPrep = constPerPrep;
    }
    
    private final String name;
    private boolean constPerPrep;
    private static final int[] NO_ATTRS = new int[0];
}
