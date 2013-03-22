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

package com.akiban.server.types3.texpressions.std;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TInstanceGenerator;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class NoArgExpression extends TScalarBase
{
    public abstract void evaluate(TExecutionContext context, PValueTarget target);

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
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
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
