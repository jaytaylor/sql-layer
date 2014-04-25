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
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.server.types.texpressions.Constantness;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class Elt extends TScalarBase
{

    private final TClass stringType;
    private final TClass intType;
    
    public Elt(TClass intType, TClass stringType)
    {
        this.intType = intType;
        this.stringType = stringType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        // ELT(<INT>, <T> ....)
        // argc >= 2
        builder.covers(intType, 0).pickingVararg(stringType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        int index = inputs.get(0).getInt32();
        int nvarargs = inputs.size();
        if (index < 1 || index >= nvarargs)
            output.putNull();
        else
            ValueTargets.copyFrom(inputs.get(index), output);
    }
     
    @Override
    protected boolean constnessMatters(int inputIndex) 
    {
        return true;
    }

    @Override
    protected Constantness constness(TPreptimeContext context, int inputIndex, LazyList<? extends TPreptimeValue> values) {
        assert inputIndex == 0 : inputIndex + " for " + values; // 0 should be enough to fully answer the question
        ValueSource indexVal = constSource(values, 0);
        if (indexVal == null)
            return Constantness.NOT_CONST;
        if (indexVal.isNull())
            return Constantness.CONST;
        int answerIndex = indexVal.getInt32();
        if (answerIndex < 1 || answerIndex >= values.size())
            return Constantness.CONST; // answer is null
        ValueSource answer = constSource(values, answerIndex);
        return answer == null ? Constantness.NOT_CONST : Constantness.CONST;
    }

    @Override
    protected boolean nullContaminates(int inputIndex) 
    {
        return inputIndex == 0;
    }

    @Override
    public String displayName()
    {
        return "ELT";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.picking();
    }
}
