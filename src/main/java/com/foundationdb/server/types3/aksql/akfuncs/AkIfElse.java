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

package com.akiban.server.types3.aksql.akfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class AkIfElse extends TScalarBase
{
    public static final TScalar INSTANCE = new AkIfElse();
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(AkBool.INSTANCE, 0).pickingCovers(null, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {

        int whichSource = inputs.get(0).getBoolean() ? 1 : 2;
        PValueSource source = inputs.get(whichSource);
        PValueTargets.copyFrom(source,  output);
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return inputIndex == 0;
    }

    @Override
    public String displayName()
    {
        return "IF";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.picking();
    }

    private AkIfElse() {}
}
