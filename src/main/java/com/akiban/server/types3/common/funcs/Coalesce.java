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
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class Coalesce extends TScalarBase {

    public static final TScalar INSTANCE = new Coalesce();

    private Coalesce() {}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.pickingVararg(null, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        for (int i = 0; i < inputs.size(); ++i) {
            if (!inputs.get(i).isNull()) {
                PValueTargets.copyFrom(inputs.get(i), output);
                return;
            }
        }
        output.putNull();
    }

    @Override
    public String displayName() {
        return "COALESCE";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    protected Constantness constness(TPreptimeContext context, int inputIndex, LazyList<? extends TPreptimeValue> values) {
        PValueSource preptimeValue = constSource(values, inputIndex);
        if (preptimeValue == null)
            return Constantness.NOT_CONST;
        return preptimeValue.isNull() ? Constantness.UNKNOWN : Constantness.CONST;
    }
}
