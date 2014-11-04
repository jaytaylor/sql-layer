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
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

/**
 * <p>A function for describing the prepare-time type information of other expression. If/when we divide bundles into
 * modular packages, this should go into a testing or debug package. Its primary purpose, at least for now, is to verify
 * in our yaml tests that expressions have the right type.</p>
 *
 * <p>The usage is <code>DESCRIBE_EXPRESSION(<i>expr</i>)</code>, and the result is a constant {@code VARCHAR(255)} which
 * describes the TInstance and constantness of <i>expr</i>.</p>
 */
public final class DescribeExpression extends TScalarBase {

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(null, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        String result = context.inputTypeAt(0).toString();
        ValueSource input = inputs.get(0);
        result = ( (input== null) ? "variable " : "const ") + result;
        output.putString(result, null);
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    protected Constantness constness(TPreptimeContext context, int inputIndex, LazyList<? extends TPreptimeValue> values) {
        return Constantness.CONST;
    }

    @Override
    protected boolean allowNonConstsInEvaluation() {
        return true;
    }

    @Override
    public String displayName() {
        return "describe_expression";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(varchar, 255);
    }

    private final TClass varchar;

    public DescribeExpression(TClass varchar) {
        this.varchar = varchar;
    }
}
