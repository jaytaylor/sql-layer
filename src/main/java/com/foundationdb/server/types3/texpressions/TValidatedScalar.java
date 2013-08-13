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

package com.foundationdb.server.types3.texpressions;

import com.foundationdb.server.explain.*;
import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.ReversedLazyList;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInputSet;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.TPreptimeContext;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;

import java.util.List;

public final class TValidatedScalar extends TValidatedOverload implements TScalar {

    // TOverload methods (straight delegation)

    @Override
    public TPreptimeValue evaluateConstant(TPreptimeContext context, LazyList<? extends TPreptimeValue> inputs) {
        return scalar.evaluateConstant(context, inputs);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        scalar.finishPreptimePhase(context);
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        scalar.evaluate(context, inputs, output);
    }

    @Override
    public String toString(List<? extends TPreparedExpression> inputs, TInstance resultType) {
        return scalar.toString(inputs, resultType);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context, List<? extends TPreparedExpression> inputs, TInstance resultType)
    {
        return scalar.getExplainer(context, inputs, resultType);
    }

    public <T> LazyList<? extends T> filterInputs(LazyList<? extends T> inputs) {
        return commuted ? new ReversedLazyList<>(inputs) : inputs;
    }

    public TValidatedScalar createCommuted() {
        if (!coversExactlyNArgs(2))
            throw new IllegalStateException("commuted overloads must take exactly two arguments: " + this);
        TClass origArg1 = inputSetAt(1).targetType();
        TClass origArg0 = inputSetAt(0).targetType();
        if (origArg0 == origArg1)
            throw new IllegalStateException("two-arg overload has same target class for both operands, so commuting "
                    + "it makes no sense: " + this);

        TInputSetBuilder builder = new TInputSetBuilder();
        builder.covers(origArg1, 0);
        builder.covers(origArg0, 1);
        List<TInputSet> commutedInputSets = builder.toList();
        return new TValidatedScalar(scalar, commutedInputSets, true);
    }

    public TValidatedScalar(TScalar scalar) {
        this(scalar, scalar.inputSets(), false);
    }

    private TValidatedScalar(TScalar scalar, List<TInputSet> inputSets, boolean commuted) {
        super(scalar, inputSets);
        this.commuted = commuted;
        this.scalar = scalar;
    }

    private final TScalar scalar;
    private final boolean commuted;
}
