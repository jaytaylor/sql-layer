/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.types3.texpressions;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.util.List;

public abstract class TOverloadBase implements TOverload {

    @Override
    public List<TInputSet> inputSets() {
        TInputSetBuilder builder = new TInputSetBuilder();
        buildInputSets(builder);
        return builder.toList();
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        for (int i = 0; i < inputs.size(); ++i) {
            if (nullContaminates(i) && inputs.get(i).isNull()) {
                output.putNull();
            }
        }
        doEvaluate(context, inputs, output);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
    }

    @Override
    public TPreptimeValue evaluateConstant(TPreptimeContext context, final LazyList<? extends TPreptimeValue> inputs) {
        for (int i = 0; i < inputs.size(); ++i) {
            if (constnessMatters(i)) {
                TPreptimeValue preptimeValue = inputs.get(i);
                PValueSource prepSource = (preptimeValue == null) ? null : preptimeValue.value();
                Constantness constness = constness(i, prepSource);
                if (constness == Constantness.NOT_CONST)
                    return null;
                if (constness == Constantness.CONST)
                    break;
                assert constness == Constantness.UNKNOWN : constness;
            }
        }

        // at this point, assume there's a constant value and we can evaluate it

        finishPreptimePhase(context);

        TExecutionContext execContext = context.createExecutionContext();
        LazyList<PValueSource> inputValues = new LazyList<PValueSource>() {
            @Override
            public PValueSource get(int i) {
                TPreptimeValue ptValue = inputs.get(i);
                PValueSource source = ptValue.value();
                assert source != null : "non-constant value where constant value expected";
                return source;
            }

            @Override
            public int size() {
                return inputs.size();
            }
        };
        PValue outputValue = new PValue(execContext.outputTInstance().typeClass().underlyingType());
        evaluate(execContext, inputValues, outputValue);
        return new TPreptimeValue(execContext.outputTInstance(), outputValue);
    }

    protected abstract void buildInputSets(TInputSetBuilder builder);

    protected abstract void doEvaluate(TExecutionContext context,
                                       LazyList<? extends PValueSource> inputs,
                                       PValueTarget output);

    protected boolean constnessMatters(int inputIndex) {
        return true;
    }

    /**
     * <p>Determines this expression's constness given information about its input's prepare-time value. If the
     * prepare-time value is known (ie, is a constant), preptimeValue will contain that value. Otherwise, preptimeValue
     * will be <tt>null</tt>. Note that the reference itself will be null, <em>not</em> a reference to a PValueSource
     * whose {@link PValueSource#isNull()} returns <tt>true</tt>. A non-null PValueSource which <tt>isNull</tt> means 
     * a prepare-time constant value of <tt>NULL</tt>.</p>
     * 
     * <p>The default is to return {@linkplain Constantness#NOT_CONST} as soon as a non-const value is seen, and
     * {@linkplain Constantness#UNKNOWN} until then.</p>
     * @param inputIndex the index of the input whose prepare-time value is being considered
     * @param preptimeValue the prepare-time value, or <tt>null</tt> if not a prepare-time constant
     * @return what is known about this expression's constness due to this input
     */
    protected Constantness constness(int inputIndex, PValueSource preptimeValue) {
        return preptimeValue == null ? Constantness.NOT_CONST : Constantness.UNKNOWN;
    }

    protected boolean nullContaminates(int inputIndex) {
        return true;
    }
    
    protected enum Constantness {
        CONST, NOT_CONST, UNKNOWN
    }
}
