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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.TExpressionExplainer;
import com.foundationdb.server.types.InputSetFlags;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.LazyListBase;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.*;
import com.google.common.base.Predicate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public abstract class TScalarBase implements TScalar {

    @Override
    public List<TInputSet> inputSets() {
        TInputSetBuilder builder = new TInputSetBuilder();
        buildInputSets(builder);
        return builder.toList();
    }

    @Override
    public InputSetFlags exactInputs() {
        TInputSetBuilder builder = new TInputSetBuilder();
        buildInputSets(builder);
        return builder.exactInputs();
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        for (int i = 0; i < inputs.size(); ++i) {
            if (nullContaminates(i) && inputs.get(i).isNull()) {
                output.putNull();
                return;
            }
        }
        doEvaluate(context, inputs, output);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
    }

    @Override
    public String toString(List<? extends TPreparedExpression> inputs, TInstance resultType) {
        StringBuilder sb = new StringBuilder(toStringName());
        sb.append('(');
        sb.append(toStringArgsPrefix());
        for (int i = 0, len = inputs.size(); i < len; ++i) {
            sb.append(inputs.get(i));
            if (i+1 < len)
                sb.append(", ");
        }
        sb.append(" -> ");
        sb.append(resultType.typeClass().name().unqualifiedName());
        sb.append(')');
        return sb.toString();
    }

    protected String toStringName() {
        return displayName();
    }

    protected String toStringArgsPrefix() {
        return "";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context, List<? extends TPreparedExpression> inputs, TInstance resultType)
    {
        return new TExpressionExplainer(Type.FUNCTION, displayName(), context, inputs);
    }

    @Override
    public String id() {
        return getClass().getName();
    }

    @Override
    public TPreptimeValue evaluateConstant(TPreptimeContext context, final LazyList<? extends TPreptimeValue> inputs) {
        if (neverConstant()) {
            return null;
        }
        for (int i = 0; i < inputs.size(); ++i) {
            if (nullContaminates(i)) {
                ValueSource constSource = constSource(inputs, i);
                if ((constSource != null) && constSource.isNull()) {
                    // Const null source on contaminating operand. Result is null.
                    return new TPreptimeValue(context.getOutputType(), 
                                              ValueSources.getNullSource(context.getOutputType()));
                }
            }
        }
        for (int i = 0; i < inputs.size(); ++i) {
            if (constnessMatters(i)) {
                Constantness constness = constness(context, i, inputs);
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
        LazyList<ValueSource> inputValues = new LazyListBase<ValueSource>() {
            @Override
            public ValueSource get(int i) {
                TPreptimeValue ptValue = inputs.get(i);
                ValueSource source = ptValue.value();
                assert allowNonConstsInEvaluation() || source != null
                        : "non-constant value where constant value expected";
                return source;
            }

            @Override
            public int size() {
                return inputs.size();
            }
        };
        Value outputValue = new Value(execContext.outputType());
        evaluate(execContext, inputValues, outputValue);
        return new TPreptimeValue(execContext.outputType(), outputValue);
    }

    @Override
    public int[] getPriorities() {
        return new int[1];
    }

    protected static ValueSource constSource(LazyList<? extends TPreptimeValue> inputs, int index) {
        TPreptimeValue tpv = inputs.get(index);
        return tpv == null ? null : tpv.value();
    }

    protected boolean allowNonConstsInEvaluation() {
        return false;
    }

    protected abstract void buildInputSets(TInputSetBuilder builder);

    protected abstract void doEvaluate(TExecutionContext context,
                                       LazyList<? extends ValueSource> inputs,
                                       ValueTarget output);

    protected boolean constnessMatters(int inputIndex) {
        return true;
    }

    /**
     * <p>Determines this expression's constness given information about its input's prepare-time value. If the
     * prepare-time value is known (ie, is a constant), preptimeValue will contain that value. Otherwise, preptimeValue
     * will be <tt>null</tt>. Note that the reference itself will be null, <em>not</em> a reference to a ValueSource
     * whose {@link com.foundationdb.server.types.value.ValueSource#isNull()} returns <tt>true</tt>. A non-null ValueSource which <tt>isNull</tt> means
     * a prepare-time constant value of <tt>NULL</tt>.</p>
     * 
     * <p>The default is to return {@linkplain Constantness#NOT_CONST} as soon as a non-const value is seen, and
     * {@linkplain Constantness#UNKNOWN} until then.</p>
     *
     * @param inputIndex the index of the input whose prepare-time value is being considered
     * @param values
     * @return what is known about this expression's constness due to this input
     */
    protected Constantness constness(TPreptimeContext context, int inputIndex, LazyList<? extends TPreptimeValue> values) {
        return constSource(values,  inputIndex) == null ? Constantness.NOT_CONST : Constantness.UNKNOWN;
    }

    protected boolean nullContaminates(int inputIndex) {
        return true;
    }

    protected boolean neverConstant() {
        return false;
    }

    protected boolean anyContaminatingNulls(List<? extends TPreptimeValue> inputs) {
        for (int i = 0, max = inputs.size(); i < max; ++i) {
            if (nullContaminates(i) && inputs.get(i).isNullable())
                return true;
        }
        return false;
    }

    protected TOverloadResult customStringFromParameter(final int parameterIndex) {
        // actual return type is exactly the same as input type
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TInstance source = inputs.get(parameterIndex).type();
                assert source.typeClass() instanceof TString : "customStringFromParameter only works with TStrings";
                return source.typeClass().instance(source.attribute(StringAttribute.MAX_LENGTH), 
                        source.attribute(StringAttribute.CHARSET), 
                        source.attribute(StringAttribute.COLLATION),
                        anyContaminatingNulls(inputs));
            }
        });
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(displayName());
        sb.append('(');

        List<TInputSet> origInputSets = inputSets();
        if (!origInputSets.isEmpty()) {
            List<TInputSet> inputSets = new ArrayList<>(origInputSets);
            Collections.sort(inputSets, INPUT_SET_COMPARATOR);
            for (Iterator<TInputSet> iter = inputSets.iterator(); iter.hasNext(); ) {
                TInputSet inputSet = iter.next();
                sb.append(inputSet);
                if (iter.hasNext())
                    sb.append(", ");
            }
        }
        sb.append(") -> ");
        sb.append(resultType());
        return sb.toString();
    }

    private static final Comparator<? super TInputSet> INPUT_SET_COMPARATOR = new Comparator<TInputSet>() {
        @Override
        public int compare(TInputSet o1, TInputSet o2) {
            return o1.firstPosition() - o2.firstPosition();
        }
    };
    
    /**
     * @inheritDoc
     * 
     */
    @Override
    public String[] registeredNames()
    {
        return new String[] {displayName()};
    }
}
