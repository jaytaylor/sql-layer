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

package com.foundationdb.server.types3.texpressions;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.LazyListBase;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TPreptimeContext;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.util.SparseArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class TPreparedFunction implements TPreparedExpression {

    @Override
    public TInstance resultType() {
        return resultType;
    }

    @Override
    public TPreptimeValue evaluateConstant(final QueryContext queryContext) {
        LazyList<TPreptimeValue> lazyInputs = new LazyListBase<TPreptimeValue>() {
            @Override
            public TPreptimeValue get(int i) {
                return inputs.get(i).evaluateConstant(queryContext);
            }

            @Override
            public int size() {
                return inputs.size();
            }
        };
        return overload.evaluateConstant(preptimeContext, overload.filterInputs(lazyInputs));
    }

    @Override
    public TEvaluatableExpression build() {
        List<TEvaluatableExpression> children = new ArrayList<>(inputs.size());
        for (TPreparedExpression input : inputs)
            children.add(input.build());
        return new TEvaluatableFunction(
                overload,
                resultType,
                children,
                preptimeContext.createExecutionContext());
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return overload.getExplainer(context, inputs, resultType);
    }

    @Override
    public String toString() {
        return overload.toString(inputs, resultType);
    }

    public TPreparedFunction(TValidatedScalar overload,
                             TInstance resultType,
                             List<? extends TPreparedExpression> inputs,
                             QueryContext queryContext)
    {
        this(overload, resultType, inputs, queryContext, null);
    }

    public TPreparedFunction(TValidatedScalar overload,
                             TInstance resultType,
                             List<? extends TPreparedExpression> inputs,
                             QueryContext queryContext,
                             SparseArray<Object> preptimeValues)
    {
        this.overload = overload;
        this.resultType = resultType;
        TInstance[] localInputTypes = new TInstance[inputs.size()];
        for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
            TPreparedExpression input = inputs.get(i);
            localInputTypes[i] = input.resultType();
        }
        this.inputs = inputs;
        this.preptimeContext = new TPreptimeContext(Arrays.asList(localInputTypes), resultType, queryContext, preptimeValues);
    }

    private final TValidatedScalar overload;
    private final TInstance resultType;
    private final List<? extends TPreparedExpression> inputs;
    private final TPreptimeContext preptimeContext;

    private static final class TEvaluatableFunction implements TEvaluatableExpression {

        @Override
        public void with(Row row) {
            for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
                TEvaluatableExpression input = inputs.get(i);
                input.with(row);
                inputValues[i] = null;
            }
        }

        @Override
        public void with(QueryContext context) {
            this.context.setQueryContext(context);
            for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
                TEvaluatableExpression input = inputs.get(i);
                input.with(context);
                inputValues[i] = null;
            }
        }

        @Override
        public void with(QueryBindings bindings) {
            for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
                TEvaluatableExpression input = inputs.get(i);
                input.with(bindings);
                inputValues[i] = null;
            }
        }

        @Override
        public PValueSource resultValue() {
            return resultValue;
        }

        @Override
        public void evaluate() {
            Arrays.fill(inputValues, null);
            overload.evaluate(context, evaluations, resultValue);
        }

        public TEvaluatableFunction(TValidatedScalar overload,
                                    TInstance resultType,
                                    final List<? extends TEvaluatableExpression> inputs,
                                    TExecutionContext context)
        {
            this.overload = overload;
            this.inputs = inputs;
            this.inputValues = new PValueSource[inputs.size()];
            this.context = context;
            resultValue = new PValue(resultType);
            this.evaluations = overload.filterInputs(new LazyListBase<PValueSource>() {
                @Override
                public PValueSource get(int i) {
                    PValueSource value = inputValues[i];
                    if (value == null) {
                        TEvaluatableExpression inputExpr = inputs.get(i);
                        inputExpr.evaluate();
                        value = inputExpr.resultValue();
                        inputValues[i] = value;
                    }
                    return value;
                }

                @Override
                public int size() {
                    return inputValues.length;
                }
            });
        }

        private final TValidatedScalar overload;
        private final PValueSource[] inputValues;
        private final List<? extends TEvaluatableExpression> inputs;
        private final PValue resultValue;
        private final TExecutionContext context;
        private final LazyList<? extends PValueSource> evaluations;
    }
}
