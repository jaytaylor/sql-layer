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

package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.TExpressionExplainer;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.SparseArray;

import java.util.Collections;

public final class TCastExpression implements TPreparedExpression {

    private static final boolean HIDE_CASTS = true;

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        TPreptimeValue inputValue = input.evaluateConstant(queryContext);
        PValue value;
        if (inputValue == null || inputValue.value() == null) {
            value = null;
        }
        else {
            value = new PValue(targetInstance);

            TExecutionContext context = new TExecutionContext(
                    new SparseArray<>(),
                    Collections.singletonList(input.resultType()),
                    targetInstance,
                    queryContext,
                    null,
                    null,
                    null
            );
            cast.evaluate(context, inputValue.value(), value);
        }
        return new TPreptimeValue(targetInstance, value);
    }

    @Override
    public TInstance resultType() {
        return targetInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new CastEvaluation(input.build(), cast, sourceInstance, targetInstance, preptimeContext);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        if (HIDE_CASTS)
            return input.getExplainer(context);
        CompoundExplainer ex = new TExpressionExplainer(Type.FUNCTION, "CAST", context);
        ex.addAttribute(Label.OPERAND, input.getExplainer(context));
        ex.addAttribute(Label.OUTPUT_TYPE, PrimitiveExplainer.getInstance(targetInstance.toString()));
        return ex;
    }

    @Override
    public String toString() {
        return input.toString(); // for backwards compatibility in OperatorCompilerTest, don't actually print the cast
    }

    public TCastExpression(TPreparedExpression input, TCast cast, TInstance targetInstance, QueryContext queryContext) {
        this.input = input;
        this.cast = cast;
        this.targetInstance = targetInstance;
        this.sourceInstance = input.resultType();
        this.preptimeContext = queryContext;
        assert sourceInstance.typeClass() == cast.sourceClass()
                : sourceInstance + " not an acceptable source for cast " + cast;
    }


    private final TInstance sourceInstance;
    private final TInstance targetInstance;
    private final TPreparedExpression input;
    private final TCast cast;
    private final QueryContext preptimeContext;

    private static class CastEvaluation implements TEvaluatableExpression {
        @Override
        public PValueSource resultValue() {
            return value;
        }

        @Override
        public void evaluate() {
            inputEval.evaluate();
            PValueSource inputVal = inputEval.resultValue();
            cast.evaluate(executionContext, inputVal, value);
        }

        @Override
        public void with(Row row) {
            inputEval.with(row);
        }

        @Override
        public void with(QueryContext context) {
            inputEval.with(context);
            this.executionContext.setQueryContext(context);
        }

        @Override
        public void with(QueryBindings bindings) {
            inputEval.with(bindings);
        }

        private CastEvaluation(TEvaluatableExpression inputEval, TCast cast,
                               TInstance sourceInstance, TInstance targetInstance, QueryContext preptimeContext)
        {
            this.inputEval = inputEval;
            this.cast = cast;
            this.value = new PValue(targetInstance);
            this.executionContext = new TExecutionContext(
                    Collections.singletonList(sourceInstance),
                    targetInstance,
                    preptimeContext);
        }

        private final TExecutionContext executionContext;
        private final TEvaluatableExpression inputEval;
        private final TCast cast;
        private final PValue value;
    }
}
