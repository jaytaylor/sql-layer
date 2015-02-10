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

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.TExpressionExplainer;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.SparseArray;

import java.util.Collections;

public final class TCastExpression implements TPreparedExpression {

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        TPreptimeValue inputValue = input.evaluateConstant(queryContext);
        Value value;
        if (inputValue == null || inputValue.value() == null) {
            value = null;
        }
        else {
            value = new Value(targetInstance);

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
        return new CastEvaluation(input.build(), cast, sourceInstance, targetInstance);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer ex = new TExpressionExplainer(Type.FUNCTION, "CAST", context);
        ex.addAttribute(Label.OPERAND, input.getExplainer(context));
        ex.addAttribute(Label.OUTPUT_TYPE, PrimitiveExplainer.getInstance(targetInstance.toStringConcise(false)));
        return ex;
    }

    @Override
    public String toString() {
        return "CAST(" + input + " AS " + targetInstance + ")";
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    public TCastExpression(TPreparedExpression input, TCast cast, TInstance targetInstance) {
        this.input = input;
        this.cast = cast;
        this.targetInstance = targetInstance;
        this.sourceInstance = input.resultType();
        assert sourceInstance.typeClass() == cast.sourceClass()
                : sourceInstance + " not an acceptable source for cast " + cast;
    }


    private final TInstance sourceInstance;
    private final TInstance targetInstance;
    private final TPreparedExpression input;
    private final TCast cast;

    private static class CastEvaluation implements TEvaluatableExpression {
        @Override
        public ValueSource resultValue() {
            return value;
        }

        @Override
        public void evaluate() {
            inputEval.evaluate();
            ValueSource inputVal = inputEval.resultValue();
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
                               TInstance sourceInstance, TInstance targetInstance)
        {
            this.inputEval = inputEval;
            this.cast = cast;
            this.value = new Value(targetInstance);
            this.executionContext = new TExecutionContext(
                    Collections.singletonList(sourceInstance),
                    targetInstance,
                    null);
        }

        private final TExecutionContext executionContext;
        private final TEvaluatableExpression inputEval;
        private final TCast cast;
        private final Value value;
    }
}
