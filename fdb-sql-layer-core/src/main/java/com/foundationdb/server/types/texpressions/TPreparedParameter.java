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
import com.foundationdb.server.types.ErrorHandlingMode;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

public final class TPreparedParameter implements TPreparedExpression {

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return new TPreptimeValue(type);
    }

    @Override
    public TInstance resultType() {
        return type;
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation(position, type);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.VARIABLE, "Variable", context);
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(position));
        return ex;
    }

    @Override
    public String toString() {
        return "Variable(pos=" + position + ')';
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    public TPreparedParameter(int position, TInstance type) {
        this.position = position;
        this.type = type;
    }

    private final int position;
    private final TInstance type;

    private static class InnerEvaluation implements TEvaluatableExpression {

        @Override
        public ValueSource resultValue() {
            return value;
        }

        @Override
        public void evaluate() {
            if (!value.hasAnyValue()) { // only need to compute this once
                TClass tClass = type.typeClass();
                ValueSource inSource = bindings.getValue(position);
                // TODO need a better execution context thinger
                TExecutionContext executionContext = new TExecutionContext(
                        null,
                        null,
                        type,
                        context,
                        ErrorHandlingMode.WARN,
                        ErrorHandlingMode.WARN,
                        ErrorHandlingMode.WARN
                );
                tClass.fromObject(executionContext, inSource, value);
            }
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
            this.context = context;
        }

        @Override
        public void with(QueryBindings bindings) {
            this.bindings = bindings;
        }

        private InnerEvaluation(int position, TInstance type) {
            this.position = position;
            this.type = type;
            this.value = new Value(type);
        }

        private final int position;
        private final Value value;
        private QueryContext context;
        private QueryBindings bindings;
        private final TInstance type;
    }
}
