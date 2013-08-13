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

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.TExpressionExplainer;
import com.foundationdb.server.types3.ErrorHandlingMode;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;

public final class TPreparedParameter implements TPreparedExpression {

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return new TPreptimeValue(tInstance);
    }

    @Override
    public TInstance resultType() {
        return tInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation(position, tInstance);
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

    public TPreparedParameter(int position, TInstance tInstance) {
        this.position = position;
        this.tInstance = tInstance;
    }

    private final int position;
    private final TInstance tInstance;

    private static class InnerEvaluation implements TEvaluatableExpression {

        @Override
        public PValueSource resultValue() {
            return pValue;
        }

        @Override
        public void evaluate() {
            if (!pValue.hasAnyValue()) { // only need to compute this once
                TClass tClass = tInstance.typeClass();
                PValueSource inSource = bindings.getPValue(position);
                // TODO need a better execution context thinger
                TExecutionContext executionContext = new TExecutionContext(
                        null,
                        null,
                        tInstance,
                        context,
                        ErrorHandlingMode.WARN,
                        ErrorHandlingMode.WARN,
                        ErrorHandlingMode.WARN
                );
                tClass.fromObject(executionContext, inSource, pValue);
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

        private InnerEvaluation(int position, TInstance tInstance) {
            this.position = position;
            this.tInstance = tInstance;
            this.pValue = new PValue(tInstance);
        }

        private final int position;
        private final PValue pValue;
        private QueryContext context;
        private QueryBindings bindings;
        private final TInstance tInstance;
    }
}
