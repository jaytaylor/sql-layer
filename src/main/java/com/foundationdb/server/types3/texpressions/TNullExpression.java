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
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueSources;

public final class TNullExpression implements TPreparedExpression {

    public TNullExpression (TInstance tInstance) {
        this.tInstance = tInstance.withNullable(true);
    }

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        TEvaluatableExpression eval = build();
        return new TPreptimeValue(tInstance, eval.resultValue());
    }

    @Override
    public TInstance resultType() {
        return tInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation(tInstance);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.LITERAL, "Literal", context);
        ex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance("NULL"));
        return ex;
    }

    @Override
    public String toString() {
        return "Literal(NULL)";
    }

    private final TInstance tInstance;

    private static class InnerEvaluation implements TEvaluatableExpression {
        @Override
        public PValueSource resultValue() {
            return valueSource;
        }

        @Override
        public void evaluate() {
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
        }

        @Override
        public void with(QueryBindings bindings) {
        }

        private InnerEvaluation(TInstance underlying) {
            this.valueSource = PValueSources.getNullSource(underlying);
        }

        private final PValueSource valueSource;
    }
}
