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
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.AkibanAppender;

public final class TPreparedLiteral implements TPreparedExpression {
    
    @Override
    public TInstance resultType() {
        return tInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new Evaluation(value);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.LITERAL, "Literal", context);
        StringBuilder sql = new StringBuilder();
        if (tInstance == null)
            sql.append("NULL");
        else
            tInstance.formatAsLiteral(value, AkibanAppender.of(sql));
        ex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance(sql.toString()));
        return ex;
    }

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        if (tInstance == null)
            return new TPreptimeValue(null);
        else
            return new TPreptimeValue(tInstance, value);
    }

    @Override
    public String toString() {
        if (tInstance == null)
            return "LiteralNull";
        StringBuilder sb = new StringBuilder("Literal(");
        tInstance.format(value, AkibanAppender.of(sb));
        sb.append(')');
        return sb.toString();
    }

    public TPreparedLiteral(TInstance tInstance, ValueSource value) {
        if (tInstance == null) {
            this.tInstance = null;
            this.value = ValueSources.getNullSource(null);
        }
        else {
            this.tInstance = tInstance;
            this.value = value;
        }
    }

    private final TInstance tInstance;
    private final ValueSource value;

    private static class Evaluation implements TEvaluatableExpression {
        @Override
        public ValueSource resultValue() {
            return value;
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

        private Evaluation(ValueSource value) {
            this.value = value;
        }

        private final ValueSource value;
    }
}
