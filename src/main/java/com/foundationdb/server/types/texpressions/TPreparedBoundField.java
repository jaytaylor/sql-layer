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
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;

public final class TPreparedBoundField implements TPreparedExpression {
    
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return new TPreptimeValue(resultType());
    }

    @Override
    public TInstance resultType() {
        return fieldExpression.resultType();
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation(fieldExpression.build(), rowPosition);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        // Extend Field inside, rather than wrapping it.
        CompoundExplainer ex = fieldExpression.getExplainer(context);
        ex.get().remove(Label.NAME); // Want to replace.
        ex.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("Bound"));
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(rowPosition));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }

    @Override
    public String toString() {
        return "Bound(" + rowPosition + ',' + fieldExpression + ')';
    }

    public TPreparedBoundField(RowType rowType, int rowPosition, int fieldPosition) {
        fieldExpression = new TPreparedField(rowType.typeAt(fieldPosition), fieldPosition);
        this.rowPosition = rowPosition;
    }

    private final TPreparedField fieldExpression;
    private final int rowPosition;

    private static class InnerEvaluation implements TEvaluatableExpression {

        @Override
        public ValueSource resultValue() {
            return fieldEvaluation.resultValue();
        }

        @Override
        public void evaluate() {
            fieldEvaluation.evaluate();
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
            fieldEvaluation.with(context);
        }

        @Override
        public void with(QueryBindings bindings) {
            fieldEvaluation.with(bindings.getRow(rowPosition));
        }

        private InnerEvaluation(TEvaluatableExpression fieldEvaluation, int rowPosition) {
            this.fieldEvaluation = fieldEvaluation;
            this.rowPosition = rowPosition;
        }

        private final TEvaluatableExpression fieldEvaluation;
        private final int rowPosition;
    }
}
