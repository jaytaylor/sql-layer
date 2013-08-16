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

package com.foundationdb.server.expression.std;

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.explain.std.ExpressionExplainer;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import java.util.Map;

public final class BoundFieldExpression implements Expression {
    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean needsBindings() {
        return true;
    }

    @Override
    public boolean needsRow() {
        return false;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(rowBindingPosition, fieldExpression.evaluation());
    }

    @Override
    public AkType valueType() {
        return fieldExpression.valueType();
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("Bound(%d,%s)", rowBindingPosition, fieldExpression.toString());
    }

    // BoundFieldExpression interface

    public BoundFieldExpression(int rowBindingPosition, FieldExpression fieldExpression) {
        this.rowBindingPosition = rowBindingPosition;
        this.fieldExpression = fieldExpression;
    }

    // state
    private final int rowBindingPosition;
    private final FieldExpression fieldExpression;

    @Override
    public String name()
    {
        return "Bound";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        // Extend Field inside, rather than wrapping it.
        CompoundExplainer ex = fieldExpression.getExplainer(context);
        ex.get().remove(Label.NAME); // Want to replace.
        ex.addAttribute(Label.NAME, PrimitiveExplainer.getInstance(name()));
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(rowBindingPosition));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }
    
    public boolean nullIsContaminating()
    {
        return true;
    }

    private static class InnerEvaluation implements ExpressionEvaluation {
        @Override
        public void of(Row row) {
        }

        @Override
        public void of(QueryContext context) {
        }

        @Override
        public void of(QueryBindings bindings) {
            fieldExpressionEvaluation.of(bindings.getRow(rowBindingPosition));
        }

        @Override
        public ValueSource eval() {
            return fieldExpressionEvaluation.eval();
        }

        @Override
        public void destroy() {
            fieldExpressionEvaluation.destroy();
        }

        @Override
        public void acquire() {
            fieldExpressionEvaluation.acquire();
        }

        @Override
        public boolean isShared() {
            return fieldExpressionEvaluation.isShared();
        }

        @Override
        public void release() {
            fieldExpressionEvaluation.release();
        }

        private InnerEvaluation(int rowBindingPosition, ExpressionEvaluation fieldExpressionEvaluation) {
            this.rowBindingPosition = rowBindingPosition;
            this.fieldExpressionEvaluation = fieldExpressionEvaluation;
        }

        private final int rowBindingPosition;
        private final ExpressionEvaluation fieldExpressionEvaluation;
    }
}
