/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;

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
        return "BOUND";
    }

    @Override
    public Explainer getExplainer()
    {
        Explainer ex = new ExpressionExplainer (Type.FUNCTION, name(), fieldExpression);
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(rowBindingPosition));
        return ex;
    }

    private static class InnerEvaluation implements ExpressionEvaluation {
        @Override
        public void of(Row row) {
        }

        @Override
        public void of(QueryContext context) {
            fieldExpressionEvaluation.of(context.getRow(rowBindingPosition));
        }

        @Override
        public ValueSource eval() {
            return fieldExpressionEvaluation.eval();
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
