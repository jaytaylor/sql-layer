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

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

import java.util.Collections;
import java.util.List;

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
    // BoundFieldExpression interface


    public BoundFieldExpression(int rowBindingPosition, FieldExpression fieldExpression) {
        this.rowBindingPosition = rowBindingPosition;
        this.fieldExpression = fieldExpression;
    }

    // state
    private final int rowBindingPosition;
    private final FieldExpression fieldExpression;

    private static class InnerEvaluation implements ExpressionEvaluation {
        @Override
        public void of(Row row) {
            assert false : "are you sure you want to do that, Dave?";
        }

        @Override
        public void of(Bindings bindings) {
            fieldExpressionEvaluation.of((Row)bindings.get(rowBindingPosition));
        }

        @Override
        public ValueSource eval() {
            return fieldExpressionEvaluation.eval();
        }

        private InnerEvaluation(int rowBindingPosition, ExpressionEvaluation fieldExpressionEvaluation) {
            this.rowBindingPosition = rowBindingPosition;
            this.fieldExpressionEvaluation = fieldExpressionEvaluation;
        }

        private final int rowBindingPosition;
        private final ExpressionEvaluation fieldExpressionEvaluation;
    }
}
