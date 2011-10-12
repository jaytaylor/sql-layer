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

package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.BoolValueSource;

public final class ScalarSubqueryExpression extends SubqueryExpression {

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(subquery(), null, // TODO: adapter?
                                   expression.evaluation(),
                                   outerRowType(), innerRowType(),
                                   bindingPosition());
    }

    @Override
    public AkType valueType() {
        return expression.valueType();
    }

    @Override
    public String toString() {
        return "VALUE(" + subquery() + ")";
    }

    public ScalarSubqueryExpression(Operator subquery, Expression expression,
                                    RowType outerRowType, RowType innerRowType, 
                                    int bindingPosition) {
        super(subquery, outerRowType, innerRowType, bindingPosition);
        this.expression = expression;
    }
                                 
    private final Expression expression;

    private static final class InnerEvaluation extends SubqueryExpressionEvaluation {
        @Override
        public ValueSource eval() {
            expressionEvaluation.of(bindings());
            open();
            Row row = next();
            expressionEvaluation.of(row);
            ValueSource result = expressionEvaluation.eval();
            // TODO: Is it legit to close it and still hang on to result?
            close();
            return result;
        }

        private InnerEvaluation(Operator subquery, StoreAdapter adapter,
                                ExpressionEvaluation expressionEvaluation, 
                                RowType outerRowType, RowType innerRowType, 
                                int bindingPosition) {
            super(subquery, adapter, outerRowType, innerRowType, bindingPosition);
            this.expressionEvaluation = expressionEvaluation;
        }

        private final ExpressionEvaluation expressionEvaluation;
    }

}
