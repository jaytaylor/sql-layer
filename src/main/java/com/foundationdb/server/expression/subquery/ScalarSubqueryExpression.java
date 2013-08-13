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

package com.foundationdb.server.expression.subquery;

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.SubqueryTooManyRowsException;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueHolder;

public final class ScalarSubqueryExpression extends SubqueryExpression {

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(subquery(), expression.evaluation(),
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

    @Override
    public String name () {
        return "VALUE";
    }
    
    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.EXPRESSIONS, expression.getExplainer(context));
        return explainer;
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
        public ValueSource doEval() {
            Row row = next();
            if (row == null)
                return NullValueSource.only();
            expressionEvaluation.of(queryContext());
            expressionEvaluation.of(queryBindings());
            expressionEvaluation.of(row);
            // Make a copy of the value evaluated right now, rather
            // than holding on to the row from the subquery cursor
            // that's about to be advanced and closed.
            ValueSource result = new ValueHolder(expressionEvaluation.eval());
            if (next() != null)
                throw new SubqueryTooManyRowsException();
            return result;
        }

        private InnerEvaluation(Operator subquery,
                                ExpressionEvaluation expressionEvaluation, 
                                RowType outerRowType, RowType innerRowType, 
                                int bindingPosition) {
            super(subquery, outerRowType, innerRowType, bindingPosition);
            this.expressionEvaluation = expressionEvaluation;
        }

        private final ExpressionEvaluation expressionEvaluation;
    }

}
