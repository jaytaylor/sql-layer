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

package com.foundationdb.sql.optimizer;

import java.util.List;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types3.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types3.texpressions.TPreparedExpression;

/** Update a row by substituting expressions for some fields. */
public class ExpressionRowUpdateFunction implements UpdateFunction
{
    private final List<Expression> expressions;
    private final List<TPreparedExpression> pExpressions;
    private final RowType rowType;

    public ExpressionRowUpdateFunction(List<Expression> expressions, List<TPreparedExpression> pExpressions, RowType rowType) {
        this.expressions = expressions;
        this.pExpressions = pExpressions;
        this.rowType = rowType;
    }

    /* UpdateFunction */

    @Override
    public boolean rowIsSelected(Row row) {
        return row.rowType().equals(rowType);
    }

    @Override
    public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
        OverlayingRow result = new OverlayingRow(original, pExpressions != null);
        int nfields = rowType.nFields();
        for (int i = 0; i < nfields; i++) {
            if (pExpressions != null) {
                assert expressions == null : "can't have both expression types";
                TPreparedExpression expression = pExpressions.get(i);
                if (expression != null) {
                    TEvaluatableExpression evaluation = expression.build();
                    evaluation.with(original);
                    evaluation.with(context);
                    evaluation.with(bindings);
                    evaluation.evaluate();
                    result.overlay(i, evaluation.resultValue());
                }
            }
            else if (expressions != null) {
                Expression expression = expressions.get(i);
                if (expression != null) {
                    ExpressionEvaluation evaluation = expression.evaluation();
                    evaluation.of(original);
                    evaluation.of(context);
                    evaluation.of(bindings);
                    result.overlay(i, evaluation.eval());
                }
            }
            else {
                assert false: "must have one expression list";
            }
        }
        return result;
    }

    @Override
    public boolean usePValues() {
        return pExpressions != null;
    }

    /* Object */

    @Override
    public String toString() {
        String exprs = (pExpressions != null) ? pExpressions.toString() : expressions.toString();
        return getClass().getSimpleName() + "(" + exprs + ")";
    }

}
