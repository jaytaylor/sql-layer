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

package com.akiban.sql.optimizer;

import java.util.List;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;

/** Update a row by substituting expressions for some fields. */
public class ExpressionRowUpdateFunction implements UpdateFunction
{
    private final List<Expression> expressions;
    private final RowType rowType;

    public ExpressionRowUpdateFunction(List<Expression> expressions, RowType rowType) {
        this.expressions = expressions;
        this.rowType = rowType;
    }

    /* UpdateFunction */

    @Override
    public boolean rowIsSelected(Row row) {
        return row.rowType().equals(rowType);
    }

    @Override
    public Row evaluate(Row original, QueryContext context) {
        OverlayingRow result = new OverlayingRow(original);
        int nfields = rowType.nFields();
        for (int i = 0; i < nfields; i++) {
            Expression expression = expressions.get(i);
            if (expression != null) {
                ExpressionEvaluation evaluation = expression.evaluation();
                evaluation.of(original);
                evaluation.of(context);
                result.overlay(i, evaluation.eval());
            }
        }
        return result;
    }

    /* Object */

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + expressions.toString() + ")";
    }

}
