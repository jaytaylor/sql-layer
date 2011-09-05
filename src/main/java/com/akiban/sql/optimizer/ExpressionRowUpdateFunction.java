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

import com.akiban.qp.expression.Expression;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.physicaloperator.UpdateFunction;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;

/** Update a row by substituting expressions for some fields. */
public class ExpressionRowUpdateFunction implements UpdateFunction
{
    private final ExpressionRow expressions;

    public ExpressionRowUpdateFunction(ExpressionRow expressions) {
        this.expressions = expressions;
    }

    /* UpdateFunction */

    @Override
    public boolean rowIsSelected(Row row) {
        return row.rowType().equals(expressions.rowType());
    }

    @Override
    public Row evaluate(Row original, Bindings bindings) {
        OverlayingRow result = new OverlayingRow(original);
        int nfields = expressions.rowType().nFields();
        for (int i = 0; i < nfields; i++) {
            Expression expression = expressions.getExpression(i);
            if (expression != null) {
                result.overlay(i, expression.evaluate(original, bindings));
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
