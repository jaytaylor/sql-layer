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
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.RowType;

public class ExpressionRow extends AbstractRow
{
    private RowType rowType;
    private Expression[] expressions;

    public ExpressionRow(RowType rowType, Expression[] expressions) {
        this.rowType = rowType;
        this.expressions = expressions;
    }

    /* AbstractRow */

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public Object field(int i, Bindings bindings) {
        if (expressions[i] == null)
            return null;
        else
            return expressions[i].evaluate(null, bindings);
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();        
    }

    /* Object */

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getClass().getSimpleName());
        str.append('[');
        int nf = rowType().nFields();
        for (int i = 0; i < nf; i++) {
            if (i > 0) str.append(", ");
            if (expressions[i] != null)
                str.append(expressions[i]);
        }
        str.append(']');
        return str.toString();
    }

}
