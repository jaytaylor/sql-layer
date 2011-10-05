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

package com.akiban.qp.expression;

import com.akiban.qp.operator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;

import java.util.List;

public final class RowBasedUnboundExpressions implements UnboundExpressions
{
    @Override
    public String toString()
    {
        return "UnboundExpressions" + expressions;
    }

    @Override
    public Row get(Bindings bindings)
    {
        return new ExpressionRow(rowType, bindings, expressions);
    }

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    public RowBasedUnboundExpressions(RowType rowType, List<Expression> expressions)
    {
        for (Expression expression : expressions) {
            if (expression == null) {
                throw new IllegalArgumentException();
            }
        }
        this.expressions = expressions;
        this.rowType = rowType;
    }

    private final List<Expression> expressions;
    private final RowType rowType;
}
