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

import java.util.ArrayList;
import java.util.List;

import com.akiban.qp.operator.Bindings;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.FromObjectValueSource;

public class ExpressionRow extends AbstractRow
{
    private RowType rowType;
    private List<Expression> expressions;

    public ExpressionRow(RowType rowType, Bindings bindings, List<Expression> expressions) {
        this.rowType = rowType;
        this.expressions = expressions;
        this.bindings = bindings;
    }

    public Expression getExpression(int i) {
        return expressions.get(i);
    }
    
    public List<Expression> getExpressions() {
        return expressions;
    }

    /* AbstractRow */

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        Expression expression = getExpression(i);
        Object value = (expression == null) ? null : expression.evaluate(null, bindings());
        source.setReflectively(value);
        return source;
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
            Expression expression = getExpression(i);
            if (expression != null)
                str.append(expression);
        }
        str.append(']');
        return str.toString();
    }

    // private methods

    private Bindings bindings() {
        return bindings;
    }

    // object state

    private final FromObjectValueSource source = new FromObjectValueSource();
    private final Bindings bindings;
}
