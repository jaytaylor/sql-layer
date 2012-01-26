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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.FromObjectValueSource;

public class ExpressionRow extends AbstractRow
{
    private RowType rowType;
    private List<? extends Expression> expressions;
    private List<ExpressionEvaluation> evaluations;

    public ExpressionRow(RowType rowType, QueryContext context, List<? extends Expression> expressions) {
        this.rowType = rowType;
        this.expressions = expressions;
        this.evaluations = new ArrayList<ExpressionEvaluation>(expressions.size());
        for (Expression expression : expressions) {
            if (expression.needsRow()) {
                throw new AkibanInternalException("expression needed a row: " + expression + " in " + expressions);
            }
            ExpressionEvaluation evaluation = expression.evaluation();
            evaluation.of(context);
            this.evaluations.add(evaluation);
        }
    }

    /* AbstractRow */

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        return evaluations.get(i).eval();
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();        
    }

    @Override
    public void release() {
    }

    @Override
    public boolean isShared() {
        return false;
    }

    @Override
    public void acquire() {
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
            Expression expression = expressions.get(i);
            if (expression != null)
                str.append(expression);
        }
        str.append(']');
        return str.toString();
    }
}
