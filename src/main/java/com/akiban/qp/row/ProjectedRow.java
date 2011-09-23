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

package com.akiban.qp.row;

import com.akiban.qp.operator.Bindings;
import com.akiban.qp.rowtype.ProjectedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;

import java.util.ArrayList;
import java.util.List;

public class ProjectedRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('(');
        boolean first = true;
        for (ExpressionEvaluation evaluation : evaluations) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(evaluation.eval());
        }
        buffer.append(')');
        return buffer.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        return evaluations.get(i).eval();
    }

    @Override
    public HKey hKey()
    {
        return null;
    }

    // AbstractRow interface

    @Override
    public void afterRelease()
    {
        row.set(null);
    }

    // ProjectedRow interface

    public ProjectedRow(ProjectedRowType rowType, Row row, Bindings bindings, List<Expression> expressions)
    {
        this.rowType = rowType;
        this.row.set(row);
        this.expressions = expressions;
        this.evaluations = createEvaluations(row, bindings);
        super.runId(row.runId());
    }

    // For use by this class

    private List<ExpressionEvaluation> createEvaluations(Row row, Bindings bindings)
    {
        List<ExpressionEvaluation> result = new ArrayList<ExpressionEvaluation>();
        for (Expression expression : this.expressions) {
            ExpressionEvaluation evaluation = expression.evaluation();
            evaluation.of(bindings);
            evaluation.of(row);
            result.add(evaluation);
        }
        return result;
    }

    // Object state

    private final ProjectedRowType rowType;
    private final RowHolder<Row> row = new RowHolder<Row>();
    private final List<Expression> expressions;
    private final List<ExpressionEvaluation> evaluations;
}
