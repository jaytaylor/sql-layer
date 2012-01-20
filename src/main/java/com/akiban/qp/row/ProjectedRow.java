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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.ProjectedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.Quote;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.AkibanAppender;

import java.util.ArrayList;
import java.util.List;

public class ProjectedRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(buffer);
        buffer.append('(');
        boolean first = true;
        for (ExpressionEvaluation evaluation : evaluations) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            evaluation.eval().appendAsString(appender, Quote.NONE);
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
        ValueHolder holder = holders[i];
        if (holder == null) {
            holders[i] = holder = new ValueHolder();
            holder.copyFrom(evaluations.get(i).eval());
        }
        return holder;
    }

    @Override
    public HKey hKey()
    {
        return null;
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable) {
        return row.containsRealRowOf(userTable);
    }

    // AbstractRow interface


    @Override
    protected void beforeAcquire() {
        row.acquire();
    }

    @Override
    public void afterRelease()
    {
        row.release();
    }

    // ProjectedRow interface

    public ProjectedRow(ProjectedRowType rowType, Row row, QueryContext context, List<Expression> expressions)
    {
        this.rowType = rowType;
        this.row = row;
        this.evaluations = createEvaluations(expressions, row, context);
        this.holders = new ValueHolder[expressions.size()];
        super.runId(row.runId());
    }

    /** Make sure all the <code>ValueHolder</code>s are full. */
    public void freeze() {
        for (int i = 0; i < holders.length; i++) {
            if (holders[i] == null) {
                eval(i);
            }
        }
    }

    // For use by this class

    private List<ExpressionEvaluation> createEvaluations(List<Expression> expressions, 
                                                         Row row, QueryContext context)
    {
        List<ExpressionEvaluation> result = new ArrayList<ExpressionEvaluation>();
        for (Expression expression : expressions) {
            ExpressionEvaluation evaluation = expression.evaluation();
            evaluation.of(context);
            evaluation.of(row);
            result.add(evaluation);
        }
        return result;
    }

    // Object state

    private final ProjectedRowType rowType;
    private final Row row;
    private final List<ExpressionEvaluation> evaluations;
    private final ValueHolder[] holders;
}
