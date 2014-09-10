/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.qp.row;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.ProjectedRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.AkibanAppender;

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
        for (int i = 0, pEvalsSize = pEvaluatableExpressions.size(); i < pEvalsSize; i++) {
            ValueSource evaluation = value(i);
            TInstance type = types.get(i);
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            if (type != null) {
                type.format(evaluation, appender);
            } else {
                buffer.append("NULL");
            }
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
    public ValueSource value(int index) {
        TEvaluatableExpression evaluatableExpression = pEvaluatableExpressions.get(index);
        if (!evaluated[index]) {
            evaluatableExpression.with(context);
            evaluatableExpression.with(bindings);
            evaluatableExpression.with(row);
            evaluatableExpression.evaluate();
            evaluated[index] = true;
        }
        return evaluatableExpression.resultValue();
    }

    @Override
    public HKey hKey()
    {
        return null;
    }

    // ProjectedRow interface

    public ProjectedRow(ProjectedRowType rowType,
                        Row row,
                        QueryContext context,
                        QueryBindings bindings,
                        List<TEvaluatableExpression> pEvaluatableExprs,
                        List<? extends TInstance> types)
    {
        this.context = context;
        this.bindings = bindings;
        this.rowType = rowType;
        this.row = row;
        this.pEvaluatableExpressions = pEvaluatableExprs;
        if (pEvaluatableExpressions == null)
            evaluated = null;
        else
            evaluated = new boolean[pEvaluatableExpressions.size()];
        this.types = types;
    }

    public static List<TEvaluatableExpression> createTEvaluatableExpressions
        (List<? extends TPreparedExpression> pExpressions)
    {
        if (pExpressions == null)
            return null;
        int n = pExpressions.size();
        List<TEvaluatableExpression> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            TEvaluatableExpression eval = pExpressions.get(i).build();
            result.add(eval);
        }
        return result;
    }


    // Object state

    private final QueryContext context;
    private final QueryBindings bindings;
    private final ProjectedRowType rowType;
    private final Row row;
    private final List<TEvaluatableExpression> pEvaluatableExpressions;
    private final boolean[] evaluated;
    private final List<? extends TInstance> types;
}
