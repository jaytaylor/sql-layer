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

package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;

public abstract class SubqueryExpressionEvaluation extends ExpressionEvaluation.Base {

    @Override
    public void of(QueryContext context) {
        this.context = context;
        this.cursor = API.cursor(subquery, context);
    }

    @Override
    public void of(Row row) {
        if (row.rowType() != outerRowType) {
            throw new IllegalArgumentException("wrong row type: " + outerRowType +
                                               " != " + row.rowType());
        }
        outerRow = row;
    }

    @Override
    public final ValueSource eval() {
        context.setRow(bindingPosition, outerRow);
        cursor.open();
        try {
            return doEval();
        }
        finally {
            cursor.close();
        }
    }

    @Override
    public void destroy()
    {
        if (cursor != null) {
            cursor.destroy();
        }
    }

    // Shareable interface

    @Override
    public void acquire() {
        outerRow.acquire();
    }

    @Override
    public boolean isShared() {
        return outerRow.isShared();
    }

    @Override
    public void release() {
        outerRow.release();
    }

    // for use by subclasses

    protected abstract ValueSource doEval();

    protected QueryContext queryContext() {
        return context;
    }

    protected Row next() {
        Row row = cursor.next();
        if ((row != null) &&
            (row.rowType() != innerRowType)) {
            throw new IllegalArgumentException("wrong row type: " + innerRowType +
                                               " != " + row.rowType());
        }
        return row;
    }

    protected SubqueryExpressionEvaluation(Operator subquery,
                                           RowType outerRowType, RowType innerRowType, 
                                           int bindingPosition) {
        this.subquery = subquery;
        this.outerRowType = outerRowType;
        this.innerRowType = innerRowType;
        this.bindingPosition = bindingPosition;
    }

    private final Operator subquery;
    private final RowType outerRowType;
    private final RowType innerRowType;
    private final int bindingPosition;
    private Cursor cursor;
    private QueryContext context;
    private Row outerRow;

}
