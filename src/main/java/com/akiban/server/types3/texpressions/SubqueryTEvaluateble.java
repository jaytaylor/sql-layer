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

package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

abstract class SubqueryTEvaluateble implements TEvaluatableExpression {

    @Override
    public PValueSource resultValue() {
        return pvalue;
    }

    @Override
    public void evaluate() {
        bindings.setRow(bindingPosition, outerRow);
        if (cursor == null) {
            cursor = API.cursor(subquery, context, bindings);
        }
        cursor.open();
        try {
            doEval(pvalue);
        }
        finally {
            cursor.close();
        }
    }

    @Override
    public void with(Row row) {
        if (row.rowType() != outerRowType) {
            throw new IllegalArgumentException("wrong row type: " + outerRowType +
                    " != " + row.rowType());
        }
        outerRow = row;
    }

    @Override
    public void with(QueryContext context) {
        this.context = context;
        cursor = null;
    }

    @Override
    public void with(QueryBindings bindings) {
        this.bindings = bindings;
        cursor = null;
    }

    protected abstract void doEval(PValueTarget out);

    protected QueryContext queryContext() {
        return context;
    }

    protected QueryBindings queryBindings() {
        return bindings;
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

    SubqueryTEvaluateble(Operator subquery, RowType outerRowType, RowType innerRowType, int bindingPosition,
                         TInstance underlying)
    {
        this.subquery = subquery;
        this.outerRowType = outerRowType;
        this.innerRowType = innerRowType;
        this.bindingPosition = bindingPosition;
        this.pvalue = new PValue(underlying);
    }

    private final Operator subquery;
    private final RowType outerRowType;
    private final RowType innerRowType;
    private final int bindingPosition;
    private final PValue pvalue;
    private Cursor cursor;
    private QueryContext context;
    private QueryBindings bindings;
    private Row outerRow;
}
