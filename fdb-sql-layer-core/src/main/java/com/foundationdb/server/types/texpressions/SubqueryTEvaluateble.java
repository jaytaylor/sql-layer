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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

abstract class SubqueryTEvaluateble implements TEvaluatableExpression {

    @Override
    public ValueSource resultValue() {
        return value;
    }

    @Override
    public void evaluate() {
        bindings.setRow(bindingPosition, outerRow);
        if (cursor == null) {
            cursor = API.cursor(subquery, context, bindings);
        }
        cursor.openTopLevel();
        try {
            doEval(value);
        } finally {
            cursor.closeTopLevel();
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

    protected abstract void doEval(ValueTarget out);

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
        this.value = new Value(underlying);
    }

    private final Operator subquery;
    private final RowType outerRowType;
    private final RowType innerRowType;
    private final int bindingPosition;
    private final Value value;
    private Cursor cursor;
    private QueryContext context;
    private QueryBindings bindings;
    private Row outerRow;
}
