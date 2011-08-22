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

package com.akiban.sql.optimizer.query;

import com.akiban.sql.StandardException;

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.ais.model.Column;

import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.API;

/** An expression evaluating a column in an actual table. */
public class ColumnExpression extends BaseExpression 
{
    private TableReference table;
    private Column column;

    public ColumnExpression(TableReference table, Column column, 
                            DataTypeDescriptor type) {
        super(type);
        this.table = table;
        this.column = column;
    }

    public TableReference getTable() {
        return table;
    }

    public Column getColumn() {
        return column;
    }

    public String toString() {
        return column.toString();
    }

    public boolean isColumn() {
        return true;
    }

    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) 
            throws StandardException {
        return API.field(fieldOffsets.getIndex(this));
    }
}
