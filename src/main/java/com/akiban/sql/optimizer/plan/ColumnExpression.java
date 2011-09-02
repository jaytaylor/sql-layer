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

package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.ais.model.Column;

import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.API;

/** An expression evaluating a column in an actual table. */
public class ColumnExpression extends BaseExpression 
{
    private ColumnSource table;
    private Column column;
    private int position;

    public ColumnExpression(TableSource table, Column column, 
                            DataTypeDescriptor type) {
        super(type);
        this.table = table;
        assert (table.getTable().getTable() == column.getUserTable());
        this.column = column;
        this.position = column.getPosition();
    }

    public ColumnExpression(ColumnSource table, int position, 
                            DataTypeDescriptor type) {
        super(type);
        this.table = table;
        this.position = position;
    }

    public ColumnSource getTable() {
        return table;
    }

    public Column getColumn() {
        return column;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        if (column != null)
            return table.getName() + "." + column.getName();
        else
            return table.getName() + "[" + position + "]";
    }

    @Override
    public boolean isColumn() {
        return true;
    }

    @Override
    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
        return API.field(fieldOffsets.getIndex(this));
    }
}
