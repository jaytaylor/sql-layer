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

import com.akiban.qp.expression.Expression;

import com.akiban.sql.StandardException;

import com.akiban.sql.types.DataTypeDescriptor;

/** An evaluated value. */
public abstract class BaseExpression 
{
    // TODO: Maybe AkType here once that's stable.
    private DataTypeDescriptor type;

    protected BaseExpression(DataTypeDescriptor type) {
        this.type = type;
    }

    public DataTypeDescriptor getType() {
        return type;
    }

    public boolean isColumn() {
        return false;
    }

    public interface ColumnExpressionToIndex {
        public int getIndex(ColumnExpression column) throws StandardException;
    }

    public abstract Expression generateExpression(ColumnExpressionToIndex fieldOffsets) throws StandardException;
}
