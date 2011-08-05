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
import com.akiban.qp.expression.API;

import com.akiban.sql.types.DataTypeDescriptor;

/** An operand with a constant value. */
public class ConstantExpression extends BaseExpression 
{
    private Object value;

    public ConstantExpression(Object value, DataTypeDescriptor type) {
        super(type);
        if (value instanceof Integer)
            value = new Long(((Integer)value).intValue());
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public String toString() {
        return value.toString();
    }

    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
        return API.literal(value);
    }
}
