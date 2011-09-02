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

import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.API;

/** An operand with a parameter value. */
public class ParameterExpression extends BaseExpression 
{
    private int position;

    public ParameterExpression(int position, DataTypeDescriptor type) {
        super(type);
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    public String toString() {
        return "$" + position;
    }

    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
        return API.variable(position);
    }
}
