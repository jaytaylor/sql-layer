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

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.expression.Expression;

import com.akiban.sql.StandardException;

/** An operation on boolean expressions.
 */
public class BooleanOperationExpression extends BooleanExpression
{
    public static enum Operation {
        AND, OR, NOT
    }

    private BaseExpression left, right;
    private Operation operation;
    
    public BooleanOperationExpression(BaseExpression left, BaseExpression right, 
                                      Operation operation, DataTypeDescriptor type) {
        super(type);
        this.left = left;
        this.right = right;
        this.operation = operation;
    }

    public BaseExpression getLeft() {
        return left;
    }
    public BaseExpression getRight() {
        return right;
    }
    public Operation getOperation() {
        return operation;
    }

    public String toString() {
        if (right == null)
            return operation + " " + left;
        else
            return left + " " + operation + " " + right;
    }

    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) 
            throws StandardException {
        throw new StandardException("NIY");
    }
}
