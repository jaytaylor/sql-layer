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

import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.expression.Expression;

/** An operation on Boolean expressions.
 */
public class BooleanOperationExpression extends BaseExpression 
                                        implements ConditionExpression
{
    public static enum Operation {
        AND, OR, NOT
    }

    private ConditionExpression left, right;
    private Operation operation;
    
    public BooleanOperationExpression(Operation operation, 
                                      ConditionExpression left, 
                                      ConditionExpression right, 
                                      DataTypeDescriptor type) {
        super(type);
        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    public Operation getOperation() {
        return operation;
    }
    public ConditionExpression getLeft() {
        return left;
    }
    public ConditionExpression getRight() {
        return right;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BooleanOperationExpression)) return false;
        BooleanOperationExpression other = (BooleanOperationExpression)obj;
        return ((operation == other.operation) &&
                left.equals(other.left) &&
                right.equals(other.right));
    }

    @Override
    public int hashCode() {
        int hash = operation.hashCode();
        hash += left.hashCode();
        hash += right.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (left.accept(v))
                right.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        ExpressionNode result = v.visit(this);
        if (result != this) return result;
        left = (ConditionExpression)left.accept(v);
        right = (ConditionExpression)right.accept(v);
        return this;
    }

    @Override
    public String toString() {
        if (right == null)
            return operation + " " + left;
        else
            return left + " " + operation + " " + right;
    }

    @Override
    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
        throw new UnsupportedSQLException("NIY", null);
    }
}
