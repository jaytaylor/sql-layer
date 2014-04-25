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

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

/**
 * An operation on Boolean expressions.
 */
public class BooleanOperationExpression extends BaseExpression implements
        ConditionExpression {
    public static enum Operation {
        AND("and"), OR("or"), NOT("not");

        private String functionName;

        Operation(String functionName) {
            this.functionName = functionName;
        }

        public String getFunctionName() {
            return functionName;
        }
    }

    private Operation operation;
    private ConditionExpression left, right;

    public BooleanOperationExpression(Operation operation,
            ConditionExpression left, ConditionExpression right,
            DataTypeDescriptor sqlType, ValueNode sqlSource,
            TInstance type) {
        super(sqlType, sqlSource, type);
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
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BooleanOperationExpression))
            return false;
        BooleanOperationExpression other = (BooleanOperationExpression) obj;
        return ((operation == other.operation) && left.equals(other.left) && right
                .equals(other.right));
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
        boolean childrenFirst = v.visitChildrenFirst(this);
        if (!childrenFirst) {
            ExpressionNode result = v.visit(this);
            if (result != this)
                return result;
        }
        left = (ConditionExpression) left.accept(v);
        right = (ConditionExpression) right.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        if (right == null)
            return operation + " " + left;
        else
            return left + " " + operation + " " + right;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (ConditionExpression) left.duplicate(map);
        right = (ConditionExpression) right.duplicate(map);
    }

}
