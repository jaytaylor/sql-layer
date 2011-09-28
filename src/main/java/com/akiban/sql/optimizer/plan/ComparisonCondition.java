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

import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** A binary comparison (equality / inequality) between two expressions.
 */
public class ComparisonCondition extends BaseExpression implements ConditionExpression 
{
    private Comparison operation;
    private ExpressionNode left, right;
    private Implementation implementation;

    public ComparisonCondition(Comparison operation,
                               ExpressionNode left, ExpressionNode right,
                               DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, AkType.BOOL, sqlSource);
        this.operation = operation;
        this.left = left;
        this.right = right;
        this.implementation = Implementation.NORMAL;
    }

    public Comparison getOperation() {
        return operation;
    }
    public ExpressionNode getLeft() {
        return left;
    }
    public ExpressionNode getRight() {
        return right;
    }

    @Override
    public Implementation getImplementation() {
        return implementation;
    }
    public void setImplementation(Implementation implementation) {
        this.implementation = implementation;
    }

    public static Comparison reverseComparison(Comparison operation) {
        switch (operation) {
        case EQ:
        case NE:
            return operation;
        case LT:
            return Comparison.GT;
        case LE:
            return Comparison.GE;
        case GT:
            return Comparison.LT;
        case GE:
            return Comparison.LE;
        default:
            assert false : operation;
            return null;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ComparisonCondition)) return false;
        ComparisonCondition other = (ComparisonCondition)obj;
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
        boolean childrenFirst = v.visitChildrenFirst(this);
        if (!childrenFirst) {
            ExpressionNode result = v.visit(this);
            if (result != this) return result;
        }
        left = left.accept(v);
        right = right.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        return left + " " + operation + " " + right;
    }

    public void reverse() {
        ExpressionNode temp = left;
        left = right;
        right = temp;
        operation = reverseComparison(operation);
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        // Index and join are likely to be pointed to elsewhere.
        return (implementation != Implementation.NORMAL);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (ExpressionNode)left.duplicate();
        right = (ExpressionNode)right.duplicate();
    }

}
