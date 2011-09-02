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

import com.akiban.qp.expression.API;
import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.Comparison;

/** A binary comparison (equality / inequality) between two expressions.
 */
public class ComparisonCondition extends BaseExpression implements ConditionExpression 
{
    private BaseExpression left, right;
    private Comparison operation;

    public ComparisonCondition(BaseExpression left, BaseExpression right,
                               Comparison operation, DataTypeDescriptor type) {
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
    public Comparison getOperation() {
        return operation;
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

    public String toString() {
        return left + " " + operation + " " + right;
    }

    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
        return API.compare(left.generateExpression(fieldOffsets),
                           operation,
                           right.generateExpression(fieldOffsets));
    }

    public void reverse() {
        BaseExpression temp = left;
        left = right;
        right = temp;
        operation = reverseComparison(operation);
    }

}
