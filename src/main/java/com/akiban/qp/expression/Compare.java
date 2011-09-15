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

package com.akiban.qp.expression;

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.Row;

class Compare implements Expression
{
    // Predicate interface

    @Override
    public Object evaluate(Row row, Bindings bindings)
    {
        Comparable leftValue = (Comparable) left.evaluate(row, bindings);
        Comparable rightValue = (Comparable) right.evaluate(row, bindings);
        if (leftValue == null || rightValue == null) {
            return false;
        }
        int c = leftValue.compareTo(rightValue);
        switch (comparison) {
            case EQ:
                return boolAsInt(c == 0);
            case NE:
                return boolAsInt(c != 0);
            case LT:
                return boolAsInt(c < 0);
            case LE:
                return boolAsInt(c <= 0);
            case GT:
                return boolAsInt(c > 0);
            case GE:
                return boolAsInt(c >= 0);
            default:
                assert false : row;
                return false;
        }
    }

    @Override
    public String toString()
    {
        return left + " " + comparison + " " + right;
    }

    // Compare predicate

    Compare(Expression left, Comparison comparison, Expression right)
    {
        this.left = left;
        this.right = right;
        this.comparison = comparison;
    }

    // For use by this class

    private long boolAsInt(boolean boolValue)
    {
        return boolValue ? 1 : 0;
    }

    // Object state

    private final Expression left;
    private final Expression right;
    private final Comparison comparison;
}
