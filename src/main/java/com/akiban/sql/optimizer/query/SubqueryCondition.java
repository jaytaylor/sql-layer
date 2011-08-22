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

import com.akiban.sql.StandardException;

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.Comparison;

/** A condition involving rows from a subquery.
 * May also include a [in]equality condition with the outer query.
 */
public class SubqueryCondition extends BooleanExpression
{
    public static enum Kind {
        EXISTS, NOT_EXISTS, ANY, ALL
    }
    
    private Kind kind;
    private BaseExpression left;
    private Comparison operation;
    private Query subquery;

    public SubqueryCondition(Kind kind, BaseExpression left, 
                             Comparison operation, Query subquery, 
                             DataTypeDescriptor type) {
        super(type);
        this.kind = kind;
        this.left = left;
        this.operation = operation;
        this.subquery = subquery;
    }

    public Kind getKind() {
        return kind;
    }

    public BaseExpression getLeft() {
        return left;
    }

    public Comparison getOperation() {
        return operation;
    }

    public Query getSubquery() {
        return subquery;
    }

    public String toString() {
        if (operation != null)
            return left + " " + operation + " " + kind + " " + subquery;
        else
            return kind + " " + subquery;
    }

    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) 
            throws StandardException {
        throw new StandardException("NIY");
    }
}
