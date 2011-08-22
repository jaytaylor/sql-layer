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

/** An expression evaluated by a subquery.
 * TODO: Think about nested result sets.
 */
public class SubqueryExpression extends BaseExpression 
{
    private Query subquery;

    public SubqueryExpression(Query subquery, DataTypeDescriptor type) {
        super(type);
        this.subquery = subquery;
    }

    public Query getSubquery() {
        return subquery;
    }

    public String toString() {
        return subquery.toString();
    }

    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) 
            throws StandardException {
        throw new StandardException("NIY");
    }
}
