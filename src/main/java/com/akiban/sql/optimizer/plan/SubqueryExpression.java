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
import com.akiban.sql.parser.ValueNode;

import com.akiban.qp.expression.Expression;

/** An expression evaluated by a subquery.
 * TODO: Think about nested result sets.
 */
public class SubqueryExpression extends BaseExpression 
{
    private Subquery subquery;

    public SubqueryExpression(Subquery subquery, 
                              DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.subquery = subquery;
    }

    public Subquery getSubquery() {
        return subquery;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SubqueryExpression)) return false;
        SubqueryExpression other = (SubqueryExpression)obj;
        // Currently this is ==; don't match whole subquery.
        return subquery.equals(other.subquery);
    }

    @Override
    public int hashCode() {
        return subquery.hashCode();
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof PlanVisitor)
                subquery.accept((PlanVisitor)v);
        }
        return v.visitLeave(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return subquery.summaryString();
    }

    @Override
    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
        throw new UnsupportedSQLException("NIY", null);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        subquery = (Subquery)subquery.duplicate(map);
    }

}
