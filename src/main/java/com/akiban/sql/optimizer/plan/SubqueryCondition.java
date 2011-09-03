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
import com.akiban.qp.expression.Comparison;

/** A condition involving rows from a subquery.
 */
public class SubqueryCondition extends BaseExpression implements ConditionExpression
{
    public static enum Kind {
        EXISTS, NOT_EXISTS
    }
    
    private Kind kind;
    private PlanNode subquery;

    public SubqueryCondition(Kind kind, PlanNode subquery, DataTypeDescriptor type) {
        super(type);
        this.kind = kind;
        this.subquery = subquery;
    }

    public Kind getKind() {
        return kind;
    }

    public PlanNode getSubquery() {
        return subquery;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SubqueryCondition)) return false;
        SubqueryCondition other = (SubqueryCondition)obj;
        return ((kind == other.kind) &&
                // Currently this is ==; don't match whole subquery.
                subquery.equals(other.subquery));
    }

    @Override
    public int hashCode() {
        int hash = kind.hashCode();
        hash += subquery.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            /** TODO
            if (v instanceof PlanVisitor)
                subquery.accept((PlanVisitor)v);
            **/
        }
        return v.visitLeave(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return kind + " " + subquery;
    }

    @Override
    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
        throw new UnsupportedSQLException("EXISTS as expression", null);
    }
}
