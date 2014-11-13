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

/** An expression evaluated by a subquery.
 */
public abstract class SubqueryExpression extends BaseExpression 
{
    private Subquery subquery;

    public SubqueryExpression(Subquery subquery, 
                              DataTypeDescriptor sqlType, ValueNode sqlSource,
                              TInstance type) {
        super(sqlType, sqlSource, type);
        this.subquery = subquery;
    }

    public Subquery getSubquery() {
        return subquery;
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
        boolean childrenFirst = v.visitChildrenFirst(this);
        if (!childrenFirst) {
            ExpressionNode result = v.visit(this);
            if (result != this) return result;
        }
        if (v instanceof PlanVisitor)
          subquery.accept((PlanVisitor)v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        return subquery.summaryString(PlanNode.SummaryConfiguration.DEFAULT);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        subquery = (Subquery)subquery.duplicate(map);
    }

}
