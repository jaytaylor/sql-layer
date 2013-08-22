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

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.CharacterTypeAttributes;

import java.util.List;

/** A key expression in an ORDER BY clause. */
public class Sort extends BasePlanWithInput
{
    public static class OrderByExpression extends AnnotatedExpression {
        private boolean ascending;

        public OrderByExpression(ExpressionNode expression, boolean ascending) {
            super(expression);
            this.ascending = ascending;
        }

        public boolean isAscending() {
            return ascending;
        }
        public void setAscending(boolean ascending) {
            this.ascending = ascending;
        }

        public AkCollator getCollator() {
            return getExpression().getCollator();
        }

        public String toString() {
            if (ascending)
                return super.toString();
            else
                return super.toString() + " DESC";
        }
    }

    private List<OrderByExpression> orderBy;

    public Sort(PlanNode input, List<OrderByExpression> orderBy) {
        super(input);
        this.orderBy = orderBy;
    }

    public List<OrderByExpression> getOrderBy() {
        return orderBy;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (OrderByExpression expr : orderBy) {
                        expr.accept((ExpressionRewriteVisitor)v);
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (OrderByExpression expr : orderBy) {
                        if (!expr.getExpression().accept((ExpressionVisitor)v)) {
                            break;
                        }
                    }
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString() {
        return super.summaryString() + orderBy;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        orderBy = duplicateList(orderBy, map);
    }

}
