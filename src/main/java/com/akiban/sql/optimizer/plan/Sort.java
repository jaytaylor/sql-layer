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

import java.util.List;

/** A key expression in an ORDER BY clause. */
public class Sort extends BasePlanNode
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

        public String toString() {
            if (ascending)
                return super.toString();
            else
                return super.toString() + " DESC";
        }
    }

    private PlanNode input;
    private List<OrderByExpression> orderBy;

    public Sort(PlanNode input, List<OrderByExpression> orderBy) {
        this.input = input;
        input.setOutput(this);
        this.orderBy = orderBy;
    }

    public PlanNode getInput() {
        return input;
    }

    public List<OrderByExpression> getOrderBy() {
        return orderBy;
    }

    @Override
    public String toString() {
        return "SORT " + orderBy + "\n" + getInput();
    }

}
