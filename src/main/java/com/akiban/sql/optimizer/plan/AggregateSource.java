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
import java.util.ArrayList;

public class AggregateSource extends BasePlanWithInput implements ColumnSource
{
    private List<ExpressionNode> groupBy;
    private List<AggregateFunctionExpression> aggregates;

    public AggregateSource(PlanNode input,
                           List<ExpressionNode> groupBy) {
        super(input);
        this.groupBy = groupBy;
        this.aggregates = new ArrayList<AggregateFunctionExpression>();
    }

    public List<ExpressionNode> getGroupBy() {
        return groupBy;
    }
    public List<AggregateFunctionExpression> getAggregates() {
        return aggregates;
    }

    public String getName() {
        return "GROUP";         // TODO: Something unique needed?
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionVisitor) {
                    children:
                    if (v.visitEnter(this)) {
                        for (ExpressionNode child : groupBy) {
                            if (!child.accept((ExpressionVisitor)v))
                                break children;
                        }
                        for (AggregateFunctionExpression child : aggregates) {
                            if (!child.accept((ExpressionVisitor)v))
                                break children;
                        }
                    }
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String toString() {
        return "GROUP BY" + groupBy + aggregates + "\n" + getInput();
    }

}
