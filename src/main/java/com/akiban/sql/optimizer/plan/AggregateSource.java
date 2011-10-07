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
    public static enum Implementation {
        PRESORTED, PREAGGREGATE_RESORT, SORT, HASH, UNGROUPED
    }

    private List<ExpressionNode> groupBy;
    private List<AggregateFunctionExpression> aggregates;

    private Implementation implementation;

    public AggregateSource(PlanNode input,
                           List<ExpressionNode> groupBy) {
        super(input);
        this.groupBy = groupBy;
        if (!hasGroupBy())
            implementation = Implementation.UNGROUPED;
        this.aggregates = new ArrayList<AggregateFunctionExpression>();
    }

    public boolean hasGroupBy() {
        return !groupBy.isEmpty();
    }

    public List<ExpressionNode> getGroupBy() {
        return groupBy;
    }
    public List<AggregateFunctionExpression> getAggregates() {
        return aggregates;
    }

    /** Add a new aggregate and return its position. */
    public int addAggregate(AggregateFunctionExpression aggregate) {
        int position = groupBy.size() + aggregates.size();
        aggregates.add(aggregate);
        return position;
    }

    public ExpressionNode getField(int position) {
        if (position < groupBy.size())
            return groupBy.get(position);
        else
            return aggregates.get(position - groupBy.size());
    }

    public Implementation getImplementation() {
        return implementation;
    }
    public void setImplementation(Implementation implementation) {
        this.implementation = implementation;
    }

    public String getName() {
        return "GROUP";         // TODO: Something unique needed?
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (int i = 0; i < groupBy.size(); i++) {
                        groupBy.set(i, groupBy.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                    for (int i = 0; i < aggregates.size(); i++) {
                        aggregates.set(i, (AggregateFunctionExpression)aggregates.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    children:
                    {
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
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        if (implementation != null) {
            str.append(implementation);
            str.append(",");
        }
        if (hasGroupBy()) {
            str.append(groupBy);
            str.append(",");
        }
        str.append(aggregates);
        str.append(")");
        return str.toString();
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        groupBy = duplicateList(groupBy, map);
        aggregates = duplicateList(aggregates, map);
    }

}
