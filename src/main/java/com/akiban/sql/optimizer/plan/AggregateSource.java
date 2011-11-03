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

import com.akiban.server.types.AkType;

import java.util.List;
import java.util.ArrayList;

/** This node has three phases:<ul>
 * <li>After parsing, it only knows the group by fields.</li>
 * <li>From analysis of downstream references, aggregate expressions are filled in.</li>
 * <li>After index select, but before maps are folded, a {@link Project}
 * is split off with input expressions, which are then forgotten.</li></ul>
 */
public class AggregateSource extends BasePlanWithInput implements ColumnSource
{
    public static enum Implementation {
        PRESORTED, PREAGGREGATE_RESORT, SORT, HASH, TREE, UNGROUPED
    }

    private boolean projectSplitOff;
    private List<ExpressionNode> groupBy;
    private List<AggregateFunctionExpression> aggregates;
    private int nGroupBy;
    private List<String> aggregateFunctions;

    private Implementation implementation;

    public AggregateSource(PlanNode input,
                           List<ExpressionNode> groupBy) {
        super(input);
        this.groupBy = groupBy;
        nGroupBy = groupBy.size();
        if (!hasGroupBy())
            implementation = Implementation.UNGROUPED;
        aggregates = new ArrayList<AggregateFunctionExpression>();
    }

    public boolean isProjectSplitOff() {
        return projectSplitOff;
    }

    public boolean hasGroupBy() {
        return (nGroupBy > 0);
    }

    public List<ExpressionNode> getGroupBy() {
        assert !projectSplitOff;
        return groupBy;
    }
    public List<AggregateFunctionExpression> getAggregates() {
        assert !projectSplitOff;
        return aggregates;
    }

    /** Add a new aggregate and return its position. */
    public int addAggregate(AggregateFunctionExpression aggregate) {
        int position = groupBy.size() + aggregates.size();
        aggregates.add(aggregate);
        return position;
    }

    public ExpressionNode getField(int position) {
        assert !projectSplitOff;
        if (position < nGroupBy)
            return groupBy.get(position);
        else
            return aggregates.get(position - nGroupBy);
    }

    public int getNGroupBy() {
        return nGroupBy;
    }

    public int getNAggregates() {
        if (projectSplitOff)
            return aggregateFunctions.size();
        else
            return aggregates.size();
    }

    public int getNFields() {
        return getNGroupBy() + getNAggregates();
    }

    public List<String> getAggregateFunctions() {
        assert projectSplitOff;
        return aggregateFunctions;
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

    public List<ExpressionNode> splitOffProject() {
        List<ExpressionNode> result = new ArrayList<ExpressionNode>(groupBy);
        nGroupBy = groupBy.size();
        groupBy = null;
        aggregateFunctions = new ArrayList<String>(aggregates.size());
        for (AggregateFunctionExpression aggregate : aggregates) {
            String function = aggregate.getFunction();
            ExpressionNode operand = aggregate.getOperand();
            aggregate.setOperand(null);
            if (operand == null) {
                if ("COUNT".equals(function))
                    function = "COUNT(*)";
                operand = new ConstantExpression(1l, AkType.LONG);
            }
            aggregateFunctions.add(function);
            result.add(operand);
        }
        aggregates = null;
        projectSplitOff = true;
        return result;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v) && !projectSplitOff) {
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
        if (projectSplitOff) {
            if (hasGroupBy()) {
                str.append(nGroupBy);
                str.append(",");
            }
            str.append(aggregateFunctions);
        }
        else {
            if (hasGroupBy()) {
                str.append(groupBy);
                str.append(",");
            }
            str.append(aggregates);
        }
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
        if (!projectSplitOff) {
            groupBy = duplicateList(groupBy, map);
            aggregates = duplicateList(aggregates, map);
        }
    }

}
