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
import com.foundationdb.server.types.texpressions.TValidatedAggregator;

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
        PRESORTED, PREAGGREGATE_RESORT, SORT, HASH, TREE, UNGROUPED,
        COUNT_STAR, COUNT_TABLE_STATUS, FIRST_FROM_INDEX
    }

    private boolean projectSplitOff;
    private List<ExpressionNode> groupBy;
    private List<AggregateFunctionExpression> aggregates;
    private List<Object> options;
    private int nGroupBy;
    private List<String> aggregateFunctions;
    private List<ResolvableExpression<TValidatedAggregator>> resolvedFunctions;
    
    private TableSource table;

    private Implementation implementation;

    public AggregateSource(PlanNode input,
                           List<ExpressionNode> groupBy) {
        super(input);
        this.groupBy = groupBy;
        nGroupBy = groupBy.size();
        if (!hasGroupBy())
            implementation = Implementation.UNGROUPED;
        aggregates = new ArrayList<>();
        options = new ArrayList<>();
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

    /** Add a new grouping field and return its position. */
    public int addGroupBy(ExpressionNode expr) {
        assert !projectSplitOff;
        int position = nGroupBy++;
        assert (position == groupBy.size());
        groupBy.add(expr);
        return position;
    }

    /** Add a new aggregate and return its position. */
    public int addAggregate(AggregateFunctionExpression aggregate) {
        int position = groupBy.size() + aggregates.size();
        aggregates.add(aggregate);
        options.add(aggregate.getOption());
        return position;
    }

    public boolean hasAggregate(AggregateFunctionExpression aggregate) {
        if (aggregates.contains(aggregate)) 
            return true;
        return false;
    }

    public int getPosition(AggregateFunctionExpression aggregate) {
        return groupBy.size() + aggregates.indexOf(aggregate);
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

    public List<ResolvableExpression<TValidatedAggregator>> getResolved() {
        assert projectSplitOff;
        return resolvedFunctions;
    }

    public List<Object> getOptions()
    {
        assert projectSplitOff;
        return options;
    }

    public TableSource getTable() {
        assert (implementation == Implementation.COUNT_TABLE_STATUS);
        return table;
    }
    public void setTable(TableSource table) {
        assert (implementation == Implementation.COUNT_TABLE_STATUS);
        this.table = table;
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
        List<ExpressionNode> result = new ArrayList<>(groupBy);
        nGroupBy = groupBy.size();
        groupBy = null;
        aggregateFunctions = new ArrayList<>(aggregates.size());
        resolvedFunctions = new ArrayList<>(aggregates.size());
        for (AggregateFunctionExpression aggregate : aggregates) {
            String function = aggregate.getFunction();
            ExpressionNode operand = aggregate.getOperand();
            aggregate.setOperand(null);
            if (operand == null) {
                if ("COUNT".equals(function))
                    function = "COUNT(*)";
                TInstance type = aggregate.getType(); // Some kind of BIGINT.
                operand = new ConstantExpression(1L, type);
            }
            aggregateFunctions.add(function);
            resolvedFunctions.add(aggregate);
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
