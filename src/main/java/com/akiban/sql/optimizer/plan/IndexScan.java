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

import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import com.akiban.qp.expression.Comparison;

import com.akiban.ais.model.Index;

import java.util.*;

public class IndexScan extends BasePlanNode implements ColumnExpressionToIndex
{
    public static enum OrderEffectiveness {
        NONE, PARTIAL_GROUPED, GROUPED, SORTED
    }

    private Index index;

    private TableSource leafMostTable, rootMostTable;

    // Conditions subsumed by this index.
    // TODO: any cases where a condition is only partially handled and
    // still needs to be checked with Select?
    private List<ConditionExpression> conditions;

    // First equalities in the order of the index.
    private List<ExpressionNode> equalityComparands;
    // Followed by an optional inequality.
    private ExpressionNode lowComparand, highComparand;
    // TODO: This doesn't work for merging: consider x < ? AND x <= ?. 
    // May need building of index keys in the expressions subsystem.
    private boolean lowInclusive, highInclusive;

    // Columns in order, should the index be used as covering.
    private List<ExpressionNode> columns;

    // This is how the indexed result will be ordered from using this index.
    // TODO: Is this right? Are we allowed to switch directions
    // between segments, in which case something else figures out
    // reverse? Cf. mixed mode in Sort operator cursor.
    private List<OrderByExpression> ordering;
    private boolean reverseScan;

    private OrderEffectiveness orderEffectiveness;

    public IndexScan(Index index, 
                     TableSource leafMostTable, TableSource rootMostTable) {
        this.index = index;
        this.leafMostTable = leafMostTable;
        this.rootMostTable = rootMostTable;
    }

    public Index getIndex() {
        return index;
    }

    public TableSource getLeafMostTable() {
        return leafMostTable;
    }
    public TableSource getRootMostTable() {
        return rootMostTable;
    }

    public List<ConditionExpression> getConditions() {
        return conditions;
    }

    public List<ExpressionNode> getEqualityComparands() {
        return equalityComparands;
    }
    public ExpressionNode getLowComparand() {
        return lowComparand;
    }
    public boolean isLowInclusive() {
        return lowInclusive;
    }
    public ExpressionNode getHighComparand() {
        return highComparand;
    }
    public boolean isHighInclusive() {
        return highInclusive;
    }

    public void addEqualityCondition(ConditionExpression condition, 
                                     ExpressionNode comparand) {
        if (equalityComparands == null)
            equalityComparands = new ArrayList<ExpressionNode>();
        equalityComparands.add(comparand);
        if (conditions == null)
            conditions = new ArrayList<ConditionExpression>();
        conditions.add(condition);
    }

    public void addInequalityCondition(ConditionExpression condition, 
                                       Comparison comparison,
                                       ExpressionNode comparand) {
        if ((comparison == Comparison.GT) || (comparison == Comparison.GE)) {
            if (lowComparand == null) {
                lowComparand = comparand;
                lowInclusive = (comparison == Comparison.GE);
            }
            else if (lowInclusive == (comparison == Comparison.GE)) {
                List<ExpressionNode> operands = new ArrayList<ExpressionNode>(2);
                operands.add(lowComparand);
                operands.add(comparand);
                lowComparand = new FunctionExpression("max", 
                                                      operands,
                                                      lowComparand.getSQLtype(),
                                                      null);
            }
            else
                // TODO: Could do the MAX anyway and test the conditions later.
                // Might take some refactoring to know which
                // conditions are already there.
                return;
        }
        else if ((comparison == Comparison.LT) || (comparison == Comparison.LE)) {
            if (highComparand == null) {
                highComparand = comparand;
                highInclusive = (comparison == Comparison.LE);
            }
            else if (highInclusive == (comparison == Comparison.LE)) {
                List<ExpressionNode> operands = new ArrayList<ExpressionNode>(2);
                operands.add(highComparand);
                operands.add(comparand);
                highComparand = new FunctionExpression("min", 
                                                      operands,
                                                      highComparand.getSQLtype(),
                                                      null);
            }
            else
                // Not really an inequality.
                return;
        }
        else {
            return;
        }
        if (conditions == null)
            conditions = new ArrayList<ConditionExpression>();
        conditions.add(condition);
    }

    public List<ExpressionNode> getColumns() {
        return columns;
    }
    public void setColumns(List<ExpressionNode> columns) {
        this.columns = columns;
    }

    public List<OrderByExpression> getOrdering() {
        return ordering;
    }
    public void setOrdering(List<OrderByExpression> ordering) {
        this.ordering = ordering;
    }
                              
    public OrderEffectiveness getOrderEffectiveness() {
        return orderEffectiveness;
    }
    public void setOrderEffectiveness(OrderEffectiveness orderEffectiveness) {
        this.orderEffectiveness = orderEffectiveness;
    }

    public boolean isReverseScan() {
        return reverseScan;
    }
    public void setReverseScan(boolean reverseScan) {
        if (this.reverseScan != reverseScan) {
            this.reverseScan = reverseScan;
            // And toggle ascending for each column.
            for (OrderByExpression column : ordering) {
                column.setAscending(!column.isAscending());
            }
        }
    }

    @Override
    // For when used as covering index.
    public int getIndex(ColumnExpression column) {
        return columns.indexOf(column);
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            // TODO: Should we visit the tables; we've replaced them, right?
        }
        return v.visitLeave(this);
    }
    
    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        equalityComparands = duplicateList(equalityComparands, map);
        if (lowComparand != null)
            lowComparand = (ConditionExpression)lowComparand.duplicate(map);
        if (highComparand != null)
            highComparand = (ConditionExpression)highComparand.duplicate(map);
        ordering = duplicateList(ordering, map);
    }
    
    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(index);
        str.append(", ");
        str.append(orderEffectiveness);
        if (reverseScan)
            str.append("/reverse");
        if (equalityComparands != null) {
            for (ExpressionNode expression : equalityComparands) {
                str.append(", =");
                str.append(expression);
            }
        }
        if (lowComparand != null) {
            str.append(", ");
            str.append((lowInclusive) ? ">=" : ">");
            str.append(lowComparand);
        }
        if (highComparand != null) {
            str.append(", ");
            str.append((highInclusive) ? "<=" : "<");
            str.append(highComparand);
        }
        str.append(")");
        return str.toString();
    }

}
