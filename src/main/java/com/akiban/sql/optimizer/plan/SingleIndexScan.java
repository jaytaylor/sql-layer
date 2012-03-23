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

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.ConditionsCount.HowMany;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.rule.range.ColumnRanges;

import java.util.ArrayList;
import java.util.List;

public final class SingleIndexScan extends IndexScan {

    private Index index;
    private ColumnRanges conditionRange;
    // First equalities in the order of the index.
    private List<ExpressionNode> equalityComparands;

    // This is how the indexed result will be ordered from using this index.
    private List<OrderByExpression> ordering;

    private OrderEffectiveness orderEffectiveness;

    // Conditions subsumed by this index.
    // TODO: any cases where a condition is only partially handled and
    // still needs to be checked with Select?
    private List<ConditionExpression> conditions;
    
    // Columns in order, should the index be used as covering.
    private List<ExpressionNode> columns;

    // Followed by an optional inequality.
    private ExpressionNode lowComparand, highComparand;
    // TODO: This doesn't work for merging: consider x < ? AND x <= ?.
    // May need building of index keys in the expressions subsystem.
    private boolean lowInclusive, highInclusive;

    public SingleIndexScan(Index index, TableSource table)
    {
        super(table);
        this.index = index;
    }
    
    public SingleIndexScan(Index index, TableSource rootMost, TableSource leafMost) {
        this(index, rootMost, rootMost, leafMost, leafMost);
    }

    public SingleIndexScan(Index index,
                     TableSource rootMostTable,
                     TableSource rootMostInnerTable,
                     TableSource leafMostInnerTable,
                     TableSource leafMostTable)
    {
        super(rootMostTable, rootMostInnerTable, leafMostInnerTable, leafMostTable);
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public List<ExpressionNode> getColumns() {
        return columns;
    }

    public void setColumns(List<ExpressionNode> columns) {
        this.columns = columns;
    }

    public ColumnRanges getConditionRange() {
        return conditionRange;
    }

    public void addRangeCondition(ColumnRanges range) {
        assert conditionRange == null : conditionRange;
        conditionRange = range;
        internalGetConditions().addAll(range.getConditions());
    }

    public List<ExpressionNode> getEqualityComparands() {
        return equalityComparands;
    }

    public void addEqualityCondition(ConditionExpression condition,
                                     ExpressionNode comparand) {
        if (equalityComparands == null)
            equalityComparands = new ArrayList<ExpressionNode>();
        equalityComparands.add(comparand);
        internalGetConditions().add(condition);
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

        internalGetConditions().add(condition);
    }

    @Override
    public ExpressionNode getLowComparand() {
        return lowComparand;
    }

    @Override
    public boolean isLowInclusive() {
        return lowInclusive;
    }

    @Override
    public ExpressionNode getHighComparand() {
        return highComparand;
    }

    @Override
    public boolean isHighInclusive() {
        return highInclusive;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        if (lowComparand != null)
            lowComparand = (ConditionExpression)lowComparand.duplicate(map);
        if (highComparand != null)
            highComparand = (ConditionExpression)highComparand.duplicate(map);
        equalityComparands = duplicateList(equalityComparands, map);
        ordering = duplicateList(ordering, map);
    }

    @Override
    public List<OrderByExpression> getOrdering() {
        return ordering;
    }

    public void setOrdering(List<OrderByExpression> ordering) {
        this.ordering = ordering;
    }

    public List<ConditionExpression> getConditions() {
        return conditions;
    }

    public boolean hasConditions() {
        return ((conditions != null) && !conditions.isEmpty());
    }

    @Override
    public OrderEffectiveness getOrderEffectiveness() {
        return orderEffectiveness;
    }

    public void setOrderEffectiveness(OrderEffectiveness orderEffectiveness) {
        this.orderEffectiveness = orderEffectiveness;
    }

    @Override
    public List<IndexColumn> getIndexColumns() {
        return index.getAllColumns();
    }

    @Override
    protected String summarizeIndex() {
        return String.valueOf(index);
    }

    @Override
    protected boolean isAscendingAt(int i) {
        return index.getAllColumns().get(i).isAscending();
    }

    @Override
    public List<ConditionExpression> getGroupConditions() {
        return getConditions();
    }

    @Override
    public UserTable getLeafMostUTable() {
        return (UserTable) index.leafMostTable();
    }

    @Override
    public List<IndexColumn> getAllColumns() {
        return index.getAllColumns();
    }

    @Override
    public int getPeggedCount() {
        // Note! Really what we want are the *leading* equalities. But this method is only
        // used in the context of MultiIndexEnumerator, which will only put in leading
        // equalities.
        List<ExpressionNode> equalityComparands = getEqualityComparands();
        return (equalityComparands == null) ? 0 : equalityComparands.size();
    }

    @Override
    public void incrementConditionsCounter(ConditionsCounter<? super ConditionExpression> counter) {
        for (ConditionExpression cond : getConditions())
            counter.increment(cond);
    }

    @Override
    public boolean isUseful(ConditionsCount<? super ConditionExpression> count) {
        for (ConditionExpression cond : getConditions()) {
            if (count.getCount(cond) == HowMany.ONE)
                return true;
        }
        return false;
    }

    @Override
    public UserTable findCommonAncestor(IndexScan otherScan) {
        TableSource myTable = getLeafMostTable();
        TableSource otherTable = otherScan.getLeafMostTable();
        int myDepth = myTable.getTable().getDepth();
        int otherDepth = otherTable.getTable().getDepth();
        while (myDepth > otherDepth) {
            myTable = myTable.getParentTable();
            myDepth = myTable.getTable().getDepth();
        }
        while (otherDepth > myDepth) {
            otherTable = otherTable.getParentTable();
            otherDepth = otherTable.getTable().getDepth();
        }
        while (myTable != otherTable) {
            myTable = myTable.getParentTable();
            otherTable = otherTable.getParentTable();
        }
        return myTable.getTable().getTable();
    }

    @Override
    protected void describeConditionRange(StringBuilder output) {
        if (conditionRange != null) {
            output.append(", UNIONs of ");
            output.append(conditionRange.describeRanges());
        }
    }

    @Override
    protected void describeEqualityComparands(StringBuilder output) {
        if (equalityComparands != null) {
            for (ExpressionNode expression : equalityComparands) {
                output.append(", =");
                output.append(expression);
            }
        }
    }

    private List<ConditionExpression> internalGetConditions() {
        if (conditions == null)
            conditions = new ArrayList<ConditionExpression>();
        return conditions;
    }
}
