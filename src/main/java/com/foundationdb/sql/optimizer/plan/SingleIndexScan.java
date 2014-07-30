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

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.sql.optimizer.plan.ConditionsCount.HowMany;
import com.foundationdb.sql.optimizer.plan.Sort.OrderByExpression;
import com.foundationdb.sql.optimizer.rule.TypeResolver;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.ConstantFolder.Folder;
import com.foundationdb.sql.optimizer.rule.range.ColumnRanges;

import java.util.ArrayList;
import java.util.List;

public final class SingleIndexScan extends IndexScan implements EqualityColumnsScan {

    private Index index;
    private ColumnRanges conditionRange;
    // First equalities in the order of the index.
    private List<ExpressionNode> equalityComparands;

    // This is how the indexed result will be ordered from using this index.
    private List<OrderByExpression> ordering;

    private OrderEffectiveness orderEffectiveness;
    private boolean usesAllColumns;

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
    
    private PlanContext context; 

    public SingleIndexScan(Index index, TableSource table, PlanContext context)
    {
        super(table);
        this.index = index;
        this.context = context;
    }

    public SingleIndexScan(Index index,
                     TableSource rootMostTable,
                     TableSource rootMostInnerTable,
                     TableSource leafMostInnerTable,
                     TableSource leafMostTable,
                     PlanContext context)
    {
        super(rootMostTable, rootMostInnerTable, leafMostInnerTable, leafMostTable);
        this.index = index;
        this.context = context;
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
            equalityComparands = new ArrayList<>();
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
                List<ExpressionNode> operands = new ArrayList<>(2);
                operands.add(lowComparand);
                operands.add(comparand);
                lowComparand = new FunctionExpression("_max",
                        operands,
                        lowComparand.getSQLtype(),
                        null,
                        lowComparand.getType());
                setPreptimeValue (lowComparand);
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
                List<ExpressionNode> operands = new ArrayList<>(2);
                operands.add(highComparand);
                operands.add(comparand);
                highComparand = new FunctionExpression("_min",
                        operands,
                        highComparand.getSQLtype(),
                        null,
                        highComparand.getType());
                setPreptimeValue (highComparand);
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

    private void setPreptimeValue (ExpressionNode expression) {
        TypeResolver.ResolvingVisitor visitor =
                new TypeResolver.ResolvingVisitor(context, new Folder(context));
        visitor.visit(expression);
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

    public void setLowComparand(ExpressionNode comparand, boolean inclusive) {
        lowComparand = comparand;
        lowInclusive = inclusive;
    }

    public void setHighComparand(ExpressionNode comparand, boolean inclusive) {
        highComparand = comparand;
        highInclusive = inclusive;
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
    public int getNKeyColumns() {
        return index.getKeyColumns().size();
    }

    @Override
    public boolean usesAllColumns() {
        return usesAllColumns;
    }

    @Override
    public void setUsesAllColumns(boolean usesAllColumns) {
        this.usesAllColumns = usesAllColumns;
    }

    @Override
    protected String summarizeIndex(int indentation) {
        return String.valueOf(index);
    }

    @Override
    public boolean isAscendingAt(int i) {
        if (index.isSpatial()) {
            int firstSpatialColumn = index.firstSpatialArgument();
            if (i == firstSpatialColumn)
                return true;
            if (i > firstSpatialColumn)
                i += index.dimensions() - 1;
        }
        return index.getAllColumns().get(i).isAscending();
    }

    @Override
    public boolean isRecoverableAt(int i) {
        if (index.isSpatial()) {
            int firstSpatialColumn = index.firstSpatialArgument();
            if (i == firstSpatialColumn)
                return false;
            if (i > firstSpatialColumn)
                i += index.dimensions() - 1;
        }
        return index.getAllColumns().get(i).isRecoverable();
    }

    @Override
    public Table getLeafMostAisTable() {
        return index.leafMostTable();
    }

    @Override
    public List<IndexColumn> getAllColumns() {
        return index.getAllColumns();
    }

    @Override
    public int getNEquality() {
        int nequals = 0;
        if (equalityComparands != null)
            nequals = equalityComparands.size();
        return nequals;
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
    public Table findCommonAncestor(IndexScan otherScan) {
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
            conditions = new ArrayList<>();
        return conditions;
    }

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
        if (equalityComparands != null) {
            for (int i = 0; i < equalityComparands.size(); i++) {
                if (equalityComparands.get(i) != null)
                    equalityComparands.set(i, equalityComparands.get(i).accept(v));
            }
        }
        if (lowComparand != null)
            lowComparand = lowComparand.accept(v);
        if (highComparand != null)
            highComparand = highComparand.accept(v);
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
        if (equalityComparands != null) {
            for (ExpressionNode comparand : equalityComparands) {
                if (comparand != null)
                    comparand.accept(v);
            }
        }
        if (lowComparand != null)
            lowComparand.accept(v);
        if (highComparand != null)
            highComparand.accept(v);
    }

}
