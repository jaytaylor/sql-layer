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
import com.akiban.sql.optimizer.plan.ConditionsCount.HowMany;
import com.akiban.sql.optimizer.rule.range.ColumnRanges;

import java.util.ArrayList;
import java.util.List;

public final class SingleIndexScan extends IndexScan {

    private Index index;
    private ColumnRanges conditionRange;
    // First equalities in the order of the index.
    private List<ExpressionNode> equalityComparands;

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

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        equalityComparands = duplicateList(equalityComparands, map);
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
}
