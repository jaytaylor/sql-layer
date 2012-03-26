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

import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class MultiIndexIntersectScan extends IndexScan {
    
    private IndexScan outputScan;
    private IndexScan selectorScan;
    private int comparisonColumns;
    private List<ConditionExpression> coveringConditions;

    public MultiIndexIntersectScan(IndexScan outerScan, IndexScan selectorScan, int comparisonColumns)
    {
        this(outerScan.getRootMostTable(), outerScan.getLeafMostTable());
        assert outerScan.getRootMostTable() == outerScan.getRootMostInnerTable() : outerScan;
        assert outerScan.getLeafMostTable() == outerScan.getLeafMostInnerTable() : outerScan;
        this.outputScan = outerScan;
        this.selectorScan = selectorScan;
        this.comparisonColumns = comparisonColumns;
    }
    
    private MultiIndexIntersectScan(TableSource rootMost, TableSource leafMost) {
        super(rootMost, rootMost, leafMost, leafMost);
    }
    
    public IndexScan getOutputIndexScan() {
        return outputScan;
    }
    
    public IndexScan getSelectorIndexScan() {
        return selectorScan;
    }

    public int getComparisonFields() {
        return comparisonColumns;
    }

    public boolean[] getComparisonFieldDirections() {
        boolean[] ascending = new boolean[comparisonColumns];
        for (int i = 0; i < comparisonColumns; i++) {
            ascending[i] = getOrdering().get(getPeggedCount() + i).isAscending();
        }
        return ascending;
    }

    public int getOutputOrderingFields() {
        return getOrderingFields(outputScan);
    }

    public int getSelectorOrderingFields() {
        return getOrderingFields(selectorScan);
    }

    private int getOrderingFields(IndexScan scan) {
        return scan.getAllColumns().size() - scan.getPeggedCount();
    }

    @Override
    public List<OrderByExpression> getOrdering() {
        return outputScan.getOrdering();
    }

    @Override
    public OrderEffectiveness getOrderEffectiveness() {
        return outputScan.getOrderEffectiveness();
    }

    @Override
    public List<ConditionExpression> getGroupConditions() {
        return coveringConditions;
    }

    @Override
    public List<ExpressionNode> getEqualityComparands() {
        return outputScan.getEqualityComparands();
    }

    public void setGroupConditions(Collection<ConditionExpression> coveringConditions) {
        this.coveringConditions = new ArrayList<ConditionExpression>(coveringConditions);
    }

    @Override
    public ExpressionNode getLowComparand() {
        return outputScan.getLowComparand();
    }

    @Override
    public boolean isLowInclusive() {
        return outputScan.isLowInclusive();
    }

    @Override
    public ExpressionNode getHighComparand() {
        return outputScan.getHighComparand();
    }

    @Override
    public boolean isHighInclusive() {
        return outputScan.isHighInclusive();
    }

    @Override
    public List<ConditionExpression> getConditions() {
        return outputScan.getConditions();
    }

    @Override
    public boolean hasConditions() {
        return outputScan.hasConditions();
    }

    @Override
    public List<ExpressionNode> getColumns() {
        return outputScan.getColumns();
    }

    @Override
    public UserTable getLeafMostUTable() {
        return outputScan.getLeafMostTable().getTable().getTable();
    }

    @Override
    public List<IndexColumn> getAllColumns() {
        return outputScan.getAllColumns();
    }

    @Override
    public int getPeggedCount() {
        return outputScan.getPeggedCount();
    }

    @Override
    public void incrementConditionsCounter(ConditionsCounter<? super ConditionExpression> counter) {
        outputScan.incrementConditionsCounter(counter);
        selectorScan.incrementConditionsCounter(counter);
    }

    @Override
    public boolean isUseful(ConditionsCount<? super ConditionExpression> count) {
        return outputScan.isUseful(count) && selectorScan.isUseful(count);
    }

    @Override
    public List<IndexColumn> getIndexColumns() {
        return outputScan.getIndexColumns();
    }

    @Override
    protected String summarizeIndex() {
        StringBuilder sb = new StringBuilder("INTERSECT(");
        sb.append(getComparisonFields());
        sb.append(", ");
        outputScan.buildSummaryString(sb, false);
        sb.append(" AND ");
        selectorScan.buildSummaryString(sb, false);
        sb.append(')');
        return sb.toString();
    }

    @Override
    protected boolean isAscendingAt(int i) {
        return outputScan.isAscendingAt(i);
    }

    @Override
    public UserTable findCommonAncestor(IndexScan other) {
        return outputScan.findCommonAncestor(other);
    }
}
