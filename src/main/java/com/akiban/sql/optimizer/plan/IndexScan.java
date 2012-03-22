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
import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import java.util.*;

public abstract class IndexScan extends BasePlanNode implements IndexIntersectionNode<ConditionExpression,IndexScan>
{
    public static enum OrderEffectiveness {
        NONE, PARTIAL_GROUPED, GROUPED, SORTED
    }

    private TableSource rootMostTable, rootMostInnerTable, leafMostInnerTable, leafMostTable;

    private boolean covering;

    // Tables that would still need to be fetched if this index were used.
    private Set<TableSource> requiredTables;

    // Estimated cost of using this index.
    private CostEstimate costEstimate;
    
    // The cost of just the scan of this index, not counting lookups, flattening, etc
    private CostEstimate scanCostEstimate;

    public IndexScan(TableSource table) {
        rootMostTable = rootMostInnerTable = leafMostInnerTable = leafMostTable = table;
    }

    public IndexScan(TableSource rootMostTable, 
                     TableSource rootMostInnerTable,
                     TableSource leafMostInnerTable,
                     TableSource leafMostTable) {
        this.rootMostTable = rootMostTable;
        this.rootMostInnerTable = rootMostInnerTable;
        this.leafMostInnerTable = leafMostInnerTable;
        this.leafMostTable = leafMostTable;
    }

    public TableSource getRootMostTable() {
        return rootMostTable;
    }
    public TableSource getRootMostInnerTable() {
        return rootMostInnerTable;
    }
    public TableSource getLeafMostInnerTable() {
        return leafMostInnerTable;
    }
    public TableSource getLeafMostTable() {
        return leafMostTable;
    }

    /** Return tables included in the index, leafmost to rootmost. */
    public List<TableSource> getTables() {
        List<TableSource> tables = new ArrayList<TableSource>();
        TableSource table = leafMostTable;
        while (true) {
            tables.add(table);
            if (table == rootMostTable) break;
            table = table.getParentTable();
        }
        return tables;
    }

    public boolean isCovering() {
        return covering;
    }
    public void setCovering(boolean covering) {
        this.covering = covering;
    }

    public Set<TableSource> getRequiredTables() {
        return requiredTables;
    }
    public void setRequiredTables(Set<TableSource> requiredTables) {
        this.requiredTables = requiredTables;
    }

    public CostEstimate getScanCostEstimate() {
        return scanCostEstimate;
    }

    public void setScanCostEstimate(CostEstimate scanCostEstimate) {
        this.scanCostEstimate = scanCostEstimate;
    }

    public CostEstimate getCostEstimate() {
        return costEstimate;
    }
    public void setCostEstimate(CostEstimate costEstimate) {
        this.costEstimate = costEstimate;
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
    }

    public abstract List<OrderByExpression> getOrdering();
    public abstract OrderEffectiveness getOrderEffectiveness();
    public abstract List<ExpressionNode> getColumns();
    public abstract List<IndexColumn> getIndexColumns();
    public abstract List<ConditionExpression> getGroupConditions();
    public abstract List<ExpressionNode> getEqualityComparands();
    public abstract List<ConditionExpression> getConditions();
    public abstract boolean hasConditions();
    public abstract ExpressionNode getLowComparand();
    public abstract boolean isLowInclusive();
    public abstract ExpressionNode getHighComparand();
    public abstract boolean isHighInclusive();
    
    @Override
    public String summaryString() {
        StringBuilder sb = new StringBuilder();
        buildSummaryString(sb, true);
        return sb.toString();
    }
     
    protected void buildSummaryString(StringBuilder str, boolean full) {
        str.append(super.summaryString());
        str.append("(");
        str.append(summarizeIndex());
        str.append(", ");
        if (full && covering)
            str.append("covering/");
        if (full && getOrderEffectiveness() != null)
            str.append(getOrderEffectiveness());
        if (full && getOrdering() != null) {
            boolean anyReverse = false, allReverse = true;
            for (int i = 0; i < getOrdering().size(); i++) {
                if (getOrdering().get(i).isAscending() != isAscendingAt(i))
                    anyReverse = true;
                else
                    allReverse = false;
            }
            if (anyReverse) {
                if (allReverse)
                    str.append("/reverse");
                else {
                    for (int i = 0; i < getOrdering().size(); i++) {
                        str.append((i == 0) ? "/" : ",");
                        str.append(getOrdering().get(i).isAscending() ? "ASC" : "DESC");
                    }
                }
            }
        }
        describeEqualityComparands(str);
        if (getLowComparand() != null) {
            str.append(", ");
            str.append((isLowInclusive()) ? ">=" : ">");
            str.append(getLowComparand());
        }
        if (getHighComparand() != null) {
            str.append(", ");
            str.append((isHighInclusive()) ? "<=" : "<");
            str.append(getHighComparand());
        }
        describeConditionRange(str);
        if (full && costEstimate != null) {
            str.append(", ");
            str.append(costEstimate);
        }
        str.append(")");
    }

    protected void describeConditionRange(StringBuilder output) {}
    protected void describeEqualityComparands(StringBuilder output) {}
    protected abstract String summarizeIndex();
    protected abstract boolean isAscendingAt(int index);
}
