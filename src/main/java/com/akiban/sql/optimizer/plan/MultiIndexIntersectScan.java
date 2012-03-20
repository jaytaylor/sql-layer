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
    public List<ConditionExpression> getGroupConditions() {
        return coveringConditions;
    }
    
    public void setGroupConditions(List<ConditionExpression> coveringConditions) {
        this.coveringConditions = coveringConditions;
    }

    @Override
    public List<ConditionExpression> getConditions() {
        return outputScan.getConditions();
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
    public boolean removeCoveredConditions(Collection<? super ConditionExpression> conditions,
                                           Collection<? super ConditionExpression> removeTo) {
        // using a bitwise or on purpose here -- we do NOT want to short-circuit this, since even if the left
        // covers some conditions, we want to know which ones the right covers.
        return outputScan.removeCoveredConditions(conditions, removeTo)
                | selectorScan.removeCoveredConditions(conditions, removeTo);
    }

    @Override
    public List<IndexColumn> getIndexColumns() {
        return outputScan.getIndexColumns();
    }

    @Override
    protected String summarizeIndex() {
        StringBuilder sb = new StringBuilder("INTERSECT(");
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
