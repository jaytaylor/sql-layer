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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class MultiIndexIntersectScan extends IndexScan {
    
    private IndexScan outputScan;
    private IndexScan selectorScan;
    private int comparisonColumns;
    private List<ConditionExpression> conditions;

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
        return scan.getKeyColumns().size() + scan.getValueColumns().size();
    }

    @Override
    public List<ConditionExpression> getConditions() {
        if (conditions == null) {
            conditions = new ArrayList<ConditionExpression>();
            buildConditions(this, conditions);
        }
        return conditions;
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
    public void removeCoveredConditions(Set<? super ComparisonCondition> conditions) {
        outputScan.removeCoveredConditions(conditions);
        selectorScan.removeCoveredConditions(conditions);
    }

    @Override
    public List<IndexColumn> getKeyColumns() {
        return outputScan.getKeyColumns();
    }

    @Override
    public List<IndexColumn> getValueColumns() {
        return outputScan.getValueColumns();
    }

    @Override
    protected String summarizeIndex() {
        return String.format("INTERSECT(%s AND %s)", outputScan, selectorScan);
    }

    @Override
    protected boolean isAscendingAt(int i) {
        return getOutputIndexScan().isAscendingAt(i);
    }

    private void buildConditions(IndexScan child, List<ConditionExpression> output) {
        if (child instanceof SingleIndexScan) {
            output.addAll(child.getConditions());
        }
        else if (child instanceof MultiIndexIntersectScan) {
            MultiIndexIntersectScan miis = (MultiIndexIntersectScan) child;
            buildConditions(miis.outputScan, output);
            buildConditions(miis.selectorScan, output);
        }
    }
    
}
