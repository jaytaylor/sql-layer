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
import com.akiban.ais.model.Table;
import com.akiban.sql.optimizer.plan.MultiIndexEnumerator.MultiIndexPair;

import java.util.List;
import java.util.Map;

public final class MultiIndexIntersectScan extends IndexScan {
    
    private MultiIndexPair<ComparisonCondition> index;
    private Map<Table,TableSource> branch;
    private IndexScan outputScan;
    private IndexScan selectorScan;

    public MultiIndexIntersectScan(Map<Table,TableSource> branch,
                                   MultiIndexPair<ComparisonCondition> index)
    {
        this(branch.get(index.getOutputIndex().getIndex().rootMostTable()),
                branch.get(index.getOutputIndex().getIndex().leafMostTable()));
        this.index = index;
        this.branch = branch;
    }
    
    private MultiIndexIntersectScan(TableSource rootMost, TableSource leafMost) {
        super(rootMost, rootMost, leafMost, leafMost);
    }
    
    public IndexScan getOutputIndexScan() {
        if (outputScan == null)
            outputScan = createScan(index.getOutputIndex());
        return outputScan;
    }
    
    public IndexScan getSelectorIndexScan() {
        if (selectorScan == null)
            selectorScan = createScan(index.getSelectorIndex());
        return selectorScan;
    }

    public int getComparisonFields() {
        return index.getCommonFieldsCount();
    }

    public int getOutputOrderingFields() {
        return getOrderingFields(outputScan);
    }

    public int getSelectorOrderingFields() {
        return getOrderingFields(selectorScan);
    }

    @Override
    public List<IndexColumn> getKeyColumns() {
        return index.getOutputIndex().getIndex().getKeyColumns();
    }

    @Override
    public List<IndexColumn> getValueColumns() {
        return index.getOutputIndex().getIndex().getValueColumns();
    }

    @Override
    protected String summarizeIndex() {
        return String.valueOf(index);
    }

    @Override
    protected boolean isAscendingAt(int index) {
        throw new UnsupportedOperationException(); // TODO do I use the output index?
    }

    private IndexScan createScan(MultiIndexCandidate<ComparisonCondition> candidate) {
        // TODO this should eventually allow for nesting

        Index idx = candidate.getIndex();
        TableSource rootMost = branch.get(idx.rootMostTable());
        TableSource leafMost = branch.get(idx.leafMostTable());
        
        SingleIndexScan singleScan = new SingleIndexScan(idx, rootMost, rootMost, leafMost, leafMost);
        for (ComparisonCondition cond : candidate.getPegged())
            singleScan.addEqualityCondition(cond, cond.getRight());

        singleScan.setRequiredTables(getRequiredTables());

        return singleScan;
    }

    private int getOrderingFields(IndexScan scan) {
        return scan.getKeyColumns().size() + scan.getValueColumns().size();
    }
}
