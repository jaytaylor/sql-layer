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
import com.akiban.sql.optimizer.rule.CostEstimator;
import com.akiban.sql.optimizer.rule.range.RangeSegment;

import java.util.List;

public final class SingleIndexScan extends IndexScan {

    private Index index;

    public SingleIndexScan(Index index, TableSource table)
    {
        super(table);
        this.index = index;
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
    public List<IndexColumn> getKeyColumns() {
        return getIndex().getKeyColumns();
    }

    @Override
    protected String summarizeIndex() {
        return String.valueOf(index);
    }

    @Override
    protected boolean isAscendingAt(int i) {
        return index.getKeyColumns().get(i).isAscending();
    }

    @Override
    protected CostEstimate createBasicCostEstimate(CostEstimator costEstimator) {
        if (getConditionRange() == null) {
            return costEstimator.costIndexScan(getIndex(),
                    getEqualityComparands(),
                    getLowComparand(),
                    isLowInclusive(),
                    getHighComparand(),
                    isHighInclusive());
        }
        else {
            CostEstimate cost = null;
            for (RangeSegment segment : getConditionRange().getSegments()) {
                CostEstimate acost = costEstimator.costIndexScan(getIndex(),
                        getEqualityComparands(),
                        segment.getStart().getValueExpression(),
                        segment.getStart().isInclusive(),
                        segment.getEnd().getValueExpression(),
                        segment.getEnd().isInclusive());
                if (cost == null)
                    cost = acost;
                else
                    cost = cost.union(acost);
            }
            return cost;
        }
    }
}
