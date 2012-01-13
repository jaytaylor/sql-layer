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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.*;

import com.akiban.server.store.statistics.IndexStatistics;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;

import java.util.*;

public abstract class CostEstimator
{
    public abstract long getTableRowCount(Table table);
    public abstract IndexStatistics getIndexStatistics(Index index);

    // TODO: Temporary until fully installed.
    public boolean isEnabled() {
        return true;
    }

    // TODO: These need to be figured out for real.
    public static final double RANDOM_ACCESS_COST = 5.0;
    public static final double SEQUENTIAL_ACCESS_COST = 1.0;

    /** Estimate cost of scanning from this index. */
    public CostEstimate costIndexScan(Index index,
                                      List<ExpressionNode> equalityComparands,
                                      ExpressionNode lowComparand, boolean lowInclusive,
                                      ExpressionNode highComparand, boolean highInclusive) {
        if (index.isUnique()) {
            if ((equalityComparands != null) &&
                (equalityComparands.size() == index.getColumns().size())) {
                // Exact match from unique index; probably one row.
                return new CostEstimate(1, RANDOM_ACCESS_COST);
            }
        }
        IndexStatistics indexStats = getIndexStatistics(index);
        long rowCount = getTableRowCount(index.leafMostTable());
        if ((indexStats == null) ||
            (indexStats.getRowCount() == 0) ||
            ((equalityComparands == null) &&
             (lowComparand == null) &&
             (highComparand == null))) {
            // No stats or just used for ordering.
            // TODO: Is this too conservative?
            return new CostEstimate(rowCount, 
                                    RANDOM_ACCESS_COST + (rowCount * SEQUENTIAL_ACCESS_COST));
        }
        double scale = (double)rowCount / indexStats.getRowCount();
        return new CostEstimate(0, 0);
    }

    /** Estimate the cost of starting at the given table's index and
     * fetching the given tables, then joining them with Flatten and
     * Product. */
    // TODO: Lots of overlap with BranchJoiner. Once group joins are
    // picked through the same join enumeration of other kinds, this
    // should be better integrated.
    public CostEstimate costFlatten(TableSource indexTable,
                                    Collection<TableSource> requiredTables,
                                    long repeat) {
        int nbranches = indexTable.getTable().getTree().colorBranches();
        return new CostEstimate(0, 0);
    }

    /** Estimate the cost of a sort of the given size. */
    public CostEstimate costSort(long size) {
        return new CostEstimate(0, 0);
    }
}
