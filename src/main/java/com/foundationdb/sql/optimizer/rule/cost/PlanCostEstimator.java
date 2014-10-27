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

package com.foundationdb.sql.optimizer.rule.cost;

import com.foundationdb.server.spatial.BoxLatLon;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator.IndexIntersectionCoster;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator.SelectivityConditions;
import com.foundationdb.sql.optimizer.rule.range.RangeSegment;
import static com.foundationdb.sql.optimizer.rule.OperatorAssembler.INSERTION_SORT_MAX_LIMIT;
import static com.foundationdb.sql.optimizer.rule.cost.CostEstimator.simpleRound;

import com.foundationdb.sql.optimizer.plan.*;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.geophile.z.Space;
import com.geophile.z.SpatialObject;

import java.math.BigDecimal;
import java.util.*;

public class PlanCostEstimator
{
    protected CostEstimator costEstimator;
    private PlanEstimator planEstimator;

    public PlanCostEstimator(CostEstimator costEstimator) {
        this.costEstimator = costEstimator;
    }

    public CostEstimate getCostEstimate() {
        return costEstimator.adjustCostEstimate(planEstimator.getCostEstimate());
    }

    public static final long NO_LIMIT = -1;

    public void setLimit(long limit) {
        planEstimator.setLimit(limit);
    }

    public void indexScan(IndexScan index) {
        planEstimator = new IndexScanEstimator(index);
    }

    public void spatialIndex(SingleIndexScan index) {
        planEstimator = new SpatialIndexEstimator(index);
    }

    public void flatten(TableGroupJoinTree tableGroup,
                        TableSource indexTable,
                        Set<TableSource> requiredTables) {
        planEstimator = new FlattenEstimator(planEstimator, 
                                             tableGroup, indexTable, requiredTables);
    }

    public void groupScan(GroupScan scan,
                          TableGroupJoinTree tableGroup,
                          Set<TableSource> requiredTables) {
        planEstimator = new GroupScanEstimator(scan, tableGroup, requiredTables);
    }

    public void groupLoop(GroupLoopScan scan,
                          TableGroupJoinTree tableGroup,
                          Set<TableSource> requiredTables) {
        planEstimator = new GroupLoopEstimator(scan, tableGroup, requiredTables);
    }

    public void hKeyRow(ExpressionsHKeyScan scan) {
        planEstimator = new HKeyRowEstimator(scan);
    }

    public void fullTextScan(FullTextScan scan) {
        planEstimator = new FullTextScanEstimator(scan);
    }

    public void select(Collection<ConditionExpression> conditions,
                       SelectivityConditions selectivityConditions) {
        planEstimator = new SelectEstimator(planEstimator,
                                            conditions, selectivityConditions);
    }

    public void sort(int nfields) {
        planEstimator = new SortEstimator(planEstimator, nfields);
    }

    protected abstract class PlanEstimator {
        protected PlanEstimator input;
        protected CostEstimate costEstimate = null;
        protected long limit = NO_LIMIT;

        protected PlanEstimator(PlanEstimator input) {
            this.input = input;
        }

        protected CostEstimate getCostEstimate() {
            if (costEstimate == null) {
                estimateCost();
            }
            return costEstimate;
        }

        protected abstract void estimateCost();

        protected void setLimit(long limit) {
            this.limit = limit;
            costEstimate = null;    // Invalidate any previously computed cost.
            // NB: not passed to input; only set on end, which must then
            // propagate some limit back accordingly.
        }

        protected boolean hasLimit() {
            return (limit > 0);
        }

        protected CostEstimate inputCostEstimate() {
            return input.getCostEstimate();
        }

    }

    protected class IndexScanEstimator extends PlanEstimator {
        IndexScan index;

        protected IndexScanEstimator(IndexScan index) {
            super(null);
            this.index = index;
        }

        @Override
        protected void estimateCost() {
            costEstimate = getScanOnlyCost(index);
            long totalCount = costEstimate.getRowCount();
            if (hasLimit() && (limit < totalCount)) {
                if (index instanceof SingleIndexScan) {
                    SingleIndexScan single = (SingleIndexScan)index;
                    if (single.getConditionRange() == null) {
                        costEstimate = costEstimator.costIndexScan(single.getIndex(), 
                                                                   limit);
                        return;
                    }
                }
                // Multiple scans are involved; assume proportional.
                double setupCost = getScanSetupCost(index);
                double scanCost = costEstimate.getCost() - setupCost;
                costEstimate = new CostEstimate(limit,
                                                setupCost + scanCost * limit / totalCount);
            }
        }
    }

    protected class SpatialIndexEstimator extends PlanEstimator {
        SingleIndexScan index;

        protected SpatialIndexEstimator(SingleIndexScan index) {
            super(null);
            this.index = index;
        }

        @Override
        protected void estimateCost() {
            int nscans = 1;
            FunctionExpression func = (FunctionExpression)index.getLowComparand();
            List<ExpressionNode> operands = func.getOperands();
            Space space = index.getIndex().space();
            if ("_center".equals(func.getFunction())) {
                nscans = 2;     // One in each direction.
                costEstimate = costEstimator.costIndexScan(index.getIndex(),
                                                           index.getEqualityComparands(),
                                                           null, true,
                                                           null, true);
            } else if ("_center_radius".equals(func.getFunction())) {
                BigDecimal lat = decimalConstant(operands.get(0));
                BigDecimal lon = decimalConstant(operands.get(1));
                BigDecimal r = decimalConstant(operands.get(2));
                if ((lat != null) && (lon != null) && (r != null)) {
                    SpatialObject box = BoxLatLon.newBox(lat.subtract(r).doubleValue(),
                                                         lat.add(r).doubleValue(),
                                                         lon.subtract(r).doubleValue(),
                                                         lon.add(r).doubleValue());
                    long[] zValues = new long[box.maxZ()];
                    space.decompose(box, zValues);
                    for (int i = 0; i < box.maxZ(); i++) {
                        long z = zValues[i];
                        if (z != -1L) {
                            ExpressionNode lo = new ConstantExpression(Space.zLo(z), InternalIndexTypes.LONG.instance(true));
                            ExpressionNode hi =  new ConstantExpression(Space.zHi(z), InternalIndexTypes.LONG.instance(true));
                            CostEstimate zScanCost =
                                costEstimator.costIndexScan(index.getIndex(), index.getEqualityComparands(),
                                                            lo, true,
                                                            hi, true);
                            costEstimate =
                                costEstimate == null
                                ? zScanCost
                                : costEstimate.union(zScanCost);
                        }
                    }
                } else {
                    throw new AkibanInternalException("Operands for spatial index must all be constant numbers: " + func);
                }
            } else {
                throw new AkibanInternalException("Unexpected function for spatial index: " + func);
            }
            index.setScanCostEstimate(costEstimate);
            long totalRows = costEstimate.getRowCount();
            long nrows = totalRows;
            if (hasLimit() && (limit < totalRows)) {
                nrows = limit;
            }
            if (nscans == 1) {
                if (nrows != totalRows)
                    costEstimate = costEstimator.costIndexScan(index.getIndex(), nrows);
                return;
            }
            double setupCost = costEstimator.costIndexScan(index.getIndex(), 0).getCost();
            double scanCost = costEstimate.getCost() - setupCost;
            costEstimate = new CostEstimate(limit,
                                            setupCost * nscans +
                                            scanCost * nrows / totalRows);
        }
    }

    protected static BigDecimal decimalConstant(ExpressionNode expr) {
        // Because the distance_lat_lon function returns a double, the radius
        // may be one for comparison.
        // Also numbers may accidentally be given as integers due to formatting.
        while (expr instanceof CastExpression) {
            expr = ((CastExpression)expr).getOperand();
        }
        if (!(expr instanceof ConstantExpression)) return null;
        Object obj = ((ConstantExpression)expr).getValue();
        if (obj instanceof BigDecimalWrapper)
            obj = ((BigDecimalWrapper)obj).asBigDecimal();
        if (obj instanceof BigDecimal)
            return (BigDecimal)obj;
        else if (obj instanceof Number)
            return BigDecimal.valueOf((long)(((Number)obj).doubleValue() * 1.0e6), 6);
        else
            return null;
    }

    protected class FlattenEstimator extends PlanEstimator {
        private TableGroupJoinTree tableGroup;
        private TableSource indexTable;
        private Set<TableSource> requiredTables;

        protected FlattenEstimator(PlanEstimator input,
                                   TableGroupJoinTree tableGroup,
                                   TableSource indexTable,
                                   Set<TableSource> requiredTables) {
            super(input);
            this.tableGroup = tableGroup;
            this.indexTable = indexTable;
            this.requiredTables = requiredTables;
        }

        @Override
        protected void estimateCost() {
            CostEstimate flattenCost = costEstimator.costFlatten(tableGroup, indexTable, requiredTables);
            long flattenScale = flattenCost.getRowCount();
            input.setLimit(hasLimit() ? 
                           // Ceiling number of inputs needed to get limit flattened.
                           ((limit + flattenScale - 1) / flattenScale) : 
                           NO_LIMIT);
            costEstimate = inputCostEstimate().nest(flattenCost);
        }
    }

    protected class GroupScanEstimator extends PlanEstimator {
        private GroupScan scan;
        private TableGroupJoinTree tableGroup;
        private Set<TableSource> requiredTables;

        protected GroupScanEstimator(GroupScan scan,
                                     TableGroupJoinTree tableGroup,
                                     Set<TableSource> requiredTables) {
            super(null);
            this.scan = scan;
            this.tableGroup = tableGroup;
            this.requiredTables = requiredTables;
        }

        @Override
        protected void estimateCost() {
            if (hasLimit()) {
                Map<Table,Long> tableCounts = groupScanTableCountsToLimit(requiredTables, limit);
                if (tableCounts != null) {
                    costEstimate = costEstimator.costPartialGroupScanAndFlatten(tableGroup, requiredTables, tableCounts);
                    return;
                }
            }
            CostEstimate scanCost = costEstimator.costGroupScan(scan.getGroup().getGroup());
            CostEstimate flattenCost = costEstimator.costFlattenGroup(tableGroup, requiredTables);
            costEstimate = scanCost.sequence(flattenCost);
        }
    }

    protected class GroupLoopEstimator extends PlanEstimator {
        private GroupLoopScan scan;
        private TableGroupJoinTree tableGroup;
        private Set<TableSource> requiredTables;

        protected GroupLoopEstimator(GroupLoopScan scan,
                                     TableGroupJoinTree tableGroup,
                                     Set<TableSource> requiredTables) {
            super(null);
            this.scan = scan;
            this.tableGroup = tableGroup;
            this.requiredTables = requiredTables;
        }

        @Override
        protected void estimateCost() {
            costEstimate = costEstimator.costFlattenNested(tableGroup, scan.getOutsideTable(), scan.getInsideTable(), scan.isInsideParent(), requiredTables);
        }
    }

    protected class HKeyRowEstimator extends PlanEstimator {
        private ExpressionsHKeyScan scan;

        protected HKeyRowEstimator(ExpressionsHKeyScan scan) {
            super(null);
            this.scan = scan;
        }

        @Override
        protected void estimateCost() {
            costEstimate = costEstimator.costHKeyRow(scan.getKeys());
        }
    }

    protected class FullTextScanEstimator extends PlanEstimator {
        private FullTextScan scan;

        protected FullTextScanEstimator(FullTextScan scan) {
            super(null);
            this.scan = scan;
        }

        @Override
        protected void estimateCost() {
            costEstimate = new CostEstimate(Math.max(scan.getLimit(), 1), 1.0);
        }
    }

    protected class SelectEstimator extends PlanEstimator {
        private Collection<ConditionExpression> conditions;
        private SelectivityConditions selectivityConditions;

        protected SelectEstimator(PlanEstimator input,
                                  Collection<ConditionExpression> conditions,
                                  SelectivityConditions selectivityConditions) {
            super(input);
            this.conditions = conditions;
            this.selectivityConditions = selectivityConditions;
        }

        @Override
        protected void estimateCost() {
            double selectivity = costEstimator.conditionsSelectivity(selectivityConditions);
            // Need enough input rows before selection.
            input.setLimit(hasLimit() ? 
                           Math.round(limit / selectivity) : 
                           NO_LIMIT);
            CostEstimate inputCost = inputCostEstimate();
            CostEstimate selectCost = costEstimator.costSelect(conditions,
                                                               selectivity, 
                                                               inputCost.getRowCount());
            costEstimate = inputCost.sequence(selectCost);
        }
    }

    protected class SortEstimator extends PlanEstimator {
        private int nfields;

        protected SortEstimator(PlanEstimator input, int nfields) {
            super(input);
            this.nfields = nfields;
        }

        @Override
        protected void estimateCost() {
            input.setLimit(NO_LIMIT);
            CostEstimate inputCost = inputCostEstimate();
            CostEstimate sortCost;
            if (hasLimit() && 
                (limit <= INSERTION_SORT_MAX_LIMIT)) {
                sortCost = costEstimator.costSortWithLimit(inputCost.getRowCount(),
                                                           Math.min(limit, inputCost.getRowCount()),
                                                           nfields);
            }
            else {
                sortCost = costEstimator.costSort(inputCost.getRowCount());
            }
            costEstimate = inputCost.sequence(sortCost);
        }
    }

    protected CostEstimate getScanOnlyCost(IndexScan index) {
        CostEstimate result = index.getScanCostEstimate();
        if (result == null) {
            if (index instanceof SingleIndexScan) {
                SingleIndexScan singleIndex = (SingleIndexScan) index;
                if (singleIndex.getConditionRange() == null) {
                    result = costEstimator.costIndexScan(singleIndex.getIndex(),
                            singleIndex.getEqualityComparands(),
                            singleIndex.getLowComparand(),
                            singleIndex.isLowInclusive(),
                            singleIndex.getHighComparand(),
                            singleIndex.isHighInclusive());
                }
                else {
                    CostEstimate cost = null;
                    for (RangeSegment segment : singleIndex.getConditionRange().getSegments()) {
                        CostEstimate acost = costEstimator.costIndexScan(singleIndex.getIndex(),
                                singleIndex.getEqualityComparands(),
                                segment.getStart().getValueExpression(),
                                segment.getStart().isInclusive(),
                                segment.getEnd().getValueExpression(),
                                segment.getEnd().isInclusive());
                        if (cost == null)
                            cost = acost;
                        else
                            cost = cost.union(acost);
                    }
                    if (cost == null) {
                        cost = new CostEstimate(0, 0); // No segments means no real scan.
                    }
                    result = cost;
                }
            }
            else if (index instanceof MultiIndexIntersectScan) {
                MultiIndexIntersectScan multiIndex = (MultiIndexIntersectScan) index;
                result = costEstimator.costIndexIntersection(multiIndex, new IndexIntersectionCoster() {
                    @Override
                    public CostEstimate singleIndexScanCost(SingleIndexScan scan, CostEstimator costEstimator) {
                        return getScanOnlyCost(scan);
                    }
                });
            }
            else {
                throw new AkibanInternalException("unknown index type: " + index + "(" + index.getClass() + ")");
            }
            index.setScanCostEstimate(result);
        }
        return result;
    }

    protected double getScanSetupCost(IndexScan index) {
        if (index instanceof SingleIndexScan) {
            SingleIndexScan singleIndex = (SingleIndexScan)index;
            if (singleIndex.getConditionRange() == null) {
                return costEstimator.costIndexScan(singleIndex.getIndex(), 0).getCost();
            }
            else {
                return costEstimator.costIndexScan(singleIndex.getIndex(), 0).getCost() *
                    singleIndex.getConditionRange().getSegments().size();
            }
        }
        else if (index instanceof MultiIndexIntersectScan) {
            MultiIndexIntersectScan multiIndex = (MultiIndexIntersectScan)index;
            return getScanSetupCost(multiIndex.getOutputIndexScan()) +
                   getScanSetupCost(multiIndex.getSelectorIndexScan());
        }
        else {
            return 0.0;
        }
    }

    protected Map<Table,Long> groupScanTableCountsToLimit(Set<TableSource> requiredTables, long limit) {
        // Find the required table with the highest ordinal; we'll need limit of those
        // rows and however many of the others come before it.
        // TODO: Not as good if multiple branches are being flattened;
        // fewer are needed to start, but repeats come in via branch
        // lookup.
        TableNode lastRequired = null;
        for (TableSource table : requiredTables) {
            if ((lastRequired == null) ||
                (lastRequired.getOrdinal() < table.getTable().getOrdinal())) {
                lastRequired = table.getTable();
            }
        }
        long childCount = costEstimator.getTableRowCount(lastRequired.getTable());
        if (childCount <= limit)
            // Turns out we need the whole group before reaching the limit.
            return null;
        Map<Table,Long> tableCounts = new HashMap<>();
        tableCounts.put(lastRequired.getTable(), limit);
        Table ancestor = lastRequired.getTable();
        while (true) {
            ancestor = ancestor.getParentTable();
            if (ancestor == null) break;
            long ancestorCount = costEstimator.getTableRowCount(ancestor);
            tableCounts.put(ancestor, 
                            // Ceiling number of ancestor needed to get limit of child.
                            (limit * ancestorCount + (childCount - 1)) / childCount);
        }
        Group group = lastRequired.getTable().getGroup();
        Map<Table,Long> moreCounts = new HashMap<>();
        for (Table table : lastRequired.getTable().getAIS().getTables().values()) {
            if (table.getGroup() == group) {
                Table commonAncestor = table;
                while (!tableCounts.containsKey(commonAncestor)) {
                    commonAncestor = commonAncestor.getParentTable();
                }
                if (commonAncestor == table) continue;
                long ancestorCount = tableCounts.get(commonAncestor);
                if (table.getOrdinal() > lastRequired.getOrdinal())
                    // A table that isn't required; number skipped
                    // depends on relative position.
                    ancestorCount--;
                moreCounts.put(table,
                               simpleRound(costEstimator.getTableRowCount(table) * ancestorCount,
                                           costEstimator.getTableRowCount(commonAncestor)));
            }
        }
        tableCounts.putAll(moreCounts);
        return tableCounts;
    }

}
