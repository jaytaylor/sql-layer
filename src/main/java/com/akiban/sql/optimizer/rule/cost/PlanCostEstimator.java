/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer.rule.cost;

import com.akiban.sql.optimizer.rule.cost.CostEstimator.IndexIntersectionCoster;
import com.akiban.sql.optimizer.rule.range.RangeSegment;

import com.akiban.sql.optimizer.plan.*;

import com.akiban.server.error.AkibanInternalException;

import java.util.*;

public class PlanCostEstimator
{
    protected CostEstimator costEstimator;
    private PlanEstimator planEstimator;

    public PlanCostEstimator(CostEstimator costEstimator) {
        this.costEstimator = costEstimator;
    }

    public CostEstimate getCostEstimate() {
        return planEstimator.getCostEstimate();
    }

    public void setLimit(long limit) {
        planEstimator.setLimit(limit);
    }

    public void indexScan(IndexScan index) {
        planEstimator = new IndexScanEstimator(index);
    }

    public void groupScan(GroupScan scan) {
        planEstimator = new GroupScanEstimator(scan);
    }

    public void flatten(TableGroupJoinTree tableGroup,
                        TableSource indexTable,
                        Set<TableSource> requiredTables) {
        planEstimator = new FlattenEstimator(planEstimator, 
                                             tableGroup, indexTable, requiredTables);
    }

    public void flattenGroup(TableGroupJoinTree tableGroup,
                             Set<TableSource> requiredTables) {
        planEstimator = new FlattenGroupEstimator(planEstimator, 
                                                  tableGroup, requiredTables);
    }

    public void select(Collection<ConditionExpression> conditions,
                       Map<ColumnExpression,Collection<ComparisonCondition>> selectivityConditions) {
        planEstimator = new SelectEstimator(planEstimator,
                                            conditions, selectivityConditions);
    }

    public void sort() {
        planEstimator = new SortEstimator(planEstimator);
    }

    protected abstract class PlanEstimator {
        protected PlanEstimator input;
        protected CostEstimate costEstimate = null;
        protected long limit = -1;

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
        }
    }

    protected class GroupScanEstimator extends PlanEstimator {
        GroupScan scan;

        protected GroupScanEstimator(GroupScan scan) {
            super(null);
            this.scan = scan;
        }

        @Override
        protected void estimateCost() {
            costEstimate = costEstimator.costGroupScan(scan.getGroup().getGroup());
        }
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
            costEstimate = inputCostEstimate()
                .nest(costEstimator.costFlatten(tableGroup, indexTable, requiredTables));
        }
    }

    protected class FlattenGroupEstimator extends PlanEstimator {
        private TableGroupJoinTree tableGroup;
        private Set<TableSource> requiredTables;

        protected FlattenGroupEstimator(PlanEstimator input,
                                        TableGroupJoinTree tableGroup,
                                        Set<TableSource> requiredTables) {
            super(input);
            this.tableGroup = tableGroup;
            this.requiredTables = requiredTables;
        }

        @Override
        protected void estimateCost() {
            costEstimate = inputCostEstimate()
                .sequence(costEstimator.costFlattenGroup(tableGroup, requiredTables));
        }
    }

    protected class SelectEstimator extends PlanEstimator {
        private Collection<ConditionExpression> conditions;
        private Map<ColumnExpression,Collection<ComparisonCondition>> selectivityConditions;

        protected SelectEstimator(PlanEstimator input,
                                  Collection<ConditionExpression> conditions,
                                  Map<ColumnExpression,Collection<ComparisonCondition>> selectivityConditions) {
            super(input);
            this.conditions = conditions;
            this.selectivityConditions = selectivityConditions;
        }

        @Override
        protected void estimateCost() {
            CostEstimate inputCost = inputCostEstimate();
            CostEstimate selectCost = costEstimator.costSelect(conditions,
                                                               selectivityConditions, 
                                                               inputCost.getRowCount());
            costEstimate = inputCost.sequence(selectCost);
        }
    }

    protected class SortEstimator extends PlanEstimator {

        protected SortEstimator(PlanEstimator input) {
            super(input);
        }

        @Override
        protected void estimateCost() {
            CostEstimate inputCost = inputCostEstimate();
            CostEstimate sortCost = costEstimator.costSort(inputCost.getRowCount());
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

}
