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

import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import com.akiban.qp.expression.Comparison;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;

import java.util.*;

public class IndexGoal implements Comparator<IndexUsage>
{
    // Tables already bound outside.
    private Set<TableSource> boundTables;

    // The group we want to index.
    private TableGroup group;
    // All the tables that are joined together here, including outer
    // join ones that can't have indexes, but might have sorting or
    // covering.
    private List<TableSource> tables;

    // All the conditions that might be indexable.
    private List<ConditionExpression> conditions;

    // If both grouping and ordering are present, they must be
    // compatible: something satisfying the ordering would also handle
    // the grouping.
    private List<ExpressionNode> grouping;
    private List<OrderByExpression> ordering;

    public IndexGoal(TableGroup group) {
    }

    /** Populate given index usage according to goal.
     * @return <code>false</code> if the index is useless.
     */
    public boolean usable(IndexUsage index) {
        List<IndexColumn> indexColumns = index.getIndex().getColumns();
        int ncols = indexColumns.size();
        int nequals = 0;
        while (nequals < ncols) {
            IndexColumn indexColumn = indexColumns.get(nequals);
            ExpressionNode indexExpression = getIndexExpression(index, indexColumn);
            if (indexExpression == null) break;
            ConditionExpression equalityCondition = null;
            ExpressionNode otherComparand = null;
            for (ConditionExpression condition : conditions) {
                if (condition instanceof ComparisonCondition) {
                    ComparisonCondition ccond = (ComparisonCondition)condition;
                    if (ccond.getOperation() == Comparison.EQ) {
                        if (indexExpression.equals(ccond.getLeft())) {
                            otherComparand = ccond.getRight();
                            equalityCondition = condition;
                            break;
                        }
                        else if (indexExpression.equals(ccond.getRight())) {
                            otherComparand = ccond.getLeft();
                            equalityCondition = condition;
                            break;
                        }
                    }
                }
            }
            if (equalityCondition == null)
                break;
            index.addEqualityCondition(equalityCondition, otherComparand);
            nequals++;
        }
        if (nequals < ncols) {
            {
                IndexColumn indexColumn = indexColumns.get(nequals);
                ExpressionNode indexExpression = getIndexExpression(index, indexColumn);
                if (indexExpression != null) {
                    for (ConditionExpression condition : conditions) {
                        if (condition instanceof ComparisonCondition) {
                            ComparisonCondition ccond = (ComparisonCondition)condition;
                            ExpressionNode otherComparand = null;
                            if (indexExpression.equals(ccond.getLeft())) {
                                otherComparand = ccond.getRight();
                            }
                            else if (indexExpression.equals(ccond.getRight())) {
                                otherComparand = ccond.getLeft();
                            }
                            if (otherComparand != null) {
                                index.addInequalityCondition(condition,
                                                             ccond.getOperation(),
                                                             otherComparand);
                            }
                        }
                    }
                }
            }
            List<OrderByExpression> ordering = 
                new ArrayList<OrderByExpression>(ncols - nequals);
            for (int i = nequals; i < ncols; i++) {
                IndexColumn indexColumn = indexColumns.get(i);
                ExpressionNode indexExpression = getIndexExpression(index, indexColumn);
                if (indexExpression == null) break;
                ordering.add(new OrderByExpression(indexExpression, 
                                                   indexColumn.isAscending()));
            }
            index.setOrdering(ordering);
        }
        index.setOrderEffectiveness(determineOrderEffectiveness(index));
        return true;
    }

    // Determine how well this index does against the target.
    // Also, reverse the scan order if that helps. 
    // TODO: But see the comment on that field.
    protected IndexUsage.OrderEffectiveness
        determineOrderEffectiveness(IndexUsage index) {
        List<OrderByExpression> indexOrdering = index.getOrdering();
        List<ExpressionNode> equalityComparands = index.getEqualityComparands();
        if (indexOrdering == null) return IndexUsage.OrderEffectiveness.NONE;
        try_sorted:
        if (ordering != null) {
            Boolean reverse = null;
            int idx = 0;
            for (OrderByExpression targetColumn : ordering) {
                ExpressionNode targetExpression = targetColumn.getExpression();
                OrderByExpression indexColumn = null;
                if (idx < indexOrdering.size())
                    indexColumn = indexOrdering.get(idx);
                if ((indexColumn != null) && 
                    indexColumn.getExpression().equals(targetExpression)) {
                    if (reverse == null)
                        reverse = Boolean.valueOf(indexColumn.isAscending() != 
                                                  targetColumn.isAscending());
                    else if (reverse.booleanValue() != 
                             (indexColumn.isAscending() != targetColumn.isAscending()))
                        // Only good enough up to reversal of scan.
                        break try_sorted;
                    idx++;
                }
                else {
                    // Another possibility is that target ordering is
                    // in fact unchanged due to equality condition.
                    // TODO: Should this have been noticed earlier on
                    // so that it can be taken out of the sort?
                    if (equalityComparands.indexOf(targetExpression) >= 0)
                        continue;
                }
                break try_sorted;
            }
            if (reverse != null)
                index.setReverseScan(reverse.booleanValue());
            return IndexUsage.OrderEffectiveness.SORTED;
        }
        if (grouping != null) {
            boolean anyFound = false, allFound = true;
            for (ExpressionNode targetExpression : grouping) {
                boolean found = false;
                for (OrderByExpression indexColumn : indexOrdering) {
                    if (indexColumn.getExpression().equals(targetExpression)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    allFound = false;
                    if (equalityComparands.indexOf(targetExpression) < 0) {
                        continue;
                    }
                }
                anyFound = true;
            }
            if (anyFound) {
                if (allFound)
                    return IndexUsage.OrderEffectiveness.GROUPED;
                else
                    return IndexUsage.OrderEffectiveness.PARTIAL_GROUPED;
            }
        }
        return IndexUsage.OrderEffectiveness.NONE;
    }

    protected ExpressionNode getIndexExpression(IndexUsage index,
                                                IndexColumn indexColumn) {
        Column column = indexColumn.getColumn();
        UserTable indexTable = column.getUserTable();
        for (TableSource table = index.getLeafMostTable();
             null != table;
             table = table.getParentTable()) {
            if (table.getTable().getTable() == indexTable) {
                return new ColumnExpression(table, column);
            }
        }
        return null;
    }

    /** Find the best index on the given table. */
    public IndexUsage pickBestIndex(TableSource table) {
        IndexUsage bestIndex = null;
        for (TableIndex index : table.getTable().getTable().getIndexes()) {
            IndexUsage candidate = new IndexUsage(index, table, table);
            bestIndex = betterIndex(bestIndex, candidate);
        }
        if (table.getGroup() != null) {
            for (GroupIndex index : table.getGroup().getGroup().getIndexes()) {
                // The leaf must be used or else we'll get duplicates from a
                // scan (the indexed columns need not be root to leaf, making
                // ancestors discontiguous and duplicates hard to eliminate).
                if (index.leafMostTable() != table.getTable().getTable())
                    continue;
                // The root must be present, since the index does not
                // contain orphans.
                TableSource rootTable = table;
                while (rootTable != null) {
                    if (index.rootMostTable() == rootTable.getTable().getTable())
                        break;
                    rootTable = rootTable.getParentTable();
                }
                if (rootTable == null) continue;
                IndexUsage candidate = new IndexUsage(index, table, rootTable);
                bestIndex = betterIndex(bestIndex, candidate);
            
            }
        }
        return bestIndex;
    }

    protected IndexUsage betterIndex(IndexUsage bestIndex, IndexUsage candidate) {
        if (usable(candidate)) {
            if ((bestIndex == null) || (compare(candidate, bestIndex) > 0))
                return candidate;
        }
        return bestIndex;
    }

    /** Find the best index among the given tables. */
    public IndexUsage pickBestIndex(Collection<TableSource> tables) {
        IndexUsage bestIndex = null;
        for (TableSource table : tables) {
            IndexUsage tableIndex = pickBestIndex(table);
            if ((bestIndex == null) || (compare(tableIndex, bestIndex) > 0))
                bestIndex = tableIndex;
        }
        return bestIndex;
    }

    // TODO: This is a pretty poor substitute for evidence-based comparison.
    public int compare(IndexUsage i1, IndexUsage i2) {
        if (i1.getOrderEffectiveness() != i2.getOrderEffectiveness())
            // These are ordered worst to best.
            return i1.getOrderEffectiveness().compareTo(i2.getOrderEffectiveness());
        if (i1.getEqualityComparands() != null) {
            if (i2.getEqualityComparands() == null)
                return +1;
            else if (i1.getEqualityComparands().size() !=
                     i2.getEqualityComparands().size())
                return (i1.getEqualityComparands().size() > 
                        i2.getEqualityComparands().size()) 
                    // More conditions tested better than fewer.
                    ? +1 : -1;
        }
        else if (i2.getEqualityComparands() != null)
            return -1;
        {
            int n1 = 0, n2 = 0;
            if (i1.getLowComparand() != null)
                n1++;
            if (i1.getHighComparand() != null)
                n1++;
            if (i2.getLowComparand() != null)
                n2++;
            if (i2.getHighComparand() != null)
                n2++;
            if (n1 != n2) 
                return (n1 > n2) ? +1 : -1;
        }
        if (i1.getIndex().getColumns().size() != i2.getIndex().getColumns().size())
            return (i1.getIndex().getColumns().size() < 
                    i2.getIndex().getColumns().size()) 
                // Fewer columns indexed better than more.
                ? +1 : -1;
        // Deeper better than shallower.
        return i1.getLeafMostTable().getTable().getTable().getTableId().compareTo(i2.getLeafMostTable().getTable().getTable().getTableId());
    }

}
