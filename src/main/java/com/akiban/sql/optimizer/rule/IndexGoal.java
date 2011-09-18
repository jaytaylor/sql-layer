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

/** A goal for indexing: conditions on joined tables and ordering / grouping. */
public class IndexGoal implements Comparator<IndexScan>
{
    public static class RequiredColumns {
        private Map<TableSource,Set<ColumnExpression>> map;
        
        public RequiredColumns(Collection<TableSource> tables) {
            map = new HashMap<TableSource,Set<ColumnExpression>>(tables.size());
            for (TableSource table : tables) {
                map.put(table, new HashSet<ColumnExpression>());
            }
        }

        public RequiredColumns(RequiredColumns other) {
            map = new HashMap<TableSource,Set<ColumnExpression>>(other.map.size());
            for (Map.Entry<TableSource,Set<ColumnExpression>> entry : other.map.entrySet()) {
                map.put(entry.getKey(), new HashSet<ColumnExpression>(entry.getValue()));
            }
        }

        public Set<TableSource> getTables() {
            return map.keySet();
        }
        
        public boolean isEmpty() {
            boolean empty = true;
            for (Set<ColumnExpression> entry : map.values())
                if (!entry.isEmpty())
                    return false;
            return empty;
        }

        public void require(ColumnExpression expr) {
            Set<ColumnExpression> entry = map.get(expr.getTable());
            if (entry != null)
                entry.add(expr);
        }

        /** Opposite of {@link require}: note that we have a source for this column. */
        public void have(ColumnExpression expr) {
            Set<ColumnExpression> entry = map.get(expr.getTable());
            if (entry != null)
                entry.remove(expr);
        }
    }

    // Tables already bound outside.
    private Set<TableSource> boundTables;

    // All the conditions that might be indexable.
    private List<ConditionExpression> conditions;

    // If both grouping and ordering are present, they must be
    // compatible. Something satisfying the ordering would also handle
    // the grouping. All the order by columns must also be group by
    // columns, though not necessarily in the same order. There can't
    // be any additional order by columns, because even though it
    // would be properly grouped going into aggregation, it wouldn't
    // still be sorted by those coming out. It's hard to write such a
    // query in SQL, since the ORDER BY can't contain columns not in
    // the GROUP BY, and non-columns won't appear in the index.
    private AggregateSource grouping;
    private Sort ordering;

    private boolean attemptCovering;

    // All the columns besides those in conditions that will be needed.
    private RequiredColumns requiredColumns;

    public IndexGoal(PlanNode plan,
                     Set<TableSource> boundTables, 
                     List<ConditionExpression> conditions,
                     AggregateSource grouping,
                     Sort ordering,
                     Collection<TableSource> tables) {
        this.boundTables = boundTables;
        this.conditions = conditions;
        this.grouping = grouping;
        this.ordering = ordering;
        
        attemptCovering = !(plan instanceof BaseUpdateStatement);

        requiredColumns = new RequiredColumns(tables);
        plan.accept(new RequiredColumnsFiller(requiredColumns, conditions));
    }

    /** Populate given index usage according to goal.
     * @return <code>false</code> if the index is useless.
     */
    public boolean usable(IndexScan index) {
        List<IndexColumn> indexColumns = index.getIndex().getColumns();
        int ncols = indexColumns.size();
        List<ExpressionNode> indexExpressions = new ArrayList<ExpressionNode>(ncols);
        for (IndexColumn indexColumn : indexColumns)
            indexExpressions.add(getIndexExpression(index, indexColumn));
        index.setColumns(indexExpressions);
        int nequals = 0;
        while (nequals < ncols) {
            ExpressionNode indexExpression = indexExpressions.get(nequals);
            if (indexExpression == null) break;
            ConditionExpression equalityCondition = null;
            ExpressionNode otherComparand = null;
            for (ConditionExpression condition : conditions) {
                if (condition instanceof ComparisonCondition) {
                    ComparisonCondition ccond = (ComparisonCondition)condition;
                    ExpressionNode comparand = null;
                    if (ccond.getOperation() == Comparison.EQ) {
                        if (indexExpression.equals(ccond.getLeft())) {
                            comparand = ccond.getRight();
                        }
                        else if (indexExpression.equals(ccond.getRight())) {
                            comparand = ccond.getLeft();
                        }
                    }
                    if ((comparand != null) && constantOrBound(comparand)) {
                        equalityCondition = condition;
                        otherComparand = comparand;
                        break;
                    }
                }
            }
            if (equalityCondition == null)
                break;
            index.addEqualityCondition(equalityCondition, otherComparand);
            nequals++;
        }
        if (nequals < ncols) {
            ExpressionNode indexExpression = indexExpressions.get(nequals);
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
            List<OrderByExpression> orderBy = 
                new ArrayList<OrderByExpression>(ncols - nequals);
            for (int i = nequals; i < ncols; i++) {
                indexExpression = indexExpressions.get(i);
                if (indexExpression == null) break;
                orderBy.add(new OrderByExpression(indexExpression, 
                                                  indexColumns.get(i).isAscending()));
            }
            index.setOrdering(orderBy);
        }
        index.setOrderEffectiveness(determineOrderEffectiveness(index));
        if ((index.getOrderEffectiveness() == IndexScan.OrderEffectiveness.NONE) &&
            (index.getConditions() == null))
            return false;
        if (attemptCovering)
            index.setCovering(determineCovering(index));
        return true;
    }

    // Determine how well this index does against the target.
    // Also, reverse the scan order if that helps. 
    // TODO: But see the comment on that field.
    protected IndexScan.OrderEffectiveness
        determineOrderEffectiveness(IndexScan index) {
        List<OrderByExpression> indexOrdering = index.getOrdering();
        List<ExpressionNode> equalityComparands = index.getEqualityComparands();
        IndexScan.OrderEffectiveness result = IndexScan.OrderEffectiveness.NONE;
        if (indexOrdering == null) return result;
        try_sorted:
        if (ordering != null) {
            Boolean reverse = null;
            int idx = 0;
            for (OrderByExpression targetColumn : ordering.getOrderBy()) {
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
                    continue;
                }
                if (equalityComparands != null) {
                    // Another possibility is that target ordering is
                    // in fact unchanged due to equality condition.
                    // TODO: Should this have been noticed earlier on
                    // so that it can be taken out of the sort?
                    if (equalityComparands.contains(targetExpression))
                        continue;
                }
                break try_sorted;
            }
            if (reverse != null)
                index.setReverseScan(reverse.booleanValue());
            result = IndexScan.OrderEffectiveness.SORTED;
        }
        if (grouping != null) {
            boolean anyFound = false, allFound = true;
            List<ExpressionNode> groupBy = grouping.getGroupBy();
            for (ExpressionNode targetExpression : groupBy) {
                int found = -1;
                for (int i = 0; i < indexOrdering.size(); i++) {
                    if (targetExpression.equals(indexOrdering.get(i).getExpression())) {
                        found = i;
                        break;
                    }
                }
                if (found < 0) {
                    allFound = false;
                    if ((equalityComparands == null) ||
                        !equalityComparands.contains(targetExpression))
                        continue;
                }
                else if (found >= groupBy.size()) {
                    // Ordered by this column, but after some other
                    // stuff which will break up the group. Only
                    // partially grouped.
                    allFound = false;
                }
                anyFound = true;
            }
            if (anyFound) {
                if (!allFound)
                    return IndexScan.OrderEffectiveness.PARTIAL_GROUPED;
                else if (result == IndexScan.OrderEffectiveness.SORTED)
                    return result;
                else
                    return IndexScan.OrderEffectiveness.GROUPED;
            }
        }
        return result;
    }

    protected class UnboundFinder implements ExpressionVisitor {
        boolean found = false;

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(ExpressionNode n) {
            return !found;
        }
        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression) {
                if (!boundTables.contains(((ColumnExpression)n).getTable())) {
                    found = true;
                    return false;
                }
            }
            else if (n instanceof SubqueryExpression) {
                found = true;
                return false;
            }
            return true;
        }
    }

    /** Does the given expression have references to tables that aren't bound? */
    protected boolean constantOrBound(ExpressionNode expression) {
        UnboundFinder f = new UnboundFinder();
        expression.accept(f);
        return !f.found;
    }

    /** Get an expression form of the given index column. */
    protected ExpressionNode getIndexExpression(IndexScan index,
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
    public IndexScan pickBestIndex(TableSource table) {
        IndexScan bestIndex = null;
        for (TableIndex index : table.getTable().getTable().getIndexes()) {
            IndexScan candidate = new IndexScan(index, table, table);
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
                IndexScan candidate = new IndexScan(index, table, rootTable);
                bestIndex = betterIndex(bestIndex, candidate);
            
            }
        }
        return bestIndex;
    }

    protected IndexScan betterIndex(IndexScan bestIndex, IndexScan candidate) {
        if (usable(candidate)) {
            if ((bestIndex == null) || (compare(candidate, bestIndex) > 0))
                return candidate;
        }
        return bestIndex;
    }

    /** Find the best index among the given tables. */
    public IndexScan pickBestIndex(Collection<TableSource> tables) {
        IndexScan bestIndex = null;
        for (TableSource table : tables) {
            IndexScan tableIndex = pickBestIndex(table);
            if ((tableIndex != null) &&
                ((bestIndex == null) || (compare(tableIndex, bestIndex) > 0)))
                bestIndex = tableIndex;
        }
        return bestIndex;
    }

    // TODO: This is a pretty poor substitute for evidence-based comparison.
    public int compare(IndexScan i1, IndexScan i2) {
        if (i1.getOrderEffectiveness() != i2.getOrderEffectiveness())
            // These are ordered worst to best.
            return i1.getOrderEffectiveness().compareTo(i2.getOrderEffectiveness());
        if (i1.isCovering()) {
            if (!i2.isCovering())
                return +1;
        }
        else if (i2.isCovering())
            return -1;
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

    static class RequiredColumnsFiller implements PlanVisitor, ExpressionVisitor {
        private RequiredColumns requiredColumns;
        // TODO: Maybe make this use pointer equality instead of equals().
        private Set<ExpressionNode> excluded;
        private int excludeDepth = 0;

        public RequiredColumnsFiller(RequiredColumns requiredColumns) {
            this.requiredColumns = requiredColumns;
            excluded = new HashSet<ExpressionNode>();
        }

        public RequiredColumnsFiller(RequiredColumns requiredColumns,
                                     Collection<ConditionExpression> conditions) {
            this(requiredColumns);
            excluded.addAll(conditions);
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }
        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            if (exclude(n))
                excludeDepth++;
            return visit(n);
        }
        @Override
        public boolean visitLeave(ExpressionNode n) {
            if (exclude(n))
                excludeDepth--;
            return true;
        }
        @Override
        public boolean visit(ExpressionNode n) {
            if (excludeDepth == 0) {
                if (n instanceof ColumnExpression)
                    requiredColumns.require((ColumnExpression)n);
            }
            return true;
        }

        // Should this expression be excluded from requirement?
        protected boolean exclude(ExpressionNode expr) {
            return (excluded.contains(expr) ||
                    // Group join conditions are handled specially.
                    ((expr instanceof ConditionExpression) &&
                     (((ConditionExpression)expr).getImplementation() ==
                      ConditionExpression.Implementation.GROUP_JOIN)));
        }
    }

    protected boolean determineCovering(IndexScan index) {
        // The non-condition requirements.
        RequiredColumns requiredAfter = new RequiredColumns(requiredColumns);
        RequiredColumnsFiller filler = new RequiredColumnsFiller(requiredAfter);
        // Add in any conditions not handled by the index.
        for (ConditionExpression condition : conditions) {
            boolean found = false;
            if (index.getConditions() != null) {
                for (ConditionExpression indexCondition : index.getConditions()) {
                    if (indexCondition == condition) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found)
                condition.accept(filler);
        }
        // Remove the columns we do have from the index.
        for (ExpressionNode column : index.getColumns()) {
            if (column instanceof ColumnExpression) {
                requiredAfter.have((ColumnExpression)column);
            }
        }
        return requiredAfter.isEmpty();
    }

    /** Change WHERE, GROUP BY, and ORDER BY upstream of
     * <code>node</code> as a consequence of <code>index</code> being
     * used.
     */
    public void installUpstream(IndexScan index, PlanNode node) {
        if (index.getConditions() != null) {
            for (ConditionExpression condition : index.getConditions()) {
                // TODO: This depends on conditions being the original
                // from the Filter, and not some copy merged with join
                // conditions, etc. When it is, more work will be
                // needed to track down where to remove, though
                // setting the implementation may be enough.
                conditions.remove(condition);
                if (condition instanceof ComparisonCondition) {
                    ((ComparisonCondition)condition).setImplementation(ConditionExpression.Implementation.INDEX);
                }
            }
        }
        if (grouping != null) {
            AggregateSource.Implementation implementation;
            switch (index.getOrderEffectiveness()) {
            case SORTED:
            case GROUPED:
                implementation = AggregateSource.Implementation.PRESORTED;
                break;
            case PARTIAL_GROUPED:
                implementation = AggregateSource.Implementation.PREAGGREGATE_RESORT;
                break;
            default:
                implementation = AggregateSource.Implementation.SORT;
                break;
            }
            grouping.setImplementation(implementation);
        }
        if (ordering != null) {
            if (index.getOrderEffectiveness() == IndexScan.OrderEffectiveness.SORTED) {
                // Sort not needed: splice it out.
                ordering.getOutput().replaceInput(ordering, ordering.getInput());
            }
        }
    }
    
}
