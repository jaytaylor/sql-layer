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

import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.*;

import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index.JoinType;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.sql.optimizer.rule.range.ColumnRanges;

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
        
        public boolean hasColumns(TableSource table) {
            Set<ColumnExpression> entry = map.get(table);
            if (entry == null) return false;
            return !entry.isEmpty();
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
    private Set<ColumnSource> boundTables;

    // All the conditions that might be indexable.
    private List<ConditionExpression> conditions;
    // Where they came from.
    private List<ConditionList> conditionSources;

    // mapping of Range-expressible conditions, by their column. lazy loaded.
    private Map<ColumnExpression,ColumnRanges> columnsToRanges;

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
    private Project projectDistinct;

    private TableNode updateTarget;

    // All the columns besides those in conditions that will be needed.
    private RequiredColumns requiredColumns;

    public IndexGoal(BaseQuery query,
                     Set<ColumnSource> boundTables, 
                     List<ConditionList> conditionSources,
                     AggregateSource grouping,
                     Sort ordering,
                     Project projectDistinct,
                     Collection<TableSource> tables) {
        this.boundTables = boundTables;
        this.conditionSources = conditionSources;
        this.grouping = grouping;
        this.ordering = ordering;
        this.projectDistinct = projectDistinct;
        
        if (conditionSources.size() == 1)
            conditions = conditionSources.get(0);
        else {
            conditions = new ArrayList<ConditionExpression>();
            for (ConditionList cs : conditionSources) {
                conditions.addAll(cs);
            }
        }
            
        if ((query instanceof UpdateStatement) ||
            (query instanceof DeleteStatement))
          updateTarget = ((BaseUpdateStatement)query).getTargetTable();

        requiredColumns = new RequiredColumns(tables);
        Collection<PlanNode> orderings = (ordering == null) ? 
            Collections.<PlanNode>emptyList() : 
            Collections.<PlanNode>singletonList(ordering);
        query.accept(new RequiredColumnsFiller(requiredColumns, orderings, conditions));
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
                boolean foundInequalityCondition = false;
                for (ConditionExpression condition : conditions) {
                    if (condition instanceof ComparisonCondition) {
                        ComparisonCondition ccond = (ComparisonCondition)condition;
                        if (ccond.getOperation().equals(Comparison.NE))
                            continue; // ranges are better suited for !=
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
                            foundInequalityCondition = true;
                        }
                    }
                }
                if (!foundInequalityCondition) {
                    ColumnRanges range = rangeForIndex(indexExpression);
                    if (range != null)
                        index.addRangeCondition(range);
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
            (!index.hasConditions()))
            return false;
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
                if (targetExpression.isColumn() &&
                    (grouping != null)) {
                    if (((ColumnExpression)targetExpression).getTable() == grouping) {
                        targetExpression = grouping.getField(((ColumnExpression)targetExpression).getPosition());
                    }
                }
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
        else if (projectDistinct != null) {
            assert (ordering == null);
            boolean allFound = true;
            List<ExpressionNode> distinct = projectDistinct.getFields();
            for (ExpressionNode targetExpression : distinct) {
                int found = -1;
                for (int i = 0; i < indexOrdering.size(); i++) {
                    if (targetExpression.equals(indexOrdering.get(i).getExpression())) {
                        found = i;
                        break;
                    }
                }
                if ((found < 0) || (found >= distinct.size())) {
                    allFound = false;
                    break;
                }
            }
            if (allFound)
                return IndexScan.OrderEffectiveness.SORTED;
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

    /** Find the best index on the given table. 
     * @param groupOnly If true, this table is the optional part of a
     * LEFT join. Can still consider group indexes to it, but not
     * single table indexes on it.
     */
    public IndexScan pickBestIndex(TableSource table, boolean groupOnly) {
        IndexScan bestIndex = null;
        if (!groupOnly) {
            for (TableIndex index : table.getTable().getTable().getIndexes()) {
                IndexScan candidate = new IndexScan(index, table);
                bestIndex = betterIndex(bestIndex, candidate);
            }
        }
        if (table.getGroup() != null) {
            for (GroupIndex index : table.getGroup().getGroup().getIndexes()) {
                // The leaf must be used or else we'll get duplicates from a
                // scan (the indexed columns need not be root to leaf, making
                // ancestors discontiguous and duplicates hard to eliminate).
                if (index.leafMostTable() != table.getTable().getTable())
                    continue;
                TableSource rootTable = table;
                TableSource rootRequired = null, leafRequired = null;
                if (index.getJoinType() == JoinType.LEFT) {
                    while (rootTable != null) {
                        // TODO: These isRequired() predicates need to
                        // be relative to the group to support outer
                        // joins between groups.
                        if (rootTable.isRequired()) {
                            rootRequired = rootTable;
                            if (leafRequired == null)
                                leafRequired = rootTable;
                        }
                        else {
                            if (leafRequired != null) {
                                leafRequired = null;
                                break;
                            }
                        }
                        if (index.rootMostTable() == rootTable.getTable().getTable())
                            break;
                        rootTable = rootTable.getParentTable();
                    }
                    // The root must be present, since a LEFT index
                    // does not contain orphans.
                    if ((rootTable == null) || 
                        (rootRequired != rootTable) ||
                        (leafRequired == null))
                        continue;
                }
                else {
                    if (!table.isRequired())
                        continue;
                    leafRequired = table;
                    TableSource childTable = null;
                    while (rootTable != null) {
                        if (rootTable.isRequired()) {
                            if (rootRequired != null) {
                                rootRequired = null;
                                break;
                            }
                        }
                        else {
                            if (rootRequired == null)
                                rootRequired = childTable;
                        }
                        if (index.rootMostTable() == rootTable.getTable().getTable())
                            break;
                        childTable = rootTable;
                        rootTable = rootTable.getParentTable();
                    }
                    if ((rootTable == null) ||
                        (rootRequired == null))
                        continue;
                }
                IndexScan candidate = new IndexScan(index, rootTable, 
                                                    rootRequired, leafRequired, 
                                                    table);
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
    public IndexScan pickBestIndex(Collection<TableSource> tables,
                                   Set<TableSource> required) {
        IndexScan bestIndex = null;
        for (TableSource table : tables) {
            IndexScan tableIndex = pickBestIndex(table, !required.contains(table));
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
        private Map<PlanNode,Void> excludedPlanNodes, includedPlanNodes;
        private Map<ExpressionNode,Void> excludedExpressions;
        private Deque<Boolean> excludeNodeStack = new ArrayDeque<Boolean>();
        private boolean excludeNode = false;
        private int excludeDepth = 0;

        public RequiredColumnsFiller(RequiredColumns requiredColumns) {
            this.requiredColumns = requiredColumns;
        }

        public RequiredColumnsFiller(RequiredColumns requiredColumns,
                                     Collection<PlanNode> excludedPlanNodes,
                                     Collection<ConditionExpression> excludedExpressions) {
            this.requiredColumns = requiredColumns;
            this.excludedPlanNodes = new IdentityHashMap<PlanNode,Void>();
            for (PlanNode planNode : excludedPlanNodes)
                this.excludedPlanNodes.put(planNode, null);
            this.excludedExpressions = new IdentityHashMap<ExpressionNode,Void>();
            for (ConditionExpression condition : excludedExpressions)
                this.excludedExpressions.put(condition, null);
        }

        public void setIncludedPlanNodes(Collection<PlanNode> includedPlanNodes) {
            this.includedPlanNodes = new IdentityHashMap<PlanNode,Void>();
            for (PlanNode planNode : includedPlanNodes)
                this.includedPlanNodes.put(planNode, null);
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            // Input nodes are called within the context of their output.
            // We want to know whether just this node is excluded, not
            // it and all its inputs.
            excludeNodeStack.push(excludeNode);
            excludeNode = exclude(n);
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            excludeNode = excludeNodeStack.pop();
            return true;
        }
        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            if (!excludeNode && exclude(n))
                excludeDepth++;
            return visit(n);
        }
        @Override
        public boolean visitLeave(ExpressionNode n) {
            if (!excludeNode && exclude(n))
                excludeDepth--;
            return true;
        }
        @Override
        public boolean visit(ExpressionNode n) {
            if (!excludeNode && (excludeDepth == 0)) {
                if (n instanceof ColumnExpression)
                    requiredColumns.require((ColumnExpression)n);
            }
            return true;
        }

        // Should this plan node be excluded from the requirement?
        protected boolean exclude(PlanNode node) {
            if (includedPlanNodes != null)
                return !includedPlanNodes.containsKey(node);
            else if (excludedPlanNodes != null)
                return excludedPlanNodes.containsKey(node);
            else
                return false;
        }
        
        // Should this expression be excluded from requirement?
        protected boolean exclude(ExpressionNode expr) {
            return (((excludedExpressions != null) &&
                     excludedExpressions.containsKey(expr)) ||
                    // Group join conditions are handled specially.
                    ((expr instanceof ConditionExpression) &&
                     (((ConditionExpression)expr).getImplementation() ==
                      ConditionExpression.Implementation.GROUP_JOIN)));
        }
    }

    protected boolean determineCovering(IndexScan index) {
        // Include the non-condition requirements.
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
        // Add sort if not handled by the index.
        if ((ordering != null) &&
            (index.getOrderEffectiveness() != IndexScan.OrderEffectiveness.SORTED)) {
            // Only this node, not its inputs.
            filler.setIncludedPlanNodes(Collections.<PlanNode>singletonList(ordering));
            ordering.accept(filler);
        }
            
        // Record what tables are required: within the index if any
        // columns still needed, others if joined at all. Do this
        // before taking account of columns from a covering index,
        // since may not use it that way.
        {
            Collection<TableSource> joined = index.getTables();
            Set<TableSource> required = new HashSet<TableSource>();
            boolean moreTables = false;
            for (TableSource table : requiredAfter.getTables()) {
                if (!joined.contains(table)) {
                    moreTables = true;
                    required.add(table);
                }
                else if (requiredAfter.hasColumns(table) ||
                         (table.getTable() == updateTarget)) {
                    required.add(table);
                }
            }
            index.setRequiredTables(required);
            if (moreTables)
                // Need to join up last the index; index might point
                // to an orphan.
                return false;
        }

        if (updateTarget != null) {
          // UPDATE statements need the whole target row and are thus never covering.
          return false;
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
    public void installUpstream(IndexScan index) {
        if (index.getConditions() != null) {
            for (ConditionExpression condition : index.getConditions()) {
                for (ConditionList conditionSource : conditionSources) {
                    if (conditionSource.remove(condition))
                        break;
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
        if (projectDistinct != null) {
            Distinct distinct = (Distinct)projectDistinct.getOutput();
            Distinct.Implementation implementation;
            switch (index.getOrderEffectiveness()) {
            case SORTED:
                implementation = Distinct.Implementation.PRESORTED;
                break;
            default:
                implementation = Distinct.Implementation.SORT;
                break;
            }
            distinct.setImplementation(implementation);
        }
    }

    private ColumnRanges rangeForIndex(ExpressionNode expressionNode) {
        if (expressionNode instanceof ColumnExpression) {
            if (columnsToRanges == null) {
                columnsToRanges = new HashMap<ColumnExpression, ColumnRanges>();
                for (ConditionExpression condition : conditions) {
                    ColumnRanges range = ColumnRanges.rangeAtNode(condition);
                    if (range != null) {
                        ColumnExpression rangeColumn = range.getColumnExpression();
                        ColumnRanges oldRange = columnsToRanges.get(rangeColumn);
                        if (oldRange != null)
                            range = ColumnRanges.andRanges(range, oldRange);
                        columnsToRanges.put(rangeColumn, range);
                    }
                }
            }
            ColumnExpression columnExpression = (ColumnExpression) expressionNode;
            return columnsToRanges.get(columnExpression);
        }
        return null;
    }
}
