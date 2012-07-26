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

package com.akiban.sql.optimizer.rule.nocost;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.rule.range.ColumnRanges;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index.JoinType;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.expression.std.Comparison;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** A goal for indexing: conditions on joined tables and ordering / grouping. */
public class IndexGoal implements Comparator<IndexScan>
{
    private static final Logger logger = LoggerFactory.getLogger(IndexGoal.class);

    public static class RequiredColumns {
        private Map<TableSource,Set<ColumnExpression>> map;
        
        public RequiredColumns(Iterable<TableSource> tables) {
            map = new HashMap<TableSource,Set<ColumnExpression>>();
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
                     Iterable<TableSource> tables) {
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
    public boolean usable(SingleIndexScan index) {
        // TODO: This could be getIndexColumns(), but that would change test results.
        List<IndexColumn> indexColumns = ((SingleIndexScan)index).getIndex().getKeyColumns();
        int ncols = indexColumns.size();
        List<ExpressionNode> indexExpressions = new ArrayList<ExpressionNode>(ncols);
        List<OrderByExpression> orderBy = new ArrayList<OrderByExpression>(ncols);
        for (IndexColumn indexColumn : indexColumns) {
            ExpressionNode indexExpression = getIndexExpression(index, indexColumn);
            indexExpressions.add(indexExpression);
            orderBy.add(new OrderByExpression(indexExpression,
                                              indexColumn.isAscending()));
        }
        index.setColumns(indexExpressions);
        index.setOrdering(orderBy);
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
                else if (condition instanceof FunctionCondition) {
                    FunctionCondition fcond = (FunctionCondition)condition;
                    if (fcond.getFunction().equals("isNull") &&
                        (fcond.getOperands().size() == 1) &&
                        (fcond.getOperands().get(0).equals(indexExpression))) {
                        equalityCondition = condition;
                        otherComparand = null; // TODO: Or constant NULL, depending on API.
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
                        if ((otherComparand != null) && constantOrBound(otherComparand)) {
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
        }
        index.setOrderEffectiveness(determineOrderEffectiveness(index));
        index.setCovering(determineCovering(index));
        if ((index.getOrderEffectiveness() == IndexScan.OrderEffectiveness.NONE) &&
            !index.hasConditions() &&
            !index.isCovering())
            return false;
        return true;
    }

    // Determine how well this index does against the target.
    // Also, correct traversal order to match sort if possible.
    protected IndexScan.OrderEffectiveness determineOrderEffectiveness(SingleIndexScan index) {
        List<OrderByExpression> indexOrdering = index.getOrdering();
        BitSet reverse = new BitSet(indexOrdering.size());
        List<ExpressionNode> equalityComparands = index.getEqualityComparands();
        int nequals = (equalityComparands == null) ? 0 : equalityComparands.size();
        IndexScan.OrderEffectiveness result = IndexScan.OrderEffectiveness.NONE;
        if (indexOrdering == null) return result;
        try_sorted:
        if (ordering != null) {
            int idx = nequals;
            for (OrderByExpression targetColumn : ordering.getOrderBy()) {
                ExpressionNode targetExpression = targetColumn.getExpression();
                if (targetExpression.isColumn() &&
                    (grouping != null)) {
                    if (((ColumnExpression)targetExpression).getTable() == grouping) {
                        targetExpression = grouping.getField(((ColumnExpression)targetExpression).getPosition());
                    }
                }
                OrderByExpression indexColumn = null;
                if (idx < indexOrdering.size()) {
                    indexColumn = indexOrdering.get(idx);
                    if (indexColumn.getExpression() == null)
                        indexColumn = null; // Index sorts by unknown column.
                }
                if ((indexColumn != null) && 
                    indexColumn.getExpression().equals(targetExpression)) {
                    if (indexColumn.isAscending() != targetColumn.isAscending()) {
                        // To avoid mixed mode as much as possible,
                        // defer changing the index order until
                        // certain it will be effective.
                        reverse.set(idx, true);
                        if (idx == nequals)
                            // Likewise reverse the initial equals segment.
                            reverse.set(0, nequals, true);
                    }
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
            if ((idx > 0) && (idx < indexOrdering.size()) && reverse.get(idx-1))
                // Reverse after ORDER BY if reversed last one.
                reverse.set(idx, indexOrdering.size(), true);
            for (int i = 0; i < reverse.size(); i++) {
                if (reverse.get(i)) {
                    OrderByExpression indexColumn = indexOrdering.get(i);
                    indexColumn.setAscending(!indexColumn.isAscending());
                }
            }
            result = IndexScan.OrderEffectiveness.SORTED;
        }
        if (grouping != null) {
            boolean anyFound = false, allFound = true;
            List<ExpressionNode> groupBy = grouping.getGroupBy();
            for (ExpressionNode targetExpression : groupBy) {
                int found = -1;
                for (int i = nequals; i < indexOrdering.size(); i++) {
                    if (targetExpression.equals(indexOrdering.get(i).getExpression())) {
                        found = i - nequals;
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
                for (int i = nequals; i < indexOrdering.size(); i++) {
                    if (targetExpression.equals(indexOrdering.get(i).getExpression())) {
                        found = i - nequals;
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
     * @param groupOnly 
     */
    public IndexScan pickBestIndex(TableSource table, Set<TableSource> required) {
        SingleIndexScan bestIndex = null;
        // If this table is the optional part of a LEFT join, can
        // still consider group indexes to it, but not single table
        // indexes on it. WHERE conditions are removed before this
        // is called, see IndexPicker#determineIndexGoal().
        if (required.contains(table)) {
            for (TableIndex index : table.getTable().getTable().getIndexes()) {
                SingleIndexScan candidate = new SingleIndexScan(index, table);
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
                        if (required.contains(rootTable)) {
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
                SingleIndexScan candidate = new SingleIndexScan(index, rootTable,
                                                    rootRequired, leafRequired, 
                                                    table);
                bestIndex = betterIndex(bestIndex, candidate);
            }
        }
        return bestIndex;
    }

    protected SingleIndexScan betterIndex(SingleIndexScan bestIndex, SingleIndexScan candidate) {
        if (usable(candidate)) {
            if (bestIndex == null) {
                logger.debug("Selecting {}", candidate);
                return candidate;
            }
            else if (compare(candidate, bestIndex) > 0) {
                logger.debug("Preferring {}", candidate);
                return candidate;
            }
            else {
                logger.debug("Rejecting {}", candidate);
            }
        }
        return bestIndex;
    }

    /** Find the best index among the given tables. */
    public IndexScan pickBestIndex(Collection<TableSource> tables,
                                   Set<TableSource> required) {
        IndexScan bestIndex = null;
        for (TableSource table : tables) {
            IndexScan tableIndex = pickBestIndex(table, required);
            if ((tableIndex != null) &&
                ((bestIndex == null) || (compare(tableIndex, bestIndex) > 0)))
                bestIndex = tableIndex;
        }
        return bestIndex;
    }

    public int compare(IndexScan i1, IndexScan i2) {
        // TODO: This is a pretty poor substitute for evidence-based comparison.
        if (i1.getOrderEffectiveness() != i2.getOrderEffectiveness())
            // These are ordered worst to best.
            return i1.getOrderEffectiveness().compareTo(i2.getOrderEffectiveness());
        if (i1.isCovering()) {
            if (!i2.isCovering() &&
                (i1.hasConditions() == i2.hasConditions()))
                return +1;
        }
        else if (i2.isCovering() &&
                 (i1.hasConditions() == i2.hasConditions()))
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
        if (((SingleIndexScan)i1).getIndex().getKeyColumns().size() != ((SingleIndexScan)i2).getIndex().getKeyColumns().size())
            return (((SingleIndexScan)i1).getIndex().getKeyColumns().size() < 
                    ((SingleIndexScan)i2).getIndex().getKeyColumns().size())
                // Fewer columns indexed better than more.
                ? +1 : -1;
        // Deeper better than shallower.
        return i1.getLeafMostTable().getTable().getTable().getTableId().compareTo(i2.getLeafMostTable().getTable().getTable().getTableId());
    }

    protected boolean needSort(IndexScan index) {
        IndexScan.OrderEffectiveness effectiveness = index.getOrderEffectiveness();
        if ((ordering != null) ||
            (projectDistinct != null))
            return (effectiveness != IndexScan.OrderEffectiveness.SORTED);
        if (grouping != null)
            return ((effectiveness != IndexScan.OrderEffectiveness.SORTED) &&
                    (effectiveness != IndexScan.OrderEffectiveness.GROUPED));
        return false;
    }

    static class RequiredColumnsFiller implements PlanVisitor, ExpressionVisitor {
        private RequiredColumns requiredColumns;
        private Map<PlanNode,Void> excludedPlanNodes, includedPlanNodes;
        private Map<ExpressionNode,Void> excludedExpressions;
        private Deque<Boolean> excludeNodeStack = new ArrayDeque<Boolean>();
        private boolean excludeNode = false;
        private int excludeDepth = 0;
        private int subqueryDepth = 0;

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
            if ((n instanceof Subquery) &&
                !((Subquery)n).getOuterTables().isEmpty())
                // TODO: Might be accessing tables from outer query as
                // group joins, which we don't support currently. Make
                // sure those aren't excluded.
                subqueryDepth++;
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            excludeNode = excludeNodeStack.pop();
            if ((n instanceof Subquery) &&
                !((Subquery)n).getOuterTables().isEmpty())
                subqueryDepth--;
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
                      ConditionExpression.Implementation.GROUP_JOIN) &&
                     // Include expressions in subqueries until do joins across them.
                     (subqueryDepth == 0)));
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
        int ncols = index.getColumns().size();
        for (int i = 0; i < ncols; i++) {
            ExpressionNode column = index.getColumns().get(i);
            if ((column instanceof ColumnExpression) && index.isRecoverableAt(i)) {
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
