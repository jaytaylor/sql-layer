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

package com.akiban.sql.optimizer.rule.join_enum;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.sql.optimizer.rule.CostEstimator;
import com.akiban.sql.optimizer.rule.CostEstimator.IndexIntersectionCoster;
import com.akiban.sql.optimizer.rule.EquivalenceFinder;
import com.akiban.sql.optimizer.rule.join_enum.DPhyp.JoinOperator;
import com.akiban.sql.optimizer.rule.range.ColumnRanges;
import com.akiban.sql.optimizer.rule.range.RangeSegment;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.plan.TableGroupJoinTree.TableGroupJoinNode;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index.JoinType;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.expression.std.Comparison;

import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** The goal of a indexes within a group. */
public class GroupIndexGoal implements Comparator<IndexScan>
{
    private static final Logger logger = LoggerFactory.getLogger(GroupIndexGoal.class);
    static volatile Function<? super IndexScan,Void> intersectionEnumerationHook = null;

    // The overall goal.
    private QueryIndexGoal queryGoal;
    // The grouped tables.
    private TableGroupJoinTree tables;
    
    // All the conditions that might be indexable.
    private List<ConditionExpression> conditions;
    // Where they came from.
    private List<ConditionList> conditionSources;

    // All the columns besides those in conditions that will be needed.
    private RequiredColumns requiredColumns;

    // Tables already bound outside.
    private Set<ColumnSource> boundTables;

    // Can an index be used to take care of sorting.
    // TODO: Make this a subset of queryGoal's sorting, based on what
    // tables are in what join, rather than an all or nothing.
    private boolean sortAllowed;

    // Mapping of Range-expressible conditions, by their column. lazy loaded.
    private Map<ColumnExpression,ColumnRanges> columnsToRanges;
    
    public GroupIndexGoal(QueryIndexGoal queryGoal, TableGroupJoinTree tables) {
        this.queryGoal = queryGoal;
        this.tables = tables;

        if (queryGoal.getWhereConditions() != null) {
            conditionSources = Collections.singletonList(queryGoal.getWhereConditions());
            conditions = queryGoal.getWhereConditions();
        }
        else {
            conditionSources = Collections.emptyList();
            conditions = Collections.emptyList();
        }

        requiredColumns = new RequiredColumns(tables);

        boundTables = queryGoal.getQuery().getOuterTables();
        sortAllowed = true;
    }

    public QueryIndexGoal getQueryGoal() {
        return queryGoal;
    }

    public TableGroupJoinTree getTables() {
        return tables;
    }

    public List<ConditionList> updateContext(Set<ColumnSource> boundTables,
                                             Collection<JoinOperator> joins,
                                             Collection<JoinOperator> outsideJoins,
                                             boolean sortAllowed) {
        setBoundTables(boundTables);
        this.sortAllowed = sortAllowed;
        setJoinConditions(joins);
        updateRequiredColumns(joins, outsideJoins);
        return conditionSources;
    }

    public void setBoundTables(Set<ColumnSource> boundTables) {
        this.boundTables = boundTables;
    }
    
    public void setJoinConditions(Collection<JoinOperator> joins) {
        conditionSources = new ArrayList<ConditionList>();
        if (queryGoal.getWhereConditions() != null)
            conditionSources.add(queryGoal.getWhereConditions());
        for (JoinOperator join : joins) {
            ConditionList joinConditions = join.getJoinConditions();
            if (joinConditions != null)
                conditionSources.add(joinConditions);
        }
        switch (conditionSources.size()) {
        case 0:
            conditions = Collections.emptyList();
            break;
        case 1:
            conditions = conditionSources.get(0);
            break;
        default:
            conditions = new ArrayList<ConditionExpression>();
            for (ConditionList conditionSource : conditionSources) {
                conditions.addAll(conditionSource);
            }
        }
    }

    public void updateRequiredColumns(Collection<JoinOperator> joins,
                                      Collection<JoinOperator> outsideJoins) {
        requiredColumns.clear();
        Collection<PlanNode> orderings = (queryGoal.getOrdering() == null) ? 
            Collections.<PlanNode>emptyList() : 
            Collections.<PlanNode>singletonList(queryGoal.getOrdering());
        RequiredColumnsFiller filler = new RequiredColumnsFiller(requiredColumns, 
                                                                 orderings, conditions);
        queryGoal.getQuery().accept(filler);
        for (JoinOperator join : outsideJoins) {
            if (joins.contains(join)) continue;
            ConditionList joinConditions = join.getJoinConditions();
            if (joinConditions != null) {
                for (ConditionExpression condition : joinConditions) {
                    condition.accept(filler);
                }
            }
        }        
    }

    /** Populate given index usage according to goal.
     * @return <code>false</code> if the index is useless.
     */
    public boolean usable(SingleIndexScan index, IntersectionEnumerator enumerator) {
        int nequals = insertLeadingEqualities(index, conditions, enumerator);
        List<ExpressionNode> indexExpressions = index.getColumns();
        if (nequals < indexExpressions.size()) {
            ExpressionNode indexExpression = indexExpressions.get(nequals);
            if (indexExpression != null) {
                boolean foundInequalityCondition = false;
                for (ConditionExpression condition : conditions) {
                    if (condition instanceof ComparisonCondition) {
                        ComparisonCondition ccond = (ComparisonCondition)condition;
                        if (ccond.getOperation().equals(Comparison.NE))
                            continue; // ranges are better suited for !=
                        ExpressionNode otherComparand = null;
                        if (indexExpressionMatches(indexExpression, ccond.getLeft(), enumerator)) {
                            otherComparand = ccond.getRight();
                        }
                        else if (indexExpressionMatches(indexExpression, ccond.getRight(), enumerator)) {
                            otherComparand = ccond.getLeft();
                        }
                        if ((otherComparand != null) && constantOrBound(otherComparand, enumerator)) {
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
        index.setCostEstimate(estimateCost(index));
        return true;
    }

    private int insertLeadingEqualities(SingleIndexScan index, List<ConditionExpression> localConds,
                                        IntersectionEnumerator enumerator)
    {
        setColumnsAndOrdering(index);
        int nequals = 0;
        List<ExpressionNode> indexExpressions = index.getColumns();
        int ncols = indexExpressions.size();
        while (nequals < ncols) {
            ExpressionNode indexExpression = indexExpressions.get(nequals);
            if (indexExpression == null) break;
            ConditionExpression equalityCondition = null;
            ExpressionNode otherComparand = null;
            for (ConditionExpression condition : localConds) {
                if (condition instanceof ComparisonCondition) {
                    ComparisonCondition ccond = (ComparisonCondition)condition;
                    ExpressionNode comparand = null;
                    if (ccond.getOperation() == Comparison.EQ) {
                        if (indexExpressionMatches(indexExpression, ccond.getLeft(), enumerator)) {
                            comparand = ccond.getRight();
                        }
                        else if (indexExpressionMatches(indexExpression, ccond.getRight(), enumerator)) {
                            comparand = ccond.getLeft();
                        }
                    }
                    if ((comparand != null) && constantOrBound(comparand, enumerator)) {
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
        return nequals;
    }

    private static void setColumnsAndOrdering(SingleIndexScan index) {
        List<IndexColumn> indexColumns = index.getAllColumns();
        int ncols = indexColumns.size();
        List<OrderByExpression> orderBy = new ArrayList<OrderByExpression>(ncols);
        List<ExpressionNode> indexExpressions = new ArrayList<ExpressionNode>(ncols);
        for (IndexColumn indexColumn : indexColumns) {
            ExpressionNode indexExpression = getIndexExpression(index, indexColumn);
            indexExpressions.add(indexExpression);
            orderBy.add(new OrderByExpression(indexExpression,
                    indexColumn.isAscending()));
        }
        index.setColumns(indexExpressions);
        index.setOrdering(orderBy);
    }

    // TODO: When we support ordering for a MultiIndexIntersectScan,
    // this will need to take its ordering and work out the
    // implications for the components. Right now, Intersect_Ordered
    // only does ascending.
    private static void resetOrdering(IndexScan index) {
        List<IndexColumn> indexColumns = index.getAllColumns();
        List<OrderByExpression> orderBy = index.getOrdering();
        if (orderBy != null) {
            for (int i = 0; i < indexColumns.size(); i++) {
                orderBy.get(i).setAscending(indexColumns.get(i).isAscending());
            }        
        }
        if (index instanceof MultiIndexIntersectScan) {
            MultiIndexIntersectScan multiIndex = (MultiIndexIntersectScan)index;
            resetOrdering(multiIndex.getOutputIndexScan());
            resetOrdering(multiIndex.getSelectorIndexScan());
        }
    }

    // Determine how well this index does against the target.
    // Also, correct traversal order to match sort if possible.
    protected IndexScan.OrderEffectiveness
        determineOrderEffectiveness(SingleIndexScan index) {
        IndexScan.OrderEffectiveness result = IndexScan.OrderEffectiveness.NONE;
        if (!sortAllowed) return result;
        List<OrderByExpression> indexOrdering = index.getOrdering();
        if (indexOrdering == null) return result;
        BitSet reverse = new BitSet(indexOrdering.size());
        List<ExpressionNode> equalityComparands = index.getEqualityComparands();
        int nequals = (equalityComparands == null) ? 0 : equalityComparands.size();
        try_sorted:
        if (queryGoal.getOrdering() != null) {
            int idx = nequals;
            for (OrderByExpression targetColumn : queryGoal.getOrdering().getOrderBy()) {
                ExpressionNode targetExpression = targetColumn.getExpression();
                if (targetExpression.isColumn() &&
                    (queryGoal.getGrouping() != null)) {
                    if (((ColumnExpression)targetExpression).getTable() == queryGoal.getGrouping()) {
                        targetExpression = queryGoal.getGrouping().getField(((ColumnExpression)targetExpression).getPosition());
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
        if (queryGoal.getGrouping() != null) {
            boolean anyFound = false, allFound = true;
            List<ExpressionNode> groupBy = queryGoal.getGrouping().getGroupBy();
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
        else if (queryGoal.getProjectDistinct() != null) {
            assert (queryGoal.getOrdering() == null);
            boolean allFound = true;
            List<ExpressionNode> distinct = queryGoal.getProjectDistinct().getFields();
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
        IntersectionEnumerator enumerator;

        public UnboundFinder(IntersectionEnumerator enumerator) {
            this.enumerator = enumerator;
        }

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
                ColumnExpression columnExpression = (ColumnExpression)n;
                if (!boundTables.contains(columnExpression.getTable())) {
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
    protected boolean constantOrBound(ExpressionNode expression, IntersectionEnumerator enumerator) {
        UnboundFinder f = new UnboundFinder(enumerator);
        expression.accept(f);
        return !f.found;
    }

    /** Get an expression form of the given index column. */
    protected static ExpressionNode getIndexExpression(IndexScan index,
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

    /** Is the comparison operand what the index indexes? */
    protected boolean indexExpressionMatches(ExpressionNode indexExpression,
                                             ExpressionNode comparisonOperand,
                                             IntersectionEnumerator enumerator) {
        if (indexExpression.equals(comparisonOperand))
            return true;
        if (!(comparisonOperand instanceof ColumnExpression))
            return false;
        // See if comparing against a result column of the subquery,
        // that is, a join to the subquery that we can push down.
        // TODO: Should check column equivalences here, too. If we
        // added the below that earlier, could count such a join as a
        // group join: p JOIN (SELECT fk) sq ON p.pk = sq.fk.
        ColumnExpression comparisonColumn = (ColumnExpression)comparisonOperand;
        ColumnSource comparisonTable = comparisonColumn.getTable();
        if (!(comparisonTable instanceof SubquerySource))
            return false;
        Subquery subquery = ((SubquerySource)comparisonTable).getSubquery();
        if (subquery != queryGoal.getQuery())
            return false;
        if (!(subquery.getQuery() instanceof ResultSet))
            return false;
        ResultSet results = (ResultSet)subquery.getQuery();
        if (!(results.getInput() instanceof Project))
            return false;
        Project project = (Project)results.getInput();
        ExpressionNode insideExpression = project.getFields().get(comparisonColumn.getPosition());
        return indexExpressionMatches(indexExpression, insideExpression, enumerator);
    }

    /** Find the best index among the branches. */
    public PlanNode pickBestScan() {
        Set<TableSource> required = tables.getRequired();
        IndexScan bestIndex = null;

        IntersectionEnumerator intersections = new IntersectionEnumerator();
        for (TableGroupJoinNode table : tables) {
            IndexScan tableIndex = pickBestIndex(table.getTable(), required, intersections);
            if ((tableIndex != null) &&
                ((bestIndex == null) || (compare(tableIndex, bestIndex) > 0)))
                bestIndex = tableIndex;
        }
        bestIndex = pickBestIntersection(bestIndex, intersections);
        if (bestIndex == null)
            return new GroupScan(tables.getGroup());
        else
            return bestIndex;
    }

    private IndexScan pickBestIntersection(IndexScan previousBest, IntersectionEnumerator enumerator) {
        // filter out all leaves which are obviously bad
        if (previousBest != null) {
            CostEstimate previousBestCost = previousBest.getCostEstimate();
            for (Iterator<SingleIndexScan> iter = enumerator.leavesIterator(); iter.hasNext(); ) {
                SingleIndexScan scan = iter.next();
                if (scan.getScanCostEstimate().compareTo(previousBestCost) > 0)
                    iter.remove();
            }
        }
        Function<? super IndexScan,Void> hook = intersectionEnumerationHook;
        for (Iterator<IndexScan> iterator = enumerator.iterator(); iterator.hasNext(); ) {
            IndexScan intersectedIndex = iterator.next();
            if (hook != null)
                hook.apply(intersectedIndex);
            setIntersectionConditions(intersectedIndex);
            intersectedIndex.setCovering(determineCovering(intersectedIndex));
            intersectedIndex.setCostEstimate(estimateCost(intersectedIndex));
            if (previousBest == null) {
                logger.debug("Selecting {}", intersectedIndex);
                previousBest = intersectedIndex;
            }
            else if (compare(intersectedIndex, previousBest) > 0) {
                logger.debug("Preferring {}", intersectedIndex);
                previousBest = intersectedIndex;

            }
            else {
                logger.debug("Rejecting {}", intersectedIndex);
                // If the scan costs alone are higher than the previous best cost, there's no way this scan or
                // any scan that uses it will be the best. Just remove the whole branch.
                if (intersectedIndex.getScanCostEstimate().compareTo(previousBest.getCostEstimate()) > 0)
                    iterator.remove();
            }
        }
        return previousBest;
    }

    private void setIntersectionConditions(IndexScan rawScan) {
        MultiIndexIntersectScan scan = (MultiIndexIntersectScan) rawScan;

        ConditionsCounter<ConditionExpression> counter = new ConditionsCounter<ConditionExpression>(conditions.size());
        scan.incrementConditionsCounter(counter);
        scan.setGroupConditions(counter.getCountedConditions());
    }
    
    private class IntersectionEnumerator extends MultiIndexEnumerator<ConditionExpression,IndexScan,SingleIndexScan> {

        @Override
        protected Collection<ConditionExpression> getLeafConditions(SingleIndexScan scan) {
            int skips = scan.getPeggedCount();
            List<ConditionExpression> conditions = scan.getConditions();
            if (conditions == null)
                return null;
            int nconds = conditions.size();
            return ((skips) > 0 && (skips == nconds)) ? conditions : null;
        }

        @Override
        protected IndexScan intersect(IndexScan first, IndexScan second, int comparisons) {
            return new MultiIndexIntersectScan(first, second, comparisons);
        }

        @Override
        protected List<Column> getComparisonColumns(IndexScan first, IndexScan second) {
            EquivalenceFinder<ColumnExpression> equivs = queryGoal.getQuery().getColumnEquivalencies();
            List<ExpressionNode> firstOrdering = orderingCols(first);
            List<ExpressionNode> secondOrdering = orderingCols(second);
            int ncols = Math.min(firstOrdering.size(), secondOrdering.size());
            List<Column> result = new ArrayList<Column>(ncols);
            for (int i=0; i < ncols; ++i) {
                ColumnExpression firstCol = (ColumnExpression) firstOrdering.get(i);
                ColumnExpression secondCol = (ColumnExpression) secondOrdering.get(i);
                if (!equivs.areEquivalent(firstCol, secondCol))
                    break;
                result.add(firstCol.getColumn());
            }
            return result;
        }

        private List<ExpressionNode> orderingCols(IndexScan index) {
            List<ExpressionNode> result = index.getColumns();
            return result.subList(index.getPeggedCount(), result.size());
        }
    }

    public CostEstimate costEstimateScan(PlanNode scan) {
        // TODO: Until more nodes have this stored in them.
        if (scan instanceof IndexScan) {
            return ((IndexScan)scan).getCostEstimate();
        }
        else if (scan instanceof GroupScan) {
            return queryGoal.getCostEstimator().costGroupScan(((GroupScan)scan).getGroup().getGroup());
        }
        else {
            return null;
        }
    }

    /** Find the best index on the given table. 
     * @param required Tables reachable from root via INNER joins and hence not nullable.
     */
    public IndexScan pickBestIndex(TableSource table, Set<TableSource> required, IntersectionEnumerator enumerator) {
        IndexScan bestIndex = null;
        // Can only consider single table indexes when table is not
        // nullable (required).  If table is the optional part of a
        // LEFT join, can still consider compatible LEFT / RIGHT group
        // indexes, below.
        if (required.contains(table)) {
            for (TableIndex index : table.getTable().getTable().getIndexes()) {
                SingleIndexScan candidate = new SingleIndexScan(index, table);
                bestIndex = betterIndex(bestIndex, candidate, enumerator);
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
                bestIndex = betterIndex(bestIndex, candidate, enumerator);
            }
        }
        return bestIndex;
    }

    protected IndexScan betterIndex(IndexScan bestIndex, SingleIndexScan candidate, IntersectionEnumerator enumerator) {
        if (usable(candidate, enumerator)) {
            enumerator.addLeaf(candidate);
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

    public int compare(IndexScan i1, IndexScan i2) {
        return i2.getCostEstimate().compareTo(i1.getCostEstimate());
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
        if ((queryGoal.getOrdering() != null) &&
            (index.getOrderEffectiveness() != IndexScan.OrderEffectiveness.SORTED)) {
            // Only this node, not its inputs.
            filler.setIncludedPlanNodes(Collections.<PlanNode>singletonList(queryGoal.getOrdering()));
            queryGoal.getOrdering().accept(filler);
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
                         (table.getTable() == queryGoal.getUpdateTarget())) {
                    required.add(table);
                }
            }
            index.setRequiredTables(required);
            if (moreTables)
                // Need to join up last the index; index might point
                // to an orphan.
                return false;
        }

        if (queryGoal.getUpdateTarget() != null) {
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

    protected CostEstimate estimateCost(IndexScan index) {
        CostEstimator costEstimator = queryGoal.getCostEstimator();
        CostEstimate cost = getScanOnlyCost(index, costEstimator);
        if (!index.isCovering()) {
            CostEstimate flatten = costEstimator.costFlatten(tables,
                                                             index.getLeafMostTable(),
                                                             index.getRequiredTables());
            cost = cost.nest(flatten);
        }

        Collection<ConditionExpression> unhandledConditions = 
            new HashSet<ConditionExpression>(conditions);
        if (index.getGroupConditions() != null)
            unhandledConditions.removeAll(index.getGroupConditions());
        if (!unhandledConditions.isEmpty()) {
            CostEstimate select = costEstimator.costSelect(unhandledConditions,
                                                           cost.getRowCount());
            cost = cost.sequence(select);
        }

        if (queryGoal.needSort(index.getOrderEffectiveness())) {
            CostEstimate sort = costEstimator.costSort(cost.getRowCount());
            cost = cost.sequence(sort);
        }

        return cost;
    }

    private CostEstimate getScanOnlyCost(IndexScan index, CostEstimator costEstimator) {
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
                        return getScanOnlyCost(scan, costEstimator);
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


    public void install(PlanNode scan, List<ConditionList> conditionSources) {
        tables.setScan(scan);
        if (scan instanceof IndexScan) {
            IndexScan indexScan = (IndexScan)scan;
            if (indexScan instanceof MultiIndexIntersectScan) {
                resetOrdering(indexScan);
            }
            installConditions(indexScan, conditionSources);
            queryGoal.installOrderEffectiveness(indexScan.getOrderEffectiveness());
        }
    }

    /** Change WHERE as a consequence of <code>index</code> being
     * used, using either the sources returned by {@link updateContext} or the
     * current ones if nothing has been changed.
     */
    public void installConditions(IndexScan index, List<ConditionList> conditionSources) {
        if (index.getConditions() != null) {
            if (conditionSources == null)
                conditionSources = this.conditionSources;
            for (ConditionExpression condition : index.getConditions()) {
                for (ConditionList conditionSource : conditionSources) {
                    if (conditionSource.remove(condition))
                        break;
                }
            }
        }
    }

    // Get Range-expressible conditions for given column.
    protected ColumnRanges rangeForIndex(ExpressionNode expressionNode) {
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
            ColumnExpression columnExpression = (ColumnExpression)expressionNode;
            return columnsToRanges.get(columnExpression);
        }
        return null;
    }
    
    static class RequiredColumns {
        private Map<TableSource,Set<ColumnExpression>> map;
        
        public RequiredColumns(TableGroupJoinTree tables) {
            map = new HashMap<TableSource,Set<ColumnExpression>>();
            for (TableGroupJoinNode table : tables) {
                map.put(table.getTable(), new HashSet<ColumnExpression>());
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

        public void clear() {
            for (Set<ColumnExpression> entry : map.values())
                entry.clear();
        }
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
}
