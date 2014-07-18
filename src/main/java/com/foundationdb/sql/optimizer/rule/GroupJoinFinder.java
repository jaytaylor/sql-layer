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

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.server.error.UnsupportedSQLException;

import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;
import com.foundationdb.sql.optimizer.plan.TableGroupJoinTree.TableGroupJoinNode;

import com.foundationdb.server.types.texpressions.Comparison;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Table;

import com.foundationdb.util.ListUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.util.*;

/** Use join conditions to identify which tables are part of the same group.
 */
public class GroupJoinFinder extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(GroupJoinFinder.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        List<JoinIsland> islands = new JoinIslandFinder().find(plan.getPlan());
        new SubqueryBoundTablesTracker(plan) {}.run();
        moveAndNormalizeWhereConditions(islands);
        findGroupJoins(islands);
        reorderJoins(islands);
        isolateGroups(islands);
        moveJoinConditions(islands);
    }
    
    static class JoinIslandFinder implements PlanVisitor, ExpressionVisitor {
        private ColumnEquivalenceStack equivs = new ColumnEquivalenceStack();
        List<JoinIsland> result = new ArrayList<>();

        public List<JoinIsland> find(PlanNode root) {
            root.accept(this);
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            equivs.enterNode(n);
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            equivs.leaveNode(n);
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof Joinable) {
                Joinable joinable = (Joinable)n;
                PlanWithInput output = joinable.getOutput();
                if (!(output instanceof Joinable)) {
                    result.add(new JoinIsland(joinable, output, equivs.get(), equivs.getFks()));
                }
            }
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            return true;
        }
    }

    // A subtree of joins.
    static class JoinIsland {
        Joinable root;
        PlanWithInput output;
        ConditionList whereConditions;
        List<TableGroupJoin> whereJoins;
        EquivalenceFinder<ColumnExpression> columnEquivs;
        EquivalenceFinder<ColumnExpression> fkEquivs;

        public JoinIsland(Joinable root, PlanWithInput output, EquivalenceFinder<ColumnExpression> columnEquivs,
                EquivalenceFinder<ColumnExpression> fkEquivs) {
            this.root = root;
            this.output = output;
            this.columnEquivs = columnEquivs;
            this.fkEquivs = fkEquivs;
            if (output instanceof Select)
                whereConditions = ((Select)output).getConditions();
        }

        public BaseQuery getQuery() {
            PlanWithInput output = root.getOutput();
            BaseQuery baseQuery = null;
            while (output != null) {
                if (output instanceof BaseQuery) {
                    baseQuery = (BaseQuery) output;
                }
                output = output.getOutput();
            }
            return baseQuery;
        }
    }

    // First pass: find all the WHERE conditions above inner joins
    // and put given join condition up there, since it's equivalent.
    // While there, normalize comparisons.
    protected void moveAndNormalizeWhereConditions(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            if (island.whereConditions != null) {
                moveInnerJoinConditions(island.root, island.whereConditions);
                normalizeColumnComparisons(island.whereConditions);
            }
            normalizeColumnComparisons(island.root);
        }
    }

    // So long as there are INNER joins, move their conditions up to
    // the top-level join.
    protected void moveInnerJoinConditions(Joinable joinable,
                                           ConditionList whereConditions) {
        if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            if (joinable.isInnerJoin()) {
                ConditionList joinConditions = join.getJoinConditions();
                if (joinConditions != null) {
                    whereConditions.addAll(joinConditions);
                    joinConditions.clear();
                }
            }
            if (join.getJoinType() != JoinType.RIGHT) {
                moveInnerJoinConditions(join.getLeft(), whereConditions);
            }
            if (join.getJoinType() != JoinType.LEFT) {
                moveInnerJoinConditions(join.getRight(), whereConditions);
            }
        }
    }

    // Make comparisons involving a single column have
    // the form <col> <op> <expr>, with the child on the left in the
    // case of two columns, which is what we may then recognize as a
    // group join.
    protected void normalizeColumnComparisons(ConditionList conditions) {
        if (conditions == null) return;
        Collection<ConditionExpression> newExpressions = new ArrayList<>();
        for (ConditionExpression cond : conditions) {
            if (cond instanceof ComparisonCondition) {
                ComparisonCondition ccond = (ComparisonCondition)cond;
                ExpressionNode left = ccond.getLeft();
                ExpressionNode right = ccond.getRight();
                if (right.isColumn()) {
                    ColumnSource rightTable = ((ColumnExpression)right).getTable();
                    if (left.isColumn()) {
                        ColumnSource leftTable = ((ColumnExpression)left).getTable();
                        if (compareColumnSources(leftTable, rightTable) < 0) {
                            ccond.reverse();
                        }
                    }
                    else {
                        ccond.reverse();
                    }
                }
            }
        }
        conditions.addAll(newExpressions);
        ListUtils.removeDuplicates(conditions);
    }

    // Normalize join's conditions and any below it.
    protected void normalizeColumnComparisons(Joinable joinable) {
        if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            normalizeColumnComparisons(join.getJoinConditions());
            normalizeColumnComparisons(join.getLeft());
            normalizeColumnComparisons(join.getRight());
        }
    }

    // Third pass: put adjacent inner joined tables together in
    // left-deep ascending-ordinal order. E.g. (CO)I.
    protected void reorderJoins(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            Joinable nroot = reorderJoins(island.root);            
            if (island.root != nroot) {
                island.output.replaceInput(island.root, nroot);
                island.root = nroot;
            }
        }
    }

    protected Joinable reorderJoins(Joinable joinable) {
        if (countInnerJoins(joinable) > 1) {
            List<Joinable> joins = new ArrayList<>();
            getInnerJoins(joinable, joins);
            for (int i = 0; i < joins.size(); i++) {
                joins.set(i, reorderJoins(joins.get(i)));
            }
            return orderInnerJoins(joins);
        }
        else if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            join.setLeft(reorderJoins(join.getLeft()));
            join.setRight(reorderJoins(join.getRight()));
            if (compareJoinables(join.getLeft(), join.getRight()) > 0)
                join.reverse();
        }
        return joinable;
    }

    // Make inner joins into a tree of group-tree / non-table.
    protected Joinable orderInnerJoins(List<Joinable> joinables) {
        Map<TableGroup,List<TableSource>> groups = 
            new HashMap<>();
        List<Joinable> nonTables = new ArrayList<>();
        for (Joinable joinable : joinables) {
            if (joinable instanceof TableSource) {
                TableSource table = (TableSource)joinable;
                TableGroup group = table.getGroup();
                List<TableSource> entry = groups.get(group);
                if (entry == null) {
                    entry = new ArrayList<>();
                    groups.put(group, entry);
                }
                entry.add(table);
            }
            else
                nonTables.add(joinable);
        }
        joinables.clear();
        // Make order of groups predictable.
        List<TableGroup> keys = new ArrayList<>(groups.keySet());
        Collections.sort(keys, tableGroupComparator);
        for (TableGroup gkey : keys) {
            List<TableSource> group = groups.get(gkey);
            Collections.sort(group, tableSourceComparator);
            joinables.add(constructLeftInnerJoins(group));
        }
        joinables.addAll(nonTables);
        if (joinables.size() > 1)
            return constructRightInnerJoins(joinables);
        else
            return joinables.get(0);
    }

    // Group flattening is left-recursive.
    protected Joinable constructLeftInnerJoins(List<? extends Joinable> joinables) {
        Joinable result = joinables.get(0);
        for (int i = 1; i < joinables.size(); i++) {
            result = new JoinNode(result, joinables.get(i), JoinType.INNER);
        }
        return result;
    }

    // Nested loop joins are right-recursive.
    protected Joinable constructRightInnerJoins(List<? extends Joinable> joinables) {
        int size = joinables.size();
        Joinable result = joinables.get(--size);
        while (size > 0) {
            result = new JoinNode(joinables.get(--size), result, JoinType.INNER);
            
            // Reset the FK Join if we recreate the Join Tree
            JoinNode node = (JoinNode) result;
            if (node.getRight().isTable() && 
                    (((TableSource)node.getRight()).getParentFKJoin() != null)) {
                node.setFKJoin(((TableSource)node.getRight()).getParentFKJoin());
            }
            if (node.getFKJoin() == null && node.getLeft().isTable() &&
                    (((TableSource)node.getLeft()).getParentFKJoin() != null)) {
                node.setFKJoin(((TableSource)node.getLeft()).getParentFKJoin());
            }
        }
        return result;
    }

    // Second pass: find join conditions corresponding to group joins.
    protected void findGroupJoins(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            List<TableGroupJoin> whereJoins = new ArrayList<>();
            findGroupJoins(island.root, new ArrayDeque<JoinNode>(), 
                           island.whereConditions, whereJoins,
                           island.columnEquivs);
            island.whereJoins = whereJoins;
        }

        //Find FK Joins
        for (JoinIsland island : islands) {
            findFKGroups(island.root, 
                    new ArrayDeque<JoinNode>(),
                    island.whereConditions, 
                    island.columnEquivs,
                    island.fkEquivs);
        }

        // find single groups
        for (JoinIsland island : islands) {
            findSingleGroups(island.root);
        }
    }

    protected void findGroupJoins(Joinable joinable, 
                                  Deque<JoinNode> outputJoins,
                                  ConditionList whereConditions,
                                  List<TableGroupJoin> whereJoins,
                                  EquivalenceFinder<ColumnExpression> columnEquivs) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            for (JoinNode output : outputJoins) {
                ConditionList conditions = output.getJoinConditions();
                TableGroupJoin tableJoin = findParentJoin(table, conditions, columnEquivs);
                if (tableJoin != null) {
                    output.setGroupJoin(tableJoin);
                    return;
                }
            }
            TableGroupJoin tableJoin = findParentJoin(table, whereConditions, columnEquivs);
            if (tableJoin != null) {
                whereJoins.add(tableJoin); // Position after reordering.
                return;
            }
        }
        else if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            outputJoins.push(join);
            if (join.isInnerJoin()) {
                findGroupJoins(join.getLeft(), outputJoins, whereConditions, whereJoins, columnEquivs);
                findGroupJoins(join.getRight(), outputJoins, whereConditions, whereJoins, columnEquivs);
            }
            else {
                Deque<JoinNode> singleJoin = new ArrayDeque<>(1);
                singleJoin.push(join);
                // In a LEFT OUTER JOIN, the outer half is allowed to
                // take from higher conditions.
                if (join.getJoinType() == JoinType.LEFT)
                    findGroupJoins(join.getLeft(), outputJoins, whereConditions, whereJoins, columnEquivs);
                else
                    findGroupJoins(join.getLeft(), singleJoin, null, null, columnEquivs);
                if (join.getJoinType() == JoinType.RIGHT)
                    findGroupJoins(join.getRight(), outputJoins, whereConditions, whereJoins, columnEquivs);
                else
                    findGroupJoins(join.getRight(), singleJoin, null, null, columnEquivs);
            }
            outputJoins.pop();
        }
    }

    // Find a condition among the given conditions that matches the
    // parent join for the given table.
    protected TableGroupJoin findParentJoin(TableSource childTable,
                                            ConditionList conditions,
                                            EquivalenceFinder<ColumnExpression> columnEquivs) {
        if ((conditions == null) || conditions.isEmpty()) return null;
        TableNode childNode = childTable.getTable();
        Join groupJoin = childNode.getTable().getParentJoin();
        if (groupJoin == null) return null;
        TableNode parentNode = childNode.getTree().getNode(groupJoin.getParent());
        if (parentNode == null) return null;
        List<JoinColumn> joinColumns = groupJoin.getJoinColumns();
        int ncols = joinColumns.size();
        Map<TableSource,GroupJoinConditions> parentTables = new HashMap<>();

        for (int i = 0; i < ncols; ++i) {
            if (!findGroupCondition(joinColumns, i, childTable, conditions, true, parentTables, columnEquivs)) {
                if (!findGroupCondition(joinColumns, i, childTable, conditions, false, parentTables, columnEquivs)) {
                    return null; // join column had no direct or equivalent group joins, so we know the answer
                }
            }
        }
        
        TableSource parentTable = null;
        GroupJoinConditions groupJoinConditions = null;
        for (Map.Entry<TableSource,GroupJoinConditions> entry : parentTables.entrySet()) {
            boolean found = true;
            for (ComparisonCondition elem : entry.getValue().getConditions()) {
                if (elem == null) {
                    found = false;
                    break;
                }
            }
            if (found) {
                if (parentTable == null) {
                    parentTable = entry.getKey();
                    groupJoinConditions = entry.getValue();
                }
                else {
                    // TODO: What we need is something
                    // earlier to decide that the primary
                    // keys are equated and so share the
                    // references somehow.
                    ConditionExpression c1 = groupJoinConditions.getConditions().get(0);
                    ConditionExpression c2 = entry.getValue().getConditions().get(0);
                    if (conditions.indexOf(c1) > conditions.indexOf(c2)) {
                        // Make the order predictable for tests.
                        ConditionExpression temp = c1;
                        c1 = c2;
                        c2 = temp;
                    }
                    throw new UnsupportedSQLException("Found two possible parent joins", 
                                                      c2.getSQLsource());
                }
            }
        }
        if (parentTable == null) return null;
        TableGroup group = parentTable.getGroup();
        if (group == null) {
            group = childTable.getGroup();
            if (group == null)
                group = new TableGroup(groupJoin.getGroup());
        }
        else if (childTable.getGroup() != null) {
            group.merge(childTable.getGroup());
        }
        if (!tableAllowedInGroup(group, childTable))
            return null;
        groupJoinConditions.installGeneratedConditionsTo(conditions);
        return new TableGroupJoin(group, parentTable, childTable, groupJoinConditions.getConditions(), groupJoin);
    }

    private boolean findGroupCondition(List<JoinColumn> joinColumns, int i, TableSource childTable,
                                       ConditionList conditions, boolean requireExact,
                                       Map<TableSource, GroupJoinConditions> parentTables,
                                       EquivalenceFinder<ColumnExpression> columnEquivs)
    {
        int ncols = joinColumns.size();
        boolean found = false;
        for (ConditionExpression condition : conditions) {
            List<ColumnExpression> colExprs = findColumnExpressions(condition);
            if (!colExprs.isEmpty()) {
                ComparisonCondition ccond = (ComparisonCondition)condition;
                ComparisonCondition normalized = normalizedCond(
                        joinColumns.get(i).getChild(), joinColumns.get(i).getParent(),
                        childTable,
                        colExprs.get(0),
                        colExprs.get(1),
                        ccond,
                        requireExact,
                        columnEquivs
                );
                if (normalized != null) {
                    found = true;
                    ColumnExpression rnorm = (ColumnExpression) normalized.getRight();
                    TableSource parentSource = (TableSource) rnorm.getTable();
                    GroupJoinConditions entry = parentTables.get(parentSource);
                    if (entry == null) {
                        entry = new GroupJoinConditions(ncols);
                        parentTables.put(parentSource, entry);
                    }
                    entry.set(i, normalized, normalized != ccond);
                }
                
            }
        }
        return found;
    }
    
    private static class GroupJoinConditions {
        private List<ComparisonCondition> conditions;
        private List<ComparisonCondition> generatedConditions;

        public GroupJoinConditions(int ncols) {
            this.conditions = new ArrayList<>(Collections.<ComparisonCondition>nCopies(ncols, null));
        }
        
        public void set(int i, ComparisonCondition condition, boolean wasGenerated) {
            conditions.set(i, condition);
            if (wasGenerated) {
                if (generatedConditions == null)
                    generatedConditions = new ArrayList<>(conditions.size());
                generatedConditions.add(condition);
            }
        }
        
        public List<ComparisonCondition> getConditions() {
            return conditions;
        }

        public void installGeneratedConditionsTo(ConditionList conditionList) {
            if (generatedConditions != null)
                conditionList.addAll(generatedConditions);
        }
    }

    private ComparisonCondition normalizedCond(Column childColumn, Column parentColumn,
                                               TableSource childSource,
                                               ColumnExpression lcol, ColumnExpression rcol,
                                               ComparisonCondition originalCond, boolean requireExact,
                                               EquivalenceFinder<ColumnExpression> columnEquivs)
    {
        // look for child
        ColumnExpression childEquiv = null;
        if (lcol.getTable() == childSource && lcol.getColumn() == childColumn) {
            childEquiv = lcol;
        }
        else {
            for (ColumnExpression equivalent : columnEquivs.findEquivalents(lcol)) {
                if (equivalent.getTable() == childSource && equivalent.getColumn() == childColumn) {
                    childEquiv = equivalent;
                    break;
                }
            }
        }
        if (childEquiv == null)
            return null;
        
        // look for parent
        ColumnExpression parentEquiv = null;
        if (rcol.getColumn() == parentColumn)  {
            parentEquiv = rcol;
        }
        else {
            for (ColumnExpression equivalent : columnEquivs.findEquivalents(rcol)) {
                if (equivalent.getColumn() == parentColumn) {
                    parentEquiv = equivalent;
                    break;
                }
            }
        }
        if (parentEquiv == null)
            return null;
        
        boolean isExact = childEquiv == lcol && parentEquiv == rcol;
        if (requireExact) {
            return isExact ? originalCond : null;
        }
        else {
            assert ! isExact : "exact match found; should have been discovered by previous invocation at call site";
            return new ComparisonCondition(
                    Comparison.EQ,
                    childEquiv,
                    parentEquiv,
                    originalCond.getSQLtype(),
                    originalCond.getSQLsource(),
                    originalCond.getType()
            );
        }
    }

    protected boolean tableAllowedInGroup(TableGroup group, TableSource childTable) {
        return true;
    }

    protected void findFKGroups(Joinable joinable,
            Deque<JoinNode> outputJoins,
            ConditionList whereConditions,
            EquivalenceFinder<ColumnExpression> columnEquivs,
            EquivalenceFinder<ColumnExpression> fkEquivs) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;

            for (JoinNode output : outputJoins) {
                ConditionList conditions = output.getJoinConditions();
                if (table.getGroup() == null) {
                    findParentFKJoin (table, conditions, columnEquivs,fkEquivs);
                } else {
                    break;
                }
            }
            if (table.getGroup() == null) {
                findParentFKJoin (table, whereConditions, columnEquivs, fkEquivs);
            }
        } else if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            outputJoins.push(join);
            findFKGroups (join.getLeft(), outputJoins, whereConditions, columnEquivs, fkEquivs);
            findFKGroups (join.getRight(), outputJoins, whereConditions, columnEquivs, fkEquivs);
            outputJoins.pop();
            
            if (join.getRight().isTable() && 
                    (((TableSource)join.getRight()).getParentFKJoin() != null)) {
                join.setFKJoin(((TableSource)join.getRight()).getParentFKJoin());
            }
        }
    }

    // Find a condition among the given conditions that matches the
    // parent FK join for the given table.
    protected void findParentFKJoin(TableSource childTable,
                                            ConditionList conditions,
                                            EquivalenceFinder<ColumnExpression> columnEquivs,
                                            EquivalenceFinder<ColumnExpression> fkEquivs) {
        if ((conditions == null) || conditions.isEmpty()) return;
        TableNode childNode = childTable.getTable();
        if (childNode.getTable().getForeignKeys() == null || childNode.getTable().getForeignKeys().isEmpty()) return;
        
        for (ForeignKey key : childNode.getTable().getReferencingForeignKeys()) {
            TableFKJoin join = findFKConditions(childTable, conditions, key, columnEquivs, fkEquivs);
            if (join != null) {
                if (childTable.getGroup() == null) {
                    childTable.setGroup(new TableGroup(childTable.getTable().getTable().getGroup()));
                    childTable.setParentFKJoin(join);
                } else {
                    // TODO: If we find a child with 2 parents, don't mark this child as a
                    // FK Join. The JoinAndIndexPicker for the FK selection isn't
                    // (currently) smart enough to reorder the two joins correctly. 
                    childTable.setParentFKJoin(null);
                }
            }
        }
    }
    
    protected TableFKJoin findFKConditions (TableSource childSource,
                ConditionList conditions,
                ForeignKey key, 
                EquivalenceFinder<ColumnExpression> columnEquivs,
                EquivalenceFinder<ColumnExpression> fkEquivs) {
        List<ComparisonCondition> elements = new ArrayList<ComparisonCondition>(key.getReferencedColumns().size());
        TableFKJoin join = null;
        ColumnExpression parentEquiv = null;
        List<JoinColumn> joinColumns = key.getJoinColumns();
        
        for (int i = 0; i < joinColumns.size(); i++) {
            for (ConditionExpression condition : conditions) {
                List<ColumnExpression> columns = findColumnExpressions (condition);
                if (!columns.isEmpty()) {
                    ComparisonCondition ccond = (ComparisonCondition)condition;
                    ComparisonCondition normalized = normalizedCond(
                            joinColumns.get(i).getChild(), joinColumns.get(i).getParent(),
                            childSource,
                            columns.get(0),
                            columns.get(1),
                            ccond,
                            true,
                            fkEquivs
                    );
                    
                    if (normalized == null) {
                        normalized = normalizedCond(
                                joinColumns.get(i).getChild(), joinColumns.get(i).getParent(),
                                childSource,
                                columns.get(0),
                                columns.get(1),
                                ccond,
                                false,
                                fkEquivs
                        );                        
                    }
                    
                    if (normalized != null) {
                        elements.add(normalized);
                        parentEquiv = (ColumnExpression)normalized.getRight();
                        break;
                    }
                }
            }
        }
        if (elements.size() == key.getReferencedColumns().size()) {
            join = new TableFKJoin ((TableSource)parentEquiv.getTable(), childSource, elements, key);
        }
        return join;
    }
    
    protected List<ColumnExpression> findColumnExpressions(ConditionExpression condition) {
        ArrayList<ColumnExpression> columns = new ArrayList<> (2);
        if (condition instanceof ComparisonCondition) {
            ComparisonCondition ccond = (ComparisonCondition)condition;
            if (ccond.getOperation() == Comparison.EQ) {
                ExpressionNode left = ccond.getLeft();
                ExpressionNode right = ccond.getRight();
                if (left.isColumn() && right.isColumn()) {
                    ColumnExpression lcol = (ColumnExpression)left;
                    ColumnExpression rcol = (ColumnExpression)right;
                    if ((lcol.getTable() instanceof TableSource) && (rcol.getTable() instanceof TableSource)) {
                        columns.add(lcol);
                        columns.add(rcol);
                    }
                }
            }
        }
        return columns;
    }
    
    protected void findSingleGroups(Joinable joinable) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            if (table.getGroup() == null) {
                table.setGroup(new TableGroup(table.getTable().getTable().getGroup()));
            }
        }
        else if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            findSingleGroups(join.getLeft());
            findSingleGroups(join.getRight());
        }
    }

    // Fourth pass: wrap contiguous group joins in separate joinable.
    // We have done out best with the inner joins to make this possible,
    // but some outer joins may require that a TableGroup be broken up into
    // multiple TableJoins.
    protected void isolateGroups(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            TableGroupJoinNode tree = isolateGroupJoins(island.root);
            if (tree != null) {
                Joinable nroot = groupJoinTree(tree, island.root);
                island.output.replaceInput(island.root, nroot);
                island.root = nroot;
            }
        }
    }

    protected TableGroupJoinNode isolateGroupJoins(Joinable joinable) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            assert (table.getGroup() != null);
            return new TableGroupJoinNode(table);
        }
        if (!joinable.isJoin())
            return null;
        JoinNode join = (JoinNode)joinable;
        Joinable left = join.getLeft();
        Joinable right = join.getRight();
        TableGroupJoinNode leftTree = isolateGroupJoins(left);
        TableGroupJoinNode rightTree = isolateGroupJoins(right);
        if ((leftTree != null) && (rightTree != null) &&
            // All tables below the two sides must be from the same group.
            (leftTree.getTable().getGroup() == rightTree.getTable().getGroup())) {
            // An outer join condition must be one that can be
            // done before flattening because after that it's too
            // late to get back the outer side if the test never
            // succeeds.
            boolean joinOK;
            switch (join.getJoinType()) {
            case INNER:
                joinOK = true;
                break;
            case LEFT:
                joinOK = checkJoinConditions(join.getJoinConditions(), leftTree, rightTree);
                break;
            case RIGHT:
                // Cannot allow any non-group conditions, since even
                // one only on parent would kill the child because
                // that's how Select_HKeyOrdered works.
                joinOK = checkJoinConditions(join.getJoinConditions(), null, leftTree);
                break;
            default:
                joinOK = false;
            }
            if (joinOK) {
                // Still need to be able to splice them together.
                TableGroupJoinNode tree;
                int leftDepth = leftTree.getTable().getTable().getDepth();
                int rightDepth = rightTree.getTable().getTable().getDepth();
                if (leftDepth < rightDepth)
                    tree = spliceGroupJoins(leftTree, rightTree, join.getJoinType());
                else if (rightDepth < leftDepth)
                    tree = spliceGroupJoins(rightTree, leftTree, join.getJoinType());
                else
                    tree = null;
                if (tree != null) {
                    return tree;
                }
            }
        }
        // Did not manage to coalesce. Put in any intermediate trees.
        if (leftTree != null)
            join.setLeft(groupJoinTree(leftTree, left));
        if (rightTree != null)
            join.setRight(groupJoinTree(rightTree, right));
        // Make arbitrary joins LEFT not RIGHT.
        if (join.getJoinType() == JoinType.RIGHT)
            join.reverse();
        return null;
    }

    protected TableGroupJoinTree groupJoinTree(TableGroupJoinNode root, Joinable joins) {
        TableGroupJoinTree tree = new TableGroupJoinTree(root);
        Set<TableSource> required = new HashSet<>();
        getRequiredTables(joins, required);
        tree.setRequired(required);
        for (TableGroupJoinNode node : root) {
            node.getTable().setOutput(tree); // Instead of join that is going away.
        }
        return tree;
    }

    // Get all the tables reachable via inner joins from here.
    protected void getRequiredTables(Joinable joinable, Set<TableSource> required) {
        if (joinable instanceof TableSource) {
            required.add((TableSource)joinable);
        }
        else if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            if (join.getJoinType() != JoinType.RIGHT)
                getRequiredTables(join.getLeft(), required);
            if (join.getJoinType() != JoinType.LEFT)
                getRequiredTables(join.getRight(), required);
        }
    }

    // Combine trees at the proper branch point.
    protected TableGroupJoinNode spliceGroupJoins(TableGroupJoinNode parent, 
                                                  TableGroupJoinNode child,
                                                  JoinType joinType) {
        TableGroupJoinNode branch = parent.findTable(child.getTable().getParentTable());
        if (branch == null)
            return null;
        child.setParent(branch);
        child.setParentJoinType(joinType);
        TableGroupJoinNode prev = null;
        while (true) {
            TableGroupJoinNode next = (prev == null) ? branch.getFirstChild() : prev.getNextSibling();
            if ((next == null) || 
                (next.getTable().getTable().getOrdinal() > child.getTable().getTable().getOrdinal())) {
                child.setNextSibling(next);
                if (prev == null)
                    branch.setFirstChild(child);
                else
                    prev.setNextSibling(child);
                break;
            }
            prev = next;
        }
        return parent;
    }

    protected boolean checkJoinConditions(ConditionList joinConditions,
                                          TableGroupJoinNode outer,
                                          TableGroupJoinNode inner) {
        if (hasIllegalReferences(joinConditions, outer))
            return false;
        inner.setJoinConditions(joinConditions);
        return true;
    }

    // See whether any expression in the join condition other than the
    // grouping join references a table under the given tree.
    protected boolean hasIllegalReferences(ConditionList joinConditions,
                                           TableGroupJoinNode fromTree) {
        JoinedReferenceFinder finder = null;
        if (joinConditions != null) {
            for (ConditionExpression condition : joinConditions) {
                if (condition.getImplementation() == ConditionExpression.Implementation.GROUP_JOIN)
                    continue;   // Group condition okay.
                if (fromTree == null)
                    return true; // All non-group disallowed.
                if (finder == null)
                    finder = new JoinedReferenceFinder(fromTree);
                if (finder.find(condition))
                    return true; // Has references to other side.
            }
        }
        return false;
    }

    static class JoinedReferenceFinder implements PlanVisitor, ExpressionVisitor {
        private TableGroupJoinNode fromTree;
        private boolean found;

        public JoinedReferenceFinder(TableGroupJoinNode fromTree) {
            this.fromTree = fromTree;
        }

        public boolean find(ExpressionNode expression) {
            found = false;
            expression.accept(this);
            return found;
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
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression) {
                ColumnSource table = ((ColumnExpression)n).getTable();
                if (table instanceof TableSource) {
                    if (fromTree.findTable((TableSource)table) != null) {
                        found = true;
                    }
                }
            }
            return true;
        }
    }
    public static class ConditionTableSources implements PlanVisitor, ExpressionVisitor {
        List<TableSource> referencedTables;

        public ConditionTableSources() {
        }

        public List<TableSource> find(ExpressionNode expression) {
            referencedTables = new ArrayList<>();
            expression.accept(this);
            return referencedTables;
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
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression) {
                ColumnSource table = ((ColumnExpression)n).getTable();
                // TODO should this return a list of ColumnSources instead of TableSources?
                if (table instanceof TableSource) {
                    referencedTables.add((TableSource) table);
                }
            }
            return true;
        }
    }

    // Fifth pass: move the WHERE conditions back to their actual
    // joins, which may be different from the ones they were on in the
    // original query and reject any group joins that now cross TableJoins.
    protected void moveJoinConditions(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            moveJoinConditions(island.root, island.whereConditions, island.whereJoins);
            if (island.whereConditions != null) {
                Iterator<ConditionExpression> iterator = island.whereConditions.iterator();
                while (iterator.hasNext()) {
                    ConditionExpression condition = iterator.next();
                    List<TableSource> tableSources = new ConditionTableSources().find(condition);
                    tableSources.removeAll(island.getQuery().getOuterTables());
                    if (moveWhereCondition(tableSources, condition, island.root)) {
                        iterator.remove();
                    }
                }
            }

        }
    }

    protected void moveJoinConditions(Joinable joinable,
                                      ConditionList whereConditions, List<TableGroupJoin> whereJoins) {
        if (joinable instanceof TableGroupJoinTree) {
            for (TableGroupJoinNode table : (TableGroupJoinTree)joinable) {
                TableGroupJoin tableJoin = table.getTable().getParentJoin();
                if (tableJoin != null) {
                    if (table.getParent() == null) {
                        tableJoin.reject(); // Did not make it into the group.
                    }
                    else if (whereJoins.contains(tableJoin)) {
                        List<ComparisonCondition> joinConditions = tableJoin.getConditions();
                        // Move down from WHERE conditions to join conditions.
                        if (table.getJoinConditions() == null)
                            table.setJoinConditions(new ConditionList());
                        table.getJoinConditions().addAll(joinConditions);
                        whereConditions.removeAll(joinConditions);
                    }
                }
            }
        }
        else if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            join.setGroupJoin(null);
            moveJoinConditions(join.getLeft(), whereConditions, whereJoins);
            moveJoinConditions(join.getRight(), whereConditions, whereJoins);
        }
    }
    /**
     * Moves the given condition as far down as possible, so long as the tableSources are visible
     * @param tableSources the tableSources referenced by the condition. All sources that are declared in a nested
     *                     join will be removed from the tableSources
     * @return true if the condition was added to a joinConditions
     * TODO switch tableSources to set
     */
    private boolean moveWhereCondition(List<TableSource> tableSources, ConditionExpression condition, Joinable joinable) {
        // TODO: what about table sources from the superquery?
        // TODO: moving it into index scan
        // see com/foundationdb/sql/optimizer/rule/find-groups/subselect-via-equivalence.expected
        // If we move Any/Exists Conditions down, the InConditionReverser won't be able to find them,
        // so they get to stay in the where clause
        if (condition instanceof AnyCondition || condition instanceof ExistsCondition) {
            return false;
        }
        if (joinable instanceof TableGroupJoinTree) {
            for (TableGroupJoinNode table : (TableGroupJoinTree)joinable) {
                tableSources.remove(table.getTable());
            }
        } else if (joinable instanceof JoinNode)
        {
            JoinNode join = (JoinNode)joinable;
            if (join.isInnerJoin()) {
                // TODO if forLeft becomes empty, don't run forRight
                // TODO improve performance of this
                List<TableSource> forLeft = new ArrayList<>(tableSources);
                List<TableSource> forRight = new ArrayList<>(tableSources);
                if (moveWhereCondition(forLeft, condition, join.getLeft()) ||
                        moveWhereCondition(forRight, condition, join.getRight())) {
                    return true;
                }
                tableSources.retainAll(forLeft);
                tableSources.retainAll(forRight);
            }
            // TODO shouldn't this be moved inside the isInnerJoin()
            if (tableSources.isEmpty()) {
                if (join.getJoinConditions() == null)
                {
                    join.setJoinConditions(new ConditionList());
                }
                join.getJoinConditions().add(condition);
                return true;
            }
            return false;
        } else if (joinable instanceof TableSource)
        {
            tableSources.remove(joinable);
            return false;
        }
        return false;
    }

    static final Comparator<TableGroup> tableGroupComparator = new Comparator<TableGroup>() {
        @Override
        public int compare(TableGroup tg1, TableGroup tg2) {
            Group g1 = tg1.getGroup();
            Group g2 = tg2.getGroup();
            if (g1 != g2)
                return g1.getName().compareTo(g2.getName());
            int o1 = tg1.getMinOrdinal();
            int o2 = tg2.getMinOrdinal();
            if (o1 == o2) {
                TableSource ts1 = tg1.findByOrdinal(o1);
                TableSource ts2 = tg2.findByOrdinal(o2);
                return ts1.getName().compareTo(ts2.getName());
            }
            return o1 - o2;
        }
    };

    static final Comparator<TableSource> tableSourceComparator = new Comparator<TableSource>() {
        public int compare(TableSource t1, TableSource t2) {
            return compareTableSources(t1, t2);
        }
    };

    protected static int compareColumnSources(ColumnSource c1, ColumnSource c2) {
        if (c1 instanceof TableSource) {
            if (!(c2 instanceof TableSource))
                return +1;
            return compareTableSources((TableSource)c1, (TableSource)c2);
        }
        else if (c2 instanceof TableSource)
            return -1;
        else
            return 0;
    }
    
    protected static int compareTableSources(TableSource ts1, TableSource ts2) {
        TableNode t1 = ts1.getTable();
        Table ut1 = t1.getTable();
        Group g1 = ut1.getGroup();
        TableGroup tg1 = ts1.getGroup();
        TableNode t2 = ts2.getTable();
        Table ut2 = t2.getTable();
        Group g2 = ut2.getGroup();
        TableGroup tg2 = ts2.getGroup();
        if (g1 != g2)
            return g1.getName().compareTo(g2.getName());
        if (tg1 == tg2) {       // Including null because not yet computed.
            if (ut1 == ut2)
                return ts1.getName().compareTo(ts2.getName());
            return t1.getOrdinal() - t2.getOrdinal();
        }
        return tg1.getMinOrdinal() - tg2.getMinOrdinal();
    }

    // Return size of directly-reachable subtree of all simple inner joins.
    protected static int countInnerJoins(Joinable joinable) {
        if (!isSimpleInnerJoin(joinable))
            return 0;
        return 1 +
            countInnerJoins(((JoinNode)joinable).getLeft()) +
            countInnerJoins(((JoinNode)joinable).getRight());
    }


    // Accumulate operands of directly-reachable subtree of simple inner joins.
    protected static void getInnerJoins(Joinable joinable, Collection<Joinable> into) {
        if (!isSimpleInnerJoin(joinable))
            into.add(joinable);
        else {
            getInnerJoins(((JoinNode)joinable).getLeft(), into);
            getInnerJoins(((JoinNode)joinable).getRight(), into);
        }
    }

    // Can this inner join be reorderd?
    // TODO: If there are inner joins with conditions that didn't get
    // moved by the first pass, leave them alone. That will miss
    // opportunities.  Need to have a way to accumulate those
    // conditions and put them into the join tree.
    protected static boolean isSimpleInnerJoin(Joinable joinable) {
        return (joinable.isInnerJoin() && !((JoinNode)joinable).hasJoinConditions());
    }

    protected static int compareJoinables(Joinable j1, Joinable j2) {
        if (j1.isTable() && j2.isTable())
            return compareTableSources((TableSource)j1, (TableSource)j2);
        Group g1 = singleGroup(j1);
        Group g2 = singleGroup(j2);
        if (g1 == null) {
            if (g2 != null)
                return -1;
            else
                return 0;
        }
        else if (g2 == null)
            return +1;
        if (g1 != g2)
            return g1.getName().compareTo(g2.getName());
        int[] range1 = ordinalRange(j1);
        int[] range2 = ordinalRange(j2);
        if (range1[1] < range2[0])
            return -1;
        else if (range1[0] > range2[1])
            return +1;
        else
            return 0;
    }

    protected static Group singleGroup(Joinable j) {
        if (j.isTable())
            return ((TableSource)j).getTable().getGroup();
        else if (j.isJoin()) {
            JoinNode join = (JoinNode)j;
            Group gl = singleGroup(join.getLeft());
            Group gr = singleGroup(join.getRight());
            if (gl == gr)
                return gl;
            else
                return null;
        }
        else
            return null;
    }

    protected static int[] ordinalRange(Joinable j) {
        if (j.isTable()) {
            int ord = ((TableSource)j).getTable().getOrdinal();
            return new int[] { ord, ord };
        }
        else if (j.isJoin()) {
            JoinNode join = (JoinNode)j;
            int[] ol = ordinalRange(join.getLeft());
            int[] or = ordinalRange(join.getRight());
            if (ol[0] > or[0])
                ol[0] = or[0];
            if (ol[1] < or[1])
                ol[1] = or[1];
            return ol;
        }
        else
            return new int[] { -1, -1 };
    }

}
