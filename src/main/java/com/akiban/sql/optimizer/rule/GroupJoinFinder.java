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

import com.akiban.ais.model.Column;
import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import com.akiban.server.expression.std.Comparison;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;

import com.akiban.util.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        moveAndNormalizeWhereConditions(islands);
        findGroupJoins(islands);
        reorderJoins(islands);
        moveJoinConditions(islands);
        isolateGroups(islands);
    }
    
    static class JoinIslandFinder implements PlanVisitor, ExpressionVisitor {
        List<JoinIsland> result = new ArrayList<JoinIsland>();

        public List<JoinIsland> find(PlanNode root) {
            root.accept(this);
            return result;
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
            if (n instanceof Joinable) {
                Joinable joinable = (Joinable)n;
                PlanWithInput output = joinable.getOutput();
                if (!(output instanceof Joinable)) {
                    result.add(new JoinIsland(joinable, output));
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

        public JoinIsland(Joinable root, PlanWithInput output) {
            this.root = root;
            this.output = output;
            if (output instanceof Select)
                whereConditions = ((Select)output).getConditions();
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
        if (joinable.isInnerJoin()) {
            JoinNode join = (JoinNode)joinable;
            ConditionList joinConditions = join.getJoinConditions();
            if (joinConditions != null) {
                whereConditions.addAll(joinConditions);
                joinConditions.clear();
            }
            moveInnerJoinConditions(join.getLeft(), whereConditions);
            moveInnerJoinConditions(join.getRight(), whereConditions);
        }
    }

    // Make comparisons involving a single column have
    // the form <col> <op> <expr>, with the child on the left in the
    // case of two columns, which is what we may then recognize as a
    // group join.
    protected void normalizeColumnComparisons(ConditionList conditions) {
        if (conditions == null) return;
        Collection<ConditionExpression> newExpressions = new ArrayList<ConditionExpression>();
        for (Iterator<ConditionExpression> iterator = conditions.iterator(); iterator.hasNext(); ) {
            ConditionExpression cond = iterator.next();
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
                    boolean conditionIsObsolete = normalizeGroupJoinCondition(ccond, newExpressions);
                    if (conditionIsObsolete)
                        iterator.remove();
                }
            }
        }
        conditions.addAll(newExpressions);
        ListUtils.removeDuplicates(conditions);
    }

    private boolean normalizeGroupJoinCondition(ComparisonCondition ccond, Collection<? super ConditionExpression> out)
    {
        boolean conditionIsObsolete = false;
        if (ccond.getOperation().equals(Comparison.EQ)) {
            ExpressionNode leftRaw = ccond.getLeft();
            ExpressionNode rightRaw = ccond.getRight();
            if (leftRaw instanceof ColumnExpression && rightRaw instanceof ColumnExpression) {
                ColumnExpression ccondLeft = (ColumnExpression) leftRaw;
                ColumnExpression ccondRight = (ColumnExpression) rightRaw;

                if (ccondLeft.getColumn() == null || ccondLeft.getColumn().getUserTable() == null
                        || ccondRight.getColumn() == null || ccondRight.getColumn().getUserTable() == null)
                    return false;

                boolean conditionOnDifferentTables =
                        ccondLeft.getColumn().getUserTable() != ccondRight.getColumn().getUserTable();
                
                for (ColumnExpression leftColExprEquiv : ccondLeft.getEquivalentsPlusSelf()) {
                    for (ColumnExpression rightColExprEquiv : ccondRight.getEquivalentsPlusSelf()) {
                        Column leftColEquiv = leftColExprEquiv.getColumn();
                        Column rightColEquiv = rightColExprEquiv.getColumn();
                        UserTable leftEquivTable = leftColEquiv.getUserTable();
                        UserTable rightEquivTable = rightColEquiv.getUserTable();
                        Join parentJoin = leftEquivTable.getParentJoin();
                        if (parentJoin != null && parentJoin.getParent() != null
                                && parentJoin.getParent().equals(rightEquivTable))
                        {
                            // found a parent-child relationship
                            for (JoinColumn joinColumn : parentJoin.getJoinColumns()) {
                                Column parentCol = joinColumn.getParent();
                                Column childCol = joinColumn.getChild();
                                // look for a group join condition that isn't the original one
                                if (leftColEquiv.equals(childCol) && rightColEquiv.equals(parentCol) &&
                                    (leftColEquiv != ccondLeft.getColumn() || rightColEquiv != ccondRight.getColumn()))
                                {
                                    // create a new comparison condition that's in canonical form
                                    ComparisonCondition canonical = new ComparisonCondition(
                                            Comparison.EQ,
                                            leftColExprEquiv,
                                            rightColExprEquiv,
                                            ccond.getSQLtype(),
                                            ccond.getSQLsource()
                                    );
                                    out.add(canonical);
                                    conditionIsObsolete |= conditionOnDifferentTables;
                                    logger.debug("rewriting {} as {}", ccond, canonical);
                                }
                            }
                        }
                    }
                }
            }
        }
        return conditionIsObsolete;
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
            List<Joinable> joins = new ArrayList<Joinable>();
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
            new HashMap<TableGroup,List<TableSource>>();
        List<Joinable> nonTables = new ArrayList<Joinable>();
        for (Joinable joinable : joinables) {
            if (joinable instanceof TableSource) {
                TableSource table = (TableSource)joinable;
                TableGroup group = table.getGroup();
                List<TableSource> entry = groups.get(group);
                if (entry == null) {
                    entry = new ArrayList<TableSource>();
                    groups.put(group, entry);
                }
                entry.add(table);
            }
            else
                nonTables.add(joinable);
        }
        joinables.clear();
        // Make order of groups predictable.
        List<TableGroup> keys = new ArrayList(groups.keySet());
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
        }
        return result;
    }

    // Second pass: find join conditions corresponding to group joins.
    protected void findGroupJoins(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            List<TableGroupJoin> whereJoins = new ArrayList<TableGroupJoin>();
            findGroupJoins(island.root, new ArrayDeque<JoinNode>(), 
                           island.whereConditions, whereJoins);
            island.whereJoins = whereJoins;
        }
        for (JoinIsland island : islands) {
            findSingleGroups(island.root);
        }
    }

    protected void findGroupJoins(Joinable joinable, 
                                  Deque<JoinNode> outputJoins,
                                  ConditionList whereConditions,
                                  List<TableGroupJoin> whereJoins) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            for (JoinNode output : outputJoins) {
                ConditionList conditions = output.getJoinConditions();
                TableGroupJoin tableJoin = findParentJoin(table, conditions);
                if (tableJoin != null) {
                    output.setGroupJoin(tableJoin);
                    return;
                }
            }
            TableGroupJoin tableJoin = findParentJoin(table, whereConditions);
            if (tableJoin != null) {
                whereJoins.add(tableJoin); // Position after reordering.
                return;
            }
        }
        else if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            Joinable right = join.getRight();
            outputJoins.push(join);
            if (join.isInnerJoin()) {
                findGroupJoins(join.getLeft(), outputJoins, whereConditions, whereJoins);
                findGroupJoins(join.getRight(), outputJoins, whereConditions, whereJoins);
            }
            else {
                Deque<JoinNode> singleJoin = new ArrayDeque<JoinNode>(1);
                singleJoin.push(join);
                // In a LEFT OUTER JOIN, the outer half is allowed to
                // take from higher conditions.
                if (join.getJoinType() == JoinType.LEFT)
                    findGroupJoins(join.getLeft(), outputJoins, whereConditions, whereJoins);
                else
                    findGroupJoins(join.getLeft(), singleJoin, null, null);
                if (join.getJoinType() == JoinType.RIGHT)
                    findGroupJoins(join.getRight(), outputJoins, whereConditions, whereJoins);
                else
                    findGroupJoins(join.getRight(), singleJoin, null, null);
            }
            outputJoins.pop();
        }
    }

    // Find a condition among the given conditions that matches the
    // parent join for the given table.
    protected TableGroupJoin findParentJoin(TableSource childTable,
                                            ConditionList conditions) {
        if ((conditions == null) || conditions.isEmpty()) return null;
        TableNode childNode = childTable.getTable();
        Join groupJoin = childNode.getTable().getParentJoin();
        if (groupJoin == null) return null;
        TableNode parentNode = childNode.getTree().getNode(groupJoin.getParent());
        if (parentNode == null) return null;
        List<JoinColumn> joinColumns = groupJoin.getJoinColumns();
        int ncols = joinColumns.size();
        Map<TableSource,List<ComparisonCondition>> parentTables = 
            new HashMap<TableSource,List<ComparisonCondition>>();
        for (ConditionExpression condition : conditions) {
            if (condition instanceof ComparisonCondition) {
                ComparisonCondition ccond = (ComparisonCondition)condition;
                if (ccond.getOperation() == Comparison.EQ) {
                    ExpressionNode left = ccond.getLeft();
                    ExpressionNode right = ccond.getRight();
                    if (left.isColumn() && right.isColumn()) {
                        ColumnExpression lcol = (ColumnExpression)left;
                        ColumnExpression rcol = (ColumnExpression)right;
                        if (lcol.getTable() == childTable) {
                            ColumnSource rightSource = rcol.getTable();
                            if (rightSource instanceof TableSource) {
                                TableSource rightTable = (TableSource)rightSource;
                                if (rightTable.getTable() == parentNode) {
                                    for (int i = 0; i < ncols; i++) {
                                        JoinColumn joinColumn = joinColumns.get(i);
                                        if ((joinColumn.getChild() == lcol.getColumn()) &&
                                            (joinColumn.getParent() == rcol.getColumn())) {
                                            List<ComparisonCondition> entry = 
                                                parentTables.get(rightTable);
                                            if (entry == null) {
                                                entry = new ArrayList<ComparisonCondition>(Collections.<ComparisonCondition>nCopies(ncols, null));
                                                parentTables.put(rightTable, entry);
                                            }
                                            entry.set(i, ccond);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        TableSource parentTable = null;
        List<ComparisonCondition> groupJoinConditions = null;
        for (Map.Entry<TableSource,List<ComparisonCondition>> entry : parentTables.entrySet()) {
            boolean found = true;
            for (ComparisonCondition elem : entry.getValue()) {
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
                    ConditionExpression c1 = groupJoinConditions.get(0);
                    ConditionExpression c2 = entry.getValue().get(0);
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
        // TODO: Avoid duplicate group joins. Really, they should be
        // recognized but only one allowed to Flatten and the other
        // forced to use a nested loop, but still with BranchLookup.
        for (TableSource otherChild : group.getTables()) {
            if ((otherChild.getTable() == childTable.getTable()) &&
                (otherChild != childTable))
                return null;
        }
        return new TableGroupJoin(group, parentTable, childTable, 
                                  groupJoinConditions, groupJoin);
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
            Joinable right = join.getRight();
            findSingleGroups(join.getLeft());
            findSingleGroups(join.getRight());
        }
    }

    // Fourth pass: move the WHERE conditions back to their actual
    // joins, which may be different from the ones they were on in the
    // original query.
    protected void moveJoinConditions(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            if (!island.whereJoins.isEmpty())
                moveJoinConditions(island.root, null, 
                                   island.whereConditions, island.whereJoins);
        }        
    }

    protected void moveJoinConditions(Joinable joinable, JoinNode output,
                                      ConditionList whereConditions,
                                      List<TableGroupJoin> whereJoins) {
        if (joinable.isTable()) {
            if (output != null) {
                TableSource table = (TableSource)joinable;
                TableGroupJoin tableJoin = table.getParentJoin();
                if (whereJoins.contains(tableJoin)) {
                    output.setGroupJoin(tableJoin);
                    List<ComparisonCondition> joinConditions = tableJoin.getConditions();
                    // Move down from WHERE conditions to join conditions.
                    if (output.getJoinConditions() == null)
                        output.setJoinConditions(new ConditionList());
                    output.getJoinConditions().addAll(joinConditions);
                    whereConditions.removeAll(joinConditions);
                }
            }
        }
        else if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            moveJoinConditions(join.getLeft(), join, whereConditions, whereJoins);
            moveJoinConditions(join.getRight(), join, whereConditions, whereJoins);
        }
    }

    // Fifth pass: wrap contiguous group joins in separate joinable.
    // We have done out best with the inner joins to make this possible,
    // but some outer joins may require that a TableGroup be broken up into
    // multiple TableJoins.
    protected void isolateGroups(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            TableGroup group = isolateGroups(island.root);
            if (group != null) {
                Joinable nroot = getTableJoins(island.root, group);
                island.output.replaceInput(island.root, nroot);
                island.root = nroot;
            }
        }
    }

    protected TableGroup isolateGroups(Joinable joinable) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            assert (table.getGroup() != null);
            return table.getGroup();
        }
        if (!joinable.isJoin())
            return null;
        // Both sides must be matching groups.
        JoinNode join = (JoinNode)joinable;
        Joinable left = join.getLeft();
        Joinable right = join.getRight();
        TableGroup leftGroup = isolateGroups(left);
        TableGroup rightGroup = isolateGroups(right);
        if ((leftGroup == rightGroup) && (leftGroup != null))
            return leftGroup;
        if (leftGroup != null)
            join.setLeft(getTableJoins(left, leftGroup));
        if (rightGroup != null)
            join.setRight(getTableJoins(right, rightGroup));
        // Make arbitrary joins LEFT not RIGHT.
        if (join.getJoinType() == JoinType.RIGHT)
            join.reverse();
        return null;
    }

    // Make a new TableGroup, recording what it contains.
    protected TableJoins getTableJoins(Joinable joins, TableGroup group) {
        TableJoins tableJoins = new TableJoins(joins, group);
        getTableJoinsTables(joins, tableJoins);
        return tableJoins;
    }

    protected void getTableJoinsTables(Joinable joinable, TableJoins tableJoins) {
        if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            getTableJoinsTables(join.getLeft(), tableJoins);
            getTableJoinsTables(join.getRight(), tableJoins);
        }
        else {
            assert joinable.isTable();
            tableJoins.addTable((TableSource)joinable);
        }
    }

    static final Comparator<TableGroup> tableGroupComparator = new Comparator<TableGroup>() {
        @Override
        public int compare(TableGroup tg1, TableGroup tg2) {
            Group g1 = tg1.getGroup();
            Group g2 = tg2.getGroup();
            if (g1 != g2)
                return g1.getName().compareTo(g2.getName());
            return tg1.getMinOrdinal() - tg2.getMinOrdinal();
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
        UserTable ut1 = t1.getTable();
        Group g1 = ut1.getGroup();
        TableGroup tg1 = ts1.getGroup();
        TableNode t2 = ts2.getTable();
        UserTable ut2 = t2.getTable();
        Group g2 = ut2.getGroup();
        TableGroup tg2 = ts2.getGroup();
        if (g1 != g2)
            return g1.getName().compareTo(g2.getName());
        if (tg1 == tg2)         // Including null because not yet computed.
            return t1.getOrdinal() - t2.getOrdinal();
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
