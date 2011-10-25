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

import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;

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
            if (join.getLeft().isTable()) {
                if (join.getRight().isTable()) {
                    if (compareTableSources((TableSource)join.getLeft(),
                                            (TableSource)join.getRight()) > 0)
                        join.reverse();
                }
                else
                    join.reverse();
            }
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
            findGroupJoins(island.root, null, island.whereConditions, whereJoins);
            island.whereJoins = whereJoins;
        }
        for (JoinIsland island : islands) {
            findSingleGroups(island.root);
        }
    }

    protected void findGroupJoins(Joinable joinable, JoinNode output,
                                  ConditionList whereConditions,
                                  List<TableGroupJoin> whereJoins) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            ConditionList conditions = null;
            if (output != null)
                conditions = output.getJoinConditions();
            if ((conditions != null) && conditions.isEmpty())
                conditions = null;
            if (conditions == null)
                conditions = whereConditions;
            TableGroupJoin tableJoin = findParentJoin(table, conditions);
            if (tableJoin != null) {
                if (conditions == whereConditions)
                    whereJoins.add(tableJoin); // Position after reordering.
                else
                    output.setGroupJoin(tableJoin);
            }
        }
        else if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            Joinable right = join.getRight();
            if (!join.isInnerJoin())
                whereConditions = null;
            findGroupJoins(join.getLeft(), join, whereConditions, whereJoins);
            findGroupJoins(join.getRight(), join, whereConditions, whereJoins);
        }
    }

    // Find a condition among the given conditions that matches the
    // parent join for the given table.
    protected TableGroupJoin findParentJoin(TableSource childTable,
                                            ConditionList conditions) {
        if (conditions == null) return null;
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
    
    protected static int compareJoinables(Joinable j1, Joinable j2) {
        if (j1.isTable()) {
            if (!j2.isTable())
                return -1;
            return compareTableSources((TableSource)j1, (TableSource)j2);
        }
        else if (j2.isTable())
            return +1;
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

    // Return size of directly-reachable subtree of all inner joins.
    protected static int countInnerJoins(Joinable joinable) {
        if (!joinable.isInnerJoin())
            return 0;
        return 1 +
            countInnerJoins(((JoinNode)joinable).getLeft()) +
            countInnerJoins(((JoinNode)joinable).getRight());
    }

    // Accumulate operands of directly-reachable subtree of inner joins.
    protected static void getInnerJoins(Joinable joinable, Collection<Joinable> into) {
        if (!joinable.isInnerJoin())
            into.add(joinable);
        else {
            getInnerJoins(((JoinNode)joinable).getLeft(), into);
            getInnerJoins(((JoinNode)joinable).getRight(), into);
        }
    }

}
