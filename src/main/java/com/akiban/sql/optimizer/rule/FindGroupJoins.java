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

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.physicaloperator.API.JoinType;

import java.util.*;

/** Use join conditions to identify which tables are part of the same group.
 */
public class FindGroupJoins extends BaseRule
{
    static class JoinIslandFinder implements PlanVisitor, ExpressionVisitor {
        List<Joinable> result = new ArrayList<Joinable>();

        public List<Joinable> find(PlanNode root) {
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
                Joinable j = (Joinable)n;
                if (!(j.getOutput() instanceof Joinable))
                    result.add(j);
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

    @Override
    public PlanNode apply(PlanNode plan) {
        List<Joinable> islands = new JoinIslandFinder().find(plan);
        reorderInnerJoins(islands);
        moveAndNormalizeWhereConditions(islands);
        findGroupJoins(islands);
        return plan;
    }
    
    // First pass: put adjacent inner joined tables together in
    // right-deep ascending ordinal order.
    protected void reorderInnerJoins(List<Joinable> islands) {
        for (Joinable island : islands) {
            island.getOutput().replaceInput(island, reorderInnerJoins(island));
        }        
    }

    protected Joinable reorderInnerJoins(Joinable joinable) {
        if (countInnerJoins(joinable) > 1) {
            List<Joinable> joins = new ArrayList<Joinable>();
            getInnerJoins(joinable, joins);
            for (int i = 0; i < joins.size(); i++) {
                joins.set(i, reorderInnerJoins(joins.get(i)));
            }
            Collections.sort(joins, new Comparator<Joinable>() {
                                 public int compare(Joinable j1, Joinable j2) {
                                     return compareJoinables(j1, j2);
                                 }
                             });
            Joinable result = joins.get(0);
            for (int i = 1; i < joins.size(); i++) {
                result = new JoinNode(result, joins.get(i), JoinType.INNER_JOIN);
            }
            return result;
        }
        return joinable;
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
        TableNode t2 = ts2.getTable();
        UserTable ut2 = t2.getTable();
        Group g2 = ut2.getGroup();
        if (g1 == g2) {
            return t1.getOrdinal() - t2.getOrdinal();
        }
        else
            return g1.getName().compareTo(g2.getName());
    }

    // Return size of directly-reachable subtree of all inner joins.
    protected int countInnerJoins(Joinable joinable) {
        if (!joinable.isInnerJoin())
            return 0;
        return 1 +
            countInnerJoins(((JoinNode)joinable).getLeft()) +
            countInnerJoins(((JoinNode)joinable).getRight());
    }

    // Accumulate operands of directly-reachable subtree of inner joins.
    protected void getInnerJoins(Joinable joinable, Collection<Joinable> into) {
        if (!joinable.isInnerJoin())
            into.add(joinable);
        else {
            getInnerJoins(((JoinNode)joinable).getLeft(), into);
            getInnerJoins(((JoinNode)joinable).getRight(), into);
        }
    }
    
    // Second pass: find all the WHERE conditions above inner joins
    // and put given join condition up there, since it's equivalent.
    // Where there, normalize comparisons.
    protected void moveAndNormalizeWhereConditions(List<Joinable> islands) {
        for (Joinable island : islands) {
            if (island.getOutput() instanceof Filter) {
                List<ConditionExpression> conditions = 
                    ((Filter)island.getOutput()).getConditions();
                moveInnerJoinConditions(island, conditions);
                normalizeColumnComparisons(conditions);
            }
            normalizeColumnComparisons(island);
        }
    }

    // So long as there are INNER joins, move their conditions up to
    // the top-level join.
    protected void moveInnerJoinConditions(Joinable joinable,
                                           List<ConditionExpression> whereConditions) {
        if (joinable.isInnerJoin()) {
            JoinNode join = (JoinNode)joinable;
            List<ConditionExpression> joinConditions = join.getJoinConditions();
            whereConditions.addAll(joinConditions);
            joinConditions.clear();
            moveInnerJoinConditions(join.getLeft(), whereConditions);
            moveInnerJoinConditions(join.getRight(), whereConditions);
        }
    }

    // Make comparisons involving a single column have
    // the form <col> <op> <expr>, with the child on the left in the
    // case of two columns, which is what we may then recognize as a
    // group join.
    protected void normalizeColumnComparisons(List<ConditionExpression> conditions) {
        for (ConditionExpression cond : conditions) {
            if (cond instanceof ComparisonCondition) {
                ComparisonCondition ccond = (ComparisonCondition)cond;
                ExpressionNode left = ccond.getLeft();
                ExpressionNode right = ccond.getRight();
                if (right.isColumn()) {
                    ColumnSource rightTable = ((ColumnExpression)right).getTable();
                    if (left.isColumn()) {
                        ColumnSource leftTable = ((ColumnExpression)left).getTable();
                        if (compareColumnSources(leftTable, rightTable) > 0) {
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

    protected static int compareColumnSources(ColumnSource c1, ColumnSource c2) {
        if (c1 instanceof TableSource) {
            if (!(c2 instanceof TableSource))
                return -1;
            return compareTableSources((TableSource)c1, (TableSource)c2);
        }
        else if (c2 instanceof TableSource)
            return +1;
        else
            return 0;
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

    // Third pass: find join conditions corresponding to group joins.
    protected void findGroupJoins(List<Joinable> islands) {
        for (Joinable island : islands) {
            List<ConditionExpression> whereConditions = null;
            if (island.getOutput() instanceof Filter)
                whereConditions = ((Filter)island.getOutput()).getConditions();
            findGroupJoins(island, whereConditions);
        }
    }

    protected void findGroupJoins(Joinable joinable,
                                  List<ConditionExpression> whereConditions) {
        if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            Joinable right = join.getRight();
            if (!join.isInnerJoin())
                whereConditions = null;
            if (right.isTable()) {
                TableSource table = (TableSource)right;
                List<ConditionExpression> conditions = join.getJoinConditions();
                if (conditions.isEmpty() && (whereConditions != null)) {
                    conditions = whereConditions;
                }
                findGroupJoin(table, join, conditions);
            }
            else
                findGroupJoins(right, whereConditions);
            findGroupJoins(join.getLeft(), whereConditions);
        }
    }

    protected void findGroupJoin(TableSource childTable, JoinNode join, 
                                 List<ConditionExpression> conditions) {
        Join groupJoin = childTable.getTable().getTable().getParentJoin();
        TableNode parentTable = childTable.getTable()
            .getTree().getNode(groupJoin.getParent());
        if (parentTable == null) return;
        ComparisonCondition found = null;
        for (ConditionExpression condition : conditions) {
            if (condition instanceof ComparisonCondition) {
                ComparisonCondition ccond = (ComparisonCondition)condition;
                ExpressionNode left = ccond.getLeft();
                ExpressionNode right = ccond.getRight();
                if (left.isColumn() && right.isColumn() &&
                    (((ColumnExpression)left).getTable() == childTable)) {
                    ColumnSource rightSource = (ColumnSource)right;
                    if (rightSource instanceof TableSource) {
                        TableSource rightTable = (TableSource)rightSource;
                        if (rightTable.getTable() == parentTable) {
                            if (found == null) {
                                found = ccond;
                            }
                            else {
                                // TODO: What we need is something
                                // earlier to decide that the primary
                                // keys are equated and so share the
                                // references somehow.
                                throw new UnsupportedSQLException("Found two possible parent joins", ccond.getSQLsource());
                            }
                        }
                    }
                }
            }
        }
        if (found == null) return;
        found.setImplementation(ConditionExpression.Implementation.GROUP_JOIN);
        join.setGroupJoin(groupJoin);
    }

}
