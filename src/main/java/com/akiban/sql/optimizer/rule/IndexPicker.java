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

import com.akiban.qp.operator.API.JoinType;

import com.akiban.server.error.UnsupportedSQLException;

import java.util.*;

/** A goal for indexing: conditions on joined tables and ordering / grouping. */
public class IndexPicker extends BaseRule
{
    static class TableJoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<TableJoins> result = new ArrayList<TableJoins>();

        public List<TableJoins> find(PlanNode root) {
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
            if (n instanceof TableJoins) {
                result.add((TableJoins)n);
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
      public void apply(PlanContext planContext) {
        PlanNode plan = planContext.getPlan();
        List<TableJoins> groups = new TableJoinsFinder().find(plan);
        // TODO: For now, very conservative about everything being simple.
        if (groups.isEmpty()) return;
        if (groups.size() > 1)
            throw new UnsupportedSQLException("Joins are too complex: " + groups, null);
        TableJoins tableJoins = groups.get(0);
        pickIndexes(tableJoins, plan);
        joinBranches(tableJoins);
    }

    protected void pickIndexes(TableJoins tableJoins, PlanNode plan) {
        // Goal is to get all the tables joined right here. Others in
        // the group may come via XxxLookup in a nested loop. Can only
        // consider indexes on the inner joined tables.
        IndexGoal goal = determineIndexGoal(tableJoins, plan, tableJoins.getTables());
        IndexScan index = null;
        if (goal != null) {
            Collection<TableSource> required = new ArrayList<TableSource>();
            getRequiredTables(tableJoins.getJoins(), required);
            index = goal.pickBestIndex(required);
        }
        if (index != null) {
            goal.installUpstream(index);
        }
        PlanNode scan = index;
        if (scan == null)
            scan = new GroupScan(tableJoins.getGroup());
        tableJoins.setScan(scan);
    }

    // TODO: Put this into a separate rule.
    protected void joinBranches(TableJoins tableJoins) {
        PlanNode scan = tableJoins.getScan();
        IndexScan index = null;
        TableSource indexTable = null;
        if (scan instanceof IndexScan) {
            index = (IndexScan)scan;
            indexTable = index.getLeafMostTable();
        }
        if (index == null) {
            scan = new Flatten(scan, trimJoins(tableJoins.getJoins(), null));
        }
        else if (!index.isCovering()) {
            List<TableSource> ancestors = new ArrayList<TableSource>();
            TableSource branch = null;
            for (TableSource table : index.getRequiredTables()) {
                if (ancestorOf(table, indexTable)) {
                    ancestors.add(table);
                }
                else if (ancestorOf(indexTable, table)) {
                    if (branch == null)
                        branch = indexTable;
                }
                else if ((branch == null) ||
                         ancestorOf(table, branch)) {
                    branch = table;
                }
                else if (!ancestorOf(branch, table)) {
                    throw new UnsupportedSQLException("Sibling branches: " + table + 
                                                      " and " + branch,
                                                      null);
                }
            }
            if (branch == indexTable) {
                ancestors.remove(indexTable);
                scan = new BranchLookup(scan, indexTable, branch);
                branch = null;
            }
            if (!ancestors.isEmpty()) {
                // Access in stable order.
                Collections.sort(ancestors,
                                 new Comparator<TableSource>() {
                                     public int compare(TableSource t1, TableSource t2) {
                                         return t1.getTable().getTable().getTableId().compareTo(t2.getTable().getTable().getTableId());
                                     }
                                 });
                scan = new AncestorLookup(scan, indexTable, ancestors);
            }
            if (branch != null) {
                scan = new BranchLookup(scan, 
                                        (ancestors.isEmpty() ? 
                                         indexTable : 
                                         ancestors.get(0)), 
                                        branch);
            }
            Joinable requiredJoins = trimJoins(tableJoins.getJoins(), 
                                               index.getRequiredTables());
            if (requiredJoins != null)
                scan = new Flatten(scan, requiredJoins);
        }
        // TODO: Can now prepone some of the conditions before the flatten.
        // TODO: Better to keep the tableJoins and just replace the
        // inside? Do we need its state any more?
        tableJoins.getOutput().replaceInput(tableJoins, scan);
    }
    
    protected Joinable trimJoins(Joinable joinable, Set<TableSource> requiredTables) {
        if (joinable instanceof TableSource) {
            if ((requiredTables == null) || requiredTables.contains(joinable))
                return joinable;
            else
                return null;
        }
        else if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            removeGroupJoin(join);
            Joinable oleft = join.getLeft();
            Joinable oright = join.getRight();
            Joinable nleft = trimJoins(oleft, requiredTables);
            Joinable nright = trimJoins(oright, requiredTables);
            if ((oleft == nleft) && (oright == nright))
                return joinable;
            else if (nleft == null)
                return nright;
            else if (nright == null)
                return nleft;
            else {
                join.setLeft(nleft);
                join.setRight(nright);
                return join;
            }
        }
        else
            return joinable;
    }

    // We only want joins for their flattening pattern.
    // Make explain output more obvious by removing group join traces.
    // TODO: Also rejecting non-group joins; those could be supported with Select.
    protected void removeGroupJoin(JoinNode join) {
        List<ConditionExpression> conditions = join.getJoinConditions();
        int i = 0;
        while (i < conditions.size()) {
            ConditionExpression cond = conditions.get(i);
            if (cond.getImplementation() == 
                ConditionExpression.Implementation.GROUP_JOIN) {
                conditions.remove(i);
            }
            else {
                i++;
            }
        }
        if (!conditions.isEmpty())
            throw new UnsupportedSQLException("Non group join",
                                              conditions.get(0).getSQLsource());
        join.setGroupJoin(null);
    }

    /** Is <code>t1</code> an ancestor of <code>t2</code>? */
    protected boolean ancestorOf(TableSource t1, TableSource t2) {
        do {
            if (t1 == t2) return true;
            t2 = t2.getParentTable();
        } while (t2 != null);
        return false;
    }

    protected void getRequiredTables(Joinable joinable,
                                     Collection<TableSource> required) {
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            if (join.getJoinType() != JoinType.RIGHT_JOIN)
                getRequiredTables(join.getLeft(), required);
            if (join.getJoinType() != JoinType.LEFT_JOIN)
                getRequiredTables(join.getRight(), required);
        }
        else if (joinable instanceof TableSource) {
            required.add((TableSource)joinable);
        }
    }

    protected IndexGoal determineIndexGoal(PlanNode input, 
                                           PlanNode plan,
                                           Collection<TableSource> tables) {
        List<ConditionExpression> conditions;
        Sort ordering = null;
        AggregateSource grouping = null;
        input = input.getOutput();
        if (input instanceof Filter)
            conditions = ((Filter)input).getConditions();
        else
            return null;
        input = input.getOutput();
        if (input instanceof Sort) {
            ordering = (Sort)input;
        }
        else if (input instanceof AggregateSource) {
            grouping = (AggregateSource)input;
            input = input.getOutput();
            if (input instanceof Filter)
                input = input.getOutput();
            if (input instanceof Sort) {
                // Needs to be possible to satisfy both.
                ordering = (Sort)input;
                List<ExpressionNode> groupBy = grouping.getGroupBy();
                for (OrderByExpression orderBy : ordering.getOrderBy()) {
                    if (!groupBy.contains(orderBy.getExpression())) {
                        ordering = null;
                        break;
                    }
                }
            }
        }
        return new IndexGoal(plan, Collections.<TableSource>emptySet(),
                             conditions, grouping, ordering, tables);
    }

}
