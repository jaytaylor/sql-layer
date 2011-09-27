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
import static com.akiban.sql.optimizer.rule.GroupJoinFinder.*;

import com.akiban.qp.operator.API.JoinType;

import com.akiban.server.error.UnsupportedSQLException;

import java.util.*;

/** A goal for indexing: conditions on joined tables and ordering / grouping. */
public class IndexPicker extends BaseRule
{
    @Override
      public void apply(PlanContext planContext) {
        PlanNode plan = planContext.getPlan();
        List<Joinable> islands = new JoinIslandFinder().find(plan);
        // TODO: For now, very conservative about everything being simple.
        if (islands.isEmpty()) return;
        if (islands.size() > 1)
            throw new UnsupportedSQLException("Joins are too complex: " + islands, null);
        Joinable joins = islands.get(0);
        if (joins instanceof ExpressionsSource)
            return;             // Already set to scan.
        pickIndexes(joins, plan);
    }

    protected void pickIndexes(Joinable joins, PlanNode plan) {
        // Goal is to get all the tables joined right here. Others in
        // the group can come via XxxLookup in a nested loop. Can only
        // consider indexes on the inner joined tables.
        Collection<TableSource> tables = new ArrayList<TableSource>();
        Collection<TableSource> required = new ArrayList<TableSource>();
        TableGroup group = onlyTableGroup(null, true, joins, tables, required);
        if (group == null) return;
        IndexScan index = null;
        TableSource indexTable = null;
        IndexGoal goal = determineIndexGoal(joins, plan, tables);
        if (goal != null) {
            index = goal.pickBestIndex(required);
            if (index != null)
                indexTable = index.getLeafMostTable();
        }
        if (index != null) {
            goal.installUpstream(index, joins);
        }
        PlanNode scan = index;
        if (index == null) {
            scan = new GroupScan(group);
            scan = new Flatten(scan, trimJoins(joins, null));
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
            Joinable requiredJoins = trimJoins(joins, index.getRequiredTables());
            if (requiredJoins != null)
                scan = new Flatten(scan, requiredJoins);
        }
        // TODO: Can now prepone some of the conditions before the flatten.
        joins.getOutput().replaceInput(joins, scan);
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

    protected TableGroup onlyTableGroup(TableGroup group,
                                        boolean inRequired,
                                        Joinable joins,
                                        Collection<TableSource> tables,
                                        Collection<TableSource> required) {
        if (joins instanceof JoinNode) {
            JoinNode join = (JoinNode)joins;
            TableGroup gl = onlyTableGroup(group, 
                                           (inRequired && 
                                            (join.getJoinType() != JoinType.RIGHT_JOIN)),
                                           join.getLeft(),
                                           tables, required);
            TableGroup gr = onlyTableGroup(group, 
                                           (inRequired && 
                                            (join.getJoinType() != JoinType.LEFT_JOIN)),
                                           join.getRight(),
                                           tables, required);
            if (gl == gr) return gl;
        }
        else if (joins instanceof TableSource) {
            TableSource t = (TableSource)joins;
            TableGroup g = t.getGroup();
            if (g == null)
                throw new UnsupportedSQLException("Joins without group: " + joins, null);
            if (group == null)
                group = g;
            if (group == g) {
                tables.add(t);
                if (inRequired)
                    required.add(t);
                return g;
            }
        }
        throw new UnsupportedSQLException("Joins other than one group: " + joins, null);
    }

    protected IndexGoal determineIndexGoal(PlanNode input, 
                                           PlanNode plan,
                                           Collection<TableSource> tables) {
        List<ConditionExpression> conditions;
        Sort ordering = null;
        AggregateSource grouping = null;
        input = input.getOutput();
        if (input instanceof Select)
            conditions = ((Select)input).getConditions();
        else
            return null;
        input = input.getOutput();
        if (input instanceof Sort) {
            ordering = (Sort)input;
        }
        else if (input instanceof AggregateSource) {
            grouping = (AggregateSource)input;
            input = input.getOutput();
            if (input instanceof Select)
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
