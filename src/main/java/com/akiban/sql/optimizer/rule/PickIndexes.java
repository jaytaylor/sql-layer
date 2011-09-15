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

import com.akiban.server.error.UnsupportedSQLException;

import java.util.*;

/** A goal for indexing: conditions on joined tables and ordering / grouping. */
public class PickIndexes extends BaseRule
{
    @Override
    public PlanNode apply(PlanNode plan) {
        List<Joinable> islands = new FindGroupJoins.JoinIslandFinder().find(plan);
        // TODO: For now, very conservative about everything being simple.
        if (islands.isEmpty()) return plan;
        if (islands.size() > 1)
            throw new UnsupportedSQLException("Joins are too complex: " + islands, null);
        Joinable joins = islands.get(0);
        pickIndexes(joins);
        return plan;
    }

    protected void pickIndexes(Joinable joins) {
        TableGroup group = onlyTableGroup(joins);
        Collection<TableSource> tables, required;
        tables = group.getTables();
        required = new ArrayList<TableSource>();
        for (TableSource table : tables)
            if (table.isRequired())
                required.add(table);
        IndexUsage index = null;
        TableSource indexTable = null;
        IndexGoal goal = determineIndexGoal(joins);
        if (goal != null) {
            index = goal.pickBestIndex(required);
            if (index != null)
                indexTable = index.getLeafMostTable();
        }
        TableAccessPath groupScan = null, ancestorLookup = null, branchLookup = null;
        for (TableSource table : tables) {
            TableAccessPath accessPath;
            if (indexTable == null) {
                if (groupScan == null)
                    groupScan = new GroupScan(group);
                accessPath = groupScan;
            }
            else if (table == indexTable) {
                accessPath = index;
            }
            else if (ancestorOf(table, indexTable)) {
                if (ancestorLookup == null)
                    ancestorLookup = new AncestorLookup(indexTable);
                accessPath = ancestorLookup;
            }
            // TODO: Need to find separate subbranches, not one for all.
            else {
                if (branchLookup == null)
                    branchLookup = new BranchLookup(indexTable);
                accessPath = branchLookup;
            }
            table.setAccessPath(accessPath);
        }
    }
    
    protected boolean ancestorOf(TableSource t1, TableSource t2) {
        do {
            if (t1 == t2) return true;
            t2 = t2.getParentTable();
        } while (t2 != null);
        return false;
    }

    protected TableGroup onlyTableGroup(Joinable joins) {
        if (joins instanceof JoinNode) {
            JoinNode join = (JoinNode)joins;
            TableGroup gl = onlyTableGroup(join.getLeft());
            TableGroup gr = onlyTableGroup(join.getRight());
            if (gl == gr) return gl;
        }
        else if (joins instanceof TableSource) {
            TableGroup g = ((TableSource)joins).getGroup();
            if (g == null)
                throw new UnsupportedSQLException("Joins without group: " + joins, null);
            return g;
        }
        throw new UnsupportedSQLException("Joins other than one group: " + joins, null);
    }

    protected IndexGoal determineIndexGoal(PlanNode input) {
        List<ConditionExpression> conditions;
        List<ExpressionNode> grouping = null;
        List<OrderByExpression> ordering = null;
        input = input.getOutput();
        if (input instanceof Filter)
            conditions = ((Filter)input).getConditions();
        else
            return null;
        input = input.getOutput();
        if (input instanceof Sort) {
            ordering = ((Sort)input).getOrderBy();
        }
        else if (input instanceof AggregateSource) {
            grouping = ((AggregateSource)input).getGroupBy();
            input = input.getOutput();
            if (input instanceof Filter)
                input = input.getOutput();
            if (input instanceof Sort) {
                // Needs to be possible to satisfy both.
                ordering = ((Sort)input).getOrderBy();
                for (OrderByExpression orderBy : ordering) {
                    if (!grouping.contains(orderBy.getExpression())) {
                        ordering = null;
                        break;
                    }
                }
            }
        }
        return new IndexGoal(Collections.<TableSource>emptySet(),
                             conditions, grouping, ordering);
    }

}
