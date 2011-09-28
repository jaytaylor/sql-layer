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
    }

    protected void pickIndexes(TableJoins tableJoins, PlanNode plan) {
        // Goal is to get all the tables joined right here. Others in
        // the group may come via XxxLookup in a nested loop. Can only
        // consider indexes on the inner joined tables.
        IndexGoal goal = determineIndexGoal(tableJoins, plan, tableJoins.getTables());
        IndexScan index = null;
        if (goal != null) {
            Collection<TableSource> tables = new ArrayList<TableSource>();
            Set<TableSource> required = new HashSet<TableSource>();
            getRequiredTables(tableJoins.getJoins(), tables, required, true);
            index = goal.pickBestIndex(tables, required);
        }
        if (index != null) {
            goal.installUpstream(index);
        }
        PlanNode scan = index;
        if (scan == null)
            scan = new GroupScan(tableJoins.getGroup());
        tableJoins.setScan(scan);
    }

    protected void getRequiredTables(Joinable joinable,
                                     Collection<TableSource> tables,
                                     Set<TableSource> required,
                                     boolean allInner) {
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            getRequiredTables(join.getLeft(), tables, required,
                              allInner && (join.getJoinType() != JoinType.RIGHT_JOIN));
            getRequiredTables(join.getRight(), tables, required,
                              allInner && (join.getJoinType() != JoinType.LEFT_JOIN));
        }
        else if (joinable instanceof TableSource) {
            TableSource table = (TableSource)joinable;
            tables.add(table);
            if (allInner) required.add(table);
        }
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
            if (!grouping.hasGroupBy())
              grouping = null;
            input = input.getOutput();
            if (input instanceof Select)
                input = input.getOutput();
            if (input instanceof Sort) {
                // Needs to be possible to satisfy both.
                ordering = (Sort)input;
                if (grouping != null) {
                  List<ExpressionNode> groupBy = grouping.getGroupBy();
                  for (OrderByExpression orderBy : ordering.getOrderBy()) {
                    if (!groupBy.contains(orderBy.getExpression())) {
                      ordering = null;
                      break;
                    }
                  }
                }
            }
        }
        return new IndexGoal(plan, Collections.<TableSource>emptySet(),
                             conditions, grouping, ordering, tables);
    }

}
