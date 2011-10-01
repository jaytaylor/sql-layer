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
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import com.akiban.server.error.UnsupportedSQLException;

import java.util.*;

/** A goal for indexing: conditions on joined tables and ordering / grouping. */
public class IndexPicker extends BaseRule
{
    @Override
    public void apply(PlanContext planContext) {
        BaseQuery query = (BaseQuery)planContext.getPlan();
        List<Picker> pickers = new JoinsFinder().find(query);
        for (Picker picker : pickers)
            picker.pickIndexes();
    }

    static class Picker {
        Joinable joinable;
        BaseQuery query;
        Set<ColumnSource> boundTables;

        public Picker(Joinable joinable) {
            this.joinable = joinable;
        }

        public void pickIndexes() {
            // Start with tables outside this subquery. Then add as we
            // set up each nested loop join.
            boundTables = new HashSet<ColumnSource>(query.getOuterTables());

            // Goal is to get all the tables joined right here. Others
            // in the group may come via XxxLookup in a nested
            // loop. Can only consider table indexes on the inner
            // joined tables and group indexes corresponding to outer
            // joins.
            TableJoins tableJoins = (TableJoins)joinable;
            IndexGoal goal = determineIndexGoal(tableJoins, tableJoins.getTables());
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
                                  allInner && (join.getJoinType() != JoinType.RIGHT));
                getRequiredTables(join.getRight(), tables, required,
                                  allInner && (join.getJoinType() != JoinType.LEFT));
            }
            else if (joinable instanceof TableSource) {
                TableSource table = (TableSource)joinable;
                tables.add(table);
                if (allInner) required.add(table);
            }
        }

        protected IndexGoal determineIndexGoal(PlanNode input, 
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
            return new IndexGoal(query, boundTables, conditions, grouping, ordering, tables);
        }
    }

    // Purpose is twofold: 
    // Find top-level joins and note what query they come from; 
    // Annotate subqueries with their outer table references.
    static class JoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<Picker> result = new ArrayList<Picker>();
        Stack<SubqueryState> subqueries = new Stack<SubqueryState>();

        public List<Picker> find(BaseQuery query) {
            query.accept(this);
            for (Picker entry : result)
                if (entry.query == null)
                    entry.query = query;
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (n instanceof Subquery) {
                subqueries.push(new SubqueryState((Subquery)n));
                return true;
            }
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (n instanceof Subquery) {
                SubqueryState s = subqueries.pop();
                s.subquery.setOuterTables(s.getTablesReferencedButNotDefined());
            }
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof Joinable) {
                assert !(n instanceof TableSource); // These should be wrapped already.
                Joinable j = (Joinable)n;
                while (j.getOutput() instanceof Joinable)
                    j = (Joinable)j.getOutput();
                for (Picker entry : result) {
                    if (entry.joinable == j)
                        // Already have another set of joins to same root join.
                        return true;
                }
                Picker entry = new Picker(j);
                if (!subqueries.empty()) {
                    entry.query = subqueries.peek().subquery;
                }
                result.add(entry);
            }
            if (!subqueries.empty() &&
                (n instanceof ColumnSource)) {
                assert subqueries.peek().tablesDefined.add((ColumnSource)n) :
                    "Table defined more than once";
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
            if (!subqueries.empty() &&
                (n instanceof ColumnExpression)) {
                subqueries.peek().tablesReferenced.add(((ColumnExpression)n).getTable());
            }
            return true;
        }
    }

    static class SubqueryState {
        Subquery subquery;
        Set<ColumnSource> tablesReferenced = new HashSet<ColumnSource>();
        Set<ColumnSource> tablesDefined = new HashSet<ColumnSource>();

        public SubqueryState(Subquery subquery) {
            this.subquery = subquery;
        }

        public Set<ColumnSource> getTablesReferencedButNotDefined() {
            tablesReferenced.removeAll(tablesDefined);
            return tablesReferenced;
        }
    }

}
