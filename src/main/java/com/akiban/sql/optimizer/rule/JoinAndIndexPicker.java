/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

import com.akiban.sql.optimizer.rule.join_enum.*;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import com.akiban.server.error.UnsupportedSQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Pick joins and indexes. 
 * This the the core of actual query optimization.
 */
public class JoinAndIndexPicker extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(JoinAndIndexPicker.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext planContext) {
        BaseQuery query = (BaseQuery)planContext.getPlan();
        List<Picker> pickers = 
          new JoinsFinder(((SchemaRulesContext)planContext.getRulesContext())
                          .getCostEstimator()).find(query);
        for (Picker picker : pickers) {
            picker.apply();
        }
    }

    static class Picker {
        Map<BaseQuery,List<Picker>> subpickers;
        CostEstimator costEstimator;
        Joinable joinable;
        BaseQuery query;

        public Picker(Map<BaseQuery,List<Picker>> subpickers, 
                      CostEstimator costEstimator, 
                      Joinable joinable, BaseQuery query) {
            this.subpickers = subpickers;
            this.costEstimator = costEstimator;
            this.joinable = joinable;
            this.query = query;
        }

        public void apply() {
            QueryIndexGoal queryGoal = determineQueryIndexGoal(joinable);
        }

        protected QueryIndexGoal determineQueryIndexGoal(PlanNode input) {
            ConditionList whereConditions = null;
            Sort ordering = null;
            AggregateSource grouping = null;
            Project projectDistinct = null;
            input = input.getOutput();
            if (input instanceof Select) {
                ConditionList conds = ((Select)input).getConditions();
                if (!conds.isEmpty()) {
                    whereConditions = conds;
                }
            }
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
                            ExpressionNode orderByExpr = orderBy.getExpression();
                            if (!((orderByExpr.isColumn() &&
                                   (((ColumnExpression)orderByExpr).getTable() == grouping)) ||
                                  groupBy.contains(orderByExpr))) {
                                ordering = null;
                                break;
                            }
                        }
                    }
                }
            }
            else if (input instanceof Project) {
                Project project = (Project)input;
                input = project.getOutput();
                if (input instanceof Distinct)
                    projectDistinct = project;
                else if (input instanceof Sort)
                    ordering = (Sort)input;
            }
            return new QueryIndexGoal(query, whereConditions, 
                                      grouping, ordering, projectDistinct);
        }

    }

    // Purpose is twofold: 
    // Find top-level joins and note what query they come from; 
    // Annotate subqueries with their outer table references.
    static class JoinsFinder implements PlanVisitor, ExpressionVisitor {
        Map<BaseQuery,List<Picker>> entries;
        BaseQuery rootQuery;
        Deque<SubqueryState> subqueries = new ArrayDeque<SubqueryState>();
        CostEstimator costEstimator;

        public JoinsFinder(CostEstimator costEstimator) {
            this.costEstimator = costEstimator;
        }

        public List<Picker> find(BaseQuery query) {
            entries = new HashMap<BaseQuery,List<Picker>>();
            rootQuery = query;
            query.accept(this);
            return entries.get(query);
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
            if (!subqueries.isEmpty() &&
                (n instanceof ColumnSource)) {
                boolean added = subqueries.peek().tablesDefined.add((ColumnSource)n);
                assert added : "Table defined more than once";
            }
            if (n instanceof Joinable) {
                Joinable j = (Joinable)n;
                while (j.getOutput() instanceof Joinable)
                    j = (Joinable)j.getOutput();
                BaseQuery query = rootQuery;
                // Derived tables (SubquerySource) have their own entry.
                // Subquery expressions belong to the enclosing query.
                for (SubqueryState subquery : subqueries) {
                    if (subquery.subquery.getOutput() instanceof SubquerySource) {
                        query = subquery.subquery;
                        break;
                    }
                }
                List<Picker> entry = entries.get(query);
                if (entry == null) {
                    entry = new ArrayList<Picker>();
                    entries.put(query, entry);
                }
                for (Picker picker : entry) {
                    if (picker.joinable == j)
                        // Already have another set of joins to same root join.
                        return true;
                }
                Picker picker = new Picker(entries, costEstimator, j, query);
                entry.add(picker);
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
            if (!subqueries.isEmpty() &&
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
