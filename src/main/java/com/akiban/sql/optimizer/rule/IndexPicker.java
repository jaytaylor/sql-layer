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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** A goal for indexing: conditions on joined tables and ordering / grouping. */
public class IndexPicker extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(IndexPicker.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

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

            pickIndexes(joinable);
        }

        protected void pickIndexes(Joinable joinable) {
            if (joinable instanceof TableJoins) {
                pickIndex((TableJoins)joinable);
                return;
            }
            if (joinable instanceof JoinNode) {
                pickIndexes((JoinNode)joinable);
                return;
            }

            assert !(joinable instanceof TableSource);
            if (joinable instanceof ColumnSource) {
                // Subqueries, VALUES, etc. now available.
                boundTables.add((ColumnSource)joinable);
            }
        }

        protected void pickIndex(TableJoins tableJoins) {
            if (tableJoins.getScan() != null) 
                // This can happen if a subquery source gets replaced
                // by ordinary joins that then get dealt with.
                return;
            // Goal is to get all the tables joined right here. Others
            // in the group may come via XxxLookup in a nested
            // loop. Can only consider table indexes on the inner
            // joined tables and group indexes corresponding to outer
            // joins.
            IndexGoal goal = determineIndexGoal(tableJoins);
            IndexScan index = pickBestIndex(tableJoins, goal);
            pickedIndex(tableJoins, goal, index);
        }

        protected IndexGoal determineIndexGoal(TableJoins tableJoins) {
            return determineIndexGoal(tableJoins, tableJoins.getTables());
        }

        protected void pickedIndex(TableJoins tableJoins, 
                                   IndexGoal goal, IndexScan index) {
            if (index != null) {
                goal.installUpstream(index);
            }
            PlanNode scan = index;
            if (scan == null)
                scan = new GroupScan(tableJoins.getGroup());
            tableJoins.setScan(scan);
            boundTables.addAll(tableJoins.getTables());
        }

        protected IndexGoal determineIndexGoal(PlanNode input, 
                                               Collection<TableSource> tables) {
            List<ConditionList> conditionSources = new ArrayList<ConditionList>();
            Sort ordering = null;
            AggregateSource grouping = null;
            Project projectDistinct = null;
            while (true) {
                input = input.getOutput();
                if (!(input instanceof Joinable))
                    break;
                if (input instanceof JoinNode) {
                    ConditionList conds = ((JoinNode)input).getJoinConditions();
                    if ((conds != null) && !conds.isEmpty())
                        conditionSources.add(conds);
                }
            }
            if (input instanceof Select) {
                ConditionList conds = ((Select)input).getConditions();
                if (!conds.isEmpty()) {
                    conditionSources.add(conds);
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
            if (conditionSources.isEmpty() &&
                (ordering == null) &&
                (grouping == null) &&
                (projectDistinct == null))
                return null;
            return new IndexGoal(query, boundTables, 
                                 conditionSources, grouping, ordering, projectDistinct,
                                 tables);
        }

        protected IndexScan pickBestIndex(TableJoins tableJoins, IndexGoal goal) {
            if (goal == null)
                return null;
            Collection<TableSource> tables = new ArrayList<TableSource>();
            Set<TableSource> required = new HashSet<TableSource>();
            getRequiredTables(tableJoins.getJoins(), tables, required, true);
            return goal.pickBestIndex(tables, required);
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

        protected void pickIndexes(JoinNode join) {
            join.setImplementation(JoinNode.Implementation.NESTED_LOOPS);
            
            Joinable left = join.getLeft();
            Joinable right = join.getRight();

            boolean tryReverse = false, twoTablesInner = false;
            if (join.getReverseHook() == null) {
                switch (join.getJoinType()) {
                case INNER:
                    twoTablesInner = ((left instanceof TableJoins) && 
                                      (right instanceof TableJoins));
                    tryReverse = twoTablesInner;
                    break;
                case RIGHT:
                    join.reverse();
                }
            }
            else {
                tryReverse = join.getReverseHook().canReverse(join);
                if (!tryReverse) {
                    join.getReverseHook().didNotReverse(join);
                    left = join.getLeft();
                    right = join.getRight();
                }
            }

            if (tryReverse) {
                if (twoTablesInner) {
                    if (pickIndexesTablesInner(join))
                        return;
                }
                if (right instanceof ColumnSource) {
                    if (pickIndexesTableValues(join))
                        return;
                    left = join.getLeft();
                    right = join.getRight();
                }
            }

            // Default is just to do each in the given order.
            pickIndexes(left);
            pickIndexes(right);
        }

        // Pick indexes for two tables. See which one gets a better
        // initial index.
        // TODO: Better to find all the INNER joins beneath this and
        // do them all at once, not a pair at a time.
        protected boolean pickIndexesTablesInner(JoinNode join) {
            TableJoins left = (TableJoins)join.getLeft();
            TableJoins right = (TableJoins)join.getRight();
            IndexGoal lgoal = determineIndexGoal(left);
            IndexScan lindex = pickBestIndex(left, lgoal);
            IndexGoal rgoal = determineIndexGoal(right);
            IndexScan rindex = pickBestIndex(right, rgoal);
            boolean rightFirst = false;
            if (lindex == null)
                rightFirst = (rindex != null);
            else if (rindex != null)
                rightFirst = (lgoal.compare(lindex, rindex) < 0);
            if (rightFirst) {
                TableJoins temp = left;
                left = right;
                right = temp;

                join.reverse();
                lgoal = rgoal;
                lindex = rindex;
            }
            else if (join.getReverseHook() != null) {
                join.getReverseHook().didNotReverse(join);
            }
            // Commit to the left choice and redo the right with it bound.
            pickedIndex(left, lgoal, lindex);
            pickIndexes(right);
            return true;
        }

        // Pick indexes for table and VALUES (or generally a non-table ColumnSource).
        // Put the VALUES outside if the join condition ends up indexed.
        protected boolean pickIndexesTableValues(JoinNode join) {
            TableJoins left = leftOfValues(join);
            ColumnSource right = (ColumnSource)join.getRight();

            boundTables.add(right);
            IndexGoal lgoal = determineIndexGoal(left);
            IndexScan lindex = pickBestIndex(left, lgoal);
            boundTables.remove(right);
            
            if ((lindex == null) || !lindex.hasConditions())
                return false;

            boolean found = false;
            for (ConditionExpression joinCondition : join.getJoinConditions()) {
                if (lindex.getConditions().contains(joinCondition)) {
                    found = true;
                }
            }
            if (!found) {
                if (join.getReverseHook() != null) {
                    join.getReverseHook().didNotReverse(join);
                }
                return false;
            }
            
            // Put the VALUES outside and commit to that in the simple case.
            join.reverse();
            if (left != join.getRight())
                return false;
            pickedIndex(left, lgoal, lindex);
            return true;
        }

        // Get the table joins that seems to be most interestingly
        // joined with a VALUES.
        // TODO: Not very general.
        protected TableJoins leftOfValues(JoinNode join) {
            Joinable left = join.getLeft();
            if (left instanceof TableJoins)
                return (TableJoins)left;
            Collection<TableSource> tables = new ArrayList<TableSource>();
            for (ConditionExpression joinCondition : join.getJoinConditions()) {
                if (joinCondition instanceof ComparisonCondition) {
                    ExpressionNode lop = ((ComparisonCondition)joinCondition).getLeft();
                    if (lop.isColumn()) {
                        ColumnSource table = ((ColumnExpression)lop).getTable();
                        if (table instanceof TableSource)
                            tables.add((TableSource)table);
                    }
                }
            }
            if (tables.isEmpty())
                return null;
            return findTableSource(left, tables);
        }
        
        protected TableJoins findTableSource(Joinable joinable,
                                             Collection<TableSource> tables) {
            if (joinable instanceof TableJoins) {
                TableJoins tjoins = (TableJoins)joinable;
                for (TableSource table : tables) {
                    if (tjoins.getTables().contains(table))
                        return tjoins;
                }
            }
            else if (joinable instanceof JoinNode) {
                JoinNode join = (JoinNode)joinable;
                TableJoins result = findTableSource(join.getLeft(), tables);
                if (result != null) return result;
                return findTableSource(join.getRight(), tables);
            }
            return null;
        }
    }

    // Purpose is twofold: 
    // Find top-level joins and note what query they come from; 
    // Annotate subqueries with their outer table references.
    static class JoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<Picker> result = new ArrayList<Picker>();
        Deque<SubqueryState> subqueries = new ArrayDeque<SubqueryState>();

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
                Joinable j = (Joinable)n;
                while (j.getOutput() instanceof Joinable)
                    j = (Joinable)j.getOutput();
                for (Picker entry : result) {
                    if (entry.joinable == j)
                        // Already have another set of joins to same root join.
                        return true;
                }
                Picker entry = new Picker(j);
                if (!subqueries.isEmpty()) {
                    entry.query = subqueries.peek().subquery;
                }
                result.add(entry);
            }
            if (!subqueries.isEmpty() &&
                (n instanceof ColumnSource)) {
                boolean added = subqueries.peek().tablesDefined.add((ColumnSource)n);
                assert added : "Table defined more than once";
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
