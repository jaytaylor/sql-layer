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

package com.akiban.sql.optimizer.rule.join_enum;

import com.akiban.sql.optimizer.rule.join_enum.DPhyp.JoinOperator;
import com.akiban.sql.optimizer.rule.range.ColumnRanges;
import com.akiban.sql.optimizer.rule.range.RangeSegment;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.IndexScan.OrderEffectiveness;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import java.util.*;

/** The goal of a indexes within a group. */
public class GroupIndexGoal
{
    // The overall goal.
    private QueryIndexGoal queryGoal;
    
    // All the conditions that might be indexable.
    private List<ConditionExpression> conditions;
    // Where they came from.
    private List<ConditionList> conditionSources;

    // All the columns besides those in conditions that will be needed.
    private RequiredColumns requiredColumns;

    // Tables already bound outside.
    private Set<ColumnSource> boundTables;

    // Mapping of Range-expressible conditions, by their column. lazy loaded.
    private Map<ColumnExpression,ColumnRanges> columnsToRanges;

    public GroupIndexGoal(QueryIndexGoal queryGoal,
                          Iterable<TableSource> tables) {
        this.queryGoal = queryGoal;

        conditionSources = new ArrayList<ConditionList>();
        if (queryGoal.getWhereConditions() != null) {
            conditionSources.add(queryGoal.getWhereConditions());
            conditions = queryGoal.getWhereConditions();
        }
        else {
            conditions = Collections.emptyList();
        }

        requiredColumns = new RequiredColumns(tables);

        boundTables = queryGoal.getQuery().getOuterTables();
    }

    public void setJoinConditions(Collection<JoinOperator> joins) {
        conditionSources.clear();
        if (queryGoal.getWhereConditions() != null)
            conditionSources.add(queryGoal.getWhereConditions());
        if (!joins.isEmpty()) {
            conditions = new ArrayList<ConditionExpression>();
            for (JoinOperator join : joins) {
                ConditionList joinConditions = join.getJoinConditions();
                if (joinConditions != null) {
                    conditions.addAll(joinConditions);
                    if (join.getJoin() != null) {
                        conditionSources.add(joinConditions);
                    }
                }
            }
        }
        else {
            conditions = queryGoal.getWhereConditions();
            if (conditions == null)
                conditions = Collections.emptyList();
        }
    }

    public void updateRequiredColumns() {
        requiredColumns.clear();
        Collection<PlanNode> orderings = (queryGoal.getOrdering() == null) ? 
            Collections.<PlanNode>emptyList() : 
            Collections.<PlanNode>singletonList(queryGoal.getOrdering());
        queryGoal.getQuery().accept(new RequiredColumnsFiller(requiredColumns, 
                                                              orderings, conditions));
    }

    public void setBoundTables(Set<ColumnSource> boundTables) {
        this.boundTables = boundTables;
    }
    
    // Get Range-expressible conditions for given column.
    protected ColumnRanges rangeForIndex(ExpressionNode expressionNode) {
        if (expressionNode instanceof ColumnExpression) {
            if (columnsToRanges == null) {
                columnsToRanges = new HashMap<ColumnExpression, ColumnRanges>();
                for (ConditionExpression condition : conditions) {
                    ColumnRanges range = ColumnRanges.rangeAtNode(condition);
                    if (range != null) {
                        ColumnExpression rangeColumn = range.getColumnExpression();
                        ColumnRanges oldRange = columnsToRanges.get(rangeColumn);
                        if (oldRange != null)
                            range = ColumnRanges.andRanges(range, oldRange);
                        columnsToRanges.put(rangeColumn, range);
                    }
                }
            }
            ColumnExpression columnExpression = (ColumnExpression)expressionNode;
            return columnsToRanges.get(columnExpression);
        }
        return null;
    }
    
    static class RequiredColumns {
        private Map<TableSource,Set<ColumnExpression>> map;
        
        public RequiredColumns(Iterable<TableSource> tables) {
            map = new HashMap<TableSource,Set<ColumnExpression>>();
            for (TableSource table : tables) {
                map.put(table, new HashSet<ColumnExpression>());
            }
        }

        public RequiredColumns(RequiredColumns other) {
            map = new HashMap<TableSource,Set<ColumnExpression>>(other.map.size());
            for (Map.Entry<TableSource,Set<ColumnExpression>> entry : other.map.entrySet()) {
                map.put(entry.getKey(), new HashSet<ColumnExpression>(entry.getValue()));
            }
        }

        public Set<TableSource> getTables() {
            return map.keySet();
        }
        
        public boolean hasColumns(TableSource table) {
            Set<ColumnExpression> entry = map.get(table);
            if (entry == null) return false;
            return !entry.isEmpty();
        }

        public boolean isEmpty() {
            boolean empty = true;
            for (Set<ColumnExpression> entry : map.values())
                if (!entry.isEmpty())
                    return false;
            return empty;
        }

        public void require(ColumnExpression expr) {
            Set<ColumnExpression> entry = map.get(expr.getTable());
            if (entry != null)
                entry.add(expr);
        }

        /** Opposite of {@link require}: note that we have a source for this column. */
        public void have(ColumnExpression expr) {
            Set<ColumnExpression> entry = map.get(expr.getTable());
            if (entry != null)
                entry.remove(expr);
        }

        public void clear() {
            for (Set<ColumnExpression> entry : map.values())
                entry.clear();
        }
    }

    static class RequiredColumnsFiller implements PlanVisitor, ExpressionVisitor {
        private RequiredColumns requiredColumns;
        private Map<PlanNode,Void> excludedPlanNodes, includedPlanNodes;
        private Map<ExpressionNode,Void> excludedExpressions;
        private Deque<Boolean> excludeNodeStack = new ArrayDeque<Boolean>();
        private boolean excludeNode = false;
        private int excludeDepth = 0;
        private int subqueryDepth = 0;

        public RequiredColumnsFiller(RequiredColumns requiredColumns) {
            this.requiredColumns = requiredColumns;
        }

        public RequiredColumnsFiller(RequiredColumns requiredColumns,
                                     Collection<PlanNode> excludedPlanNodes,
                                     Collection<ConditionExpression> excludedExpressions) {
            this.requiredColumns = requiredColumns;
            this.excludedPlanNodes = new IdentityHashMap<PlanNode,Void>();
            for (PlanNode planNode : excludedPlanNodes)
                this.excludedPlanNodes.put(planNode, null);
            this.excludedExpressions = new IdentityHashMap<ExpressionNode,Void>();
            for (ConditionExpression condition : excludedExpressions)
                this.excludedExpressions.put(condition, null);
        }

        public void setIncludedPlanNodes(Collection<PlanNode> includedPlanNodes) {
            this.includedPlanNodes = new IdentityHashMap<PlanNode,Void>();
            for (PlanNode planNode : includedPlanNodes)
                this.includedPlanNodes.put(planNode, null);
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            // Input nodes are called within the context of their output.
            // We want to know whether just this node is excluded, not
            // it and all its inputs.
            excludeNodeStack.push(excludeNode);
            excludeNode = exclude(n);
            if ((n instanceof Subquery) &&
                !((Subquery)n).getOuterTables().isEmpty())
                // TODO: Might be accessing tables from outer query as
                // group joins, which we don't support currently. Make
                // sure those aren't excluded.
                subqueryDepth++;
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            excludeNode = excludeNodeStack.pop();
            if ((n instanceof Subquery) &&
                !((Subquery)n).getOuterTables().isEmpty())
                subqueryDepth--;
            return true;
        }
        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            if (!excludeNode && exclude(n))
                excludeDepth++;
            return visit(n);
        }
        @Override
        public boolean visitLeave(ExpressionNode n) {
            if (!excludeNode && exclude(n))
                excludeDepth--;
            return true;
        }
        @Override
        public boolean visit(ExpressionNode n) {
            if (!excludeNode && (excludeDepth == 0)) {
                if (n instanceof ColumnExpression)
                    requiredColumns.require((ColumnExpression)n);
            }
            return true;
        }

        // Should this plan node be excluded from the requirement?
        protected boolean exclude(PlanNode node) {
            if (includedPlanNodes != null)
                return !includedPlanNodes.containsKey(node);
            else if (excludedPlanNodes != null)
                return excludedPlanNodes.containsKey(node);
            else
                return false;
        }
        
        // Should this expression be excluded from requirement?
        protected boolean exclude(ExpressionNode expr) {
            return (((excludedExpressions != null) &&
                     excludedExpressions.containsKey(expr)) ||
                    // Group join conditions are handled specially.
                    ((expr instanceof ConditionExpression) &&
                     (((ConditionExpression)expr).getImplementation() ==
                      ConditionExpression.Implementation.GROUP_JOIN) &&
                     // Include expressions in subqueries until do joins across them.
                     (subqueryDepth == 0)));
        }
    }

}
