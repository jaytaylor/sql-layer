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

import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ColumnSource;
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ConditionExpression;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.ExpressionVisitor;
import com.akiban.sql.optimizer.plan.JoinNode;
import com.akiban.sql.optimizer.plan.PlanContext;
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.PlanVisitor;
import com.akiban.sql.optimizer.plan.Select;
import com.akiban.sql.optimizer.plan.TableSource;
import com.akiban.sql.types.DataTypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class ColumnEquivalenceFinder extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(ColumnEquivalenceFinder.class);
    
    @Override
    protected Logger getLogger() {
        return logger;
    }
    
    private static class ColumnEquivalenceVisitor implements PlanVisitor, ExpressionVisitor {

        private Set<ColumnSource> innerSources = new HashSet<ColumnSource>();
        private boolean firstSelect = true;

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
            if (n instanceof JoinNode) {
                JoinNode joinNode = (JoinNode)n;
                if (joinNode.isInnerJoin()) {
                    if (joinNode.getLeft() instanceof ColumnSource) {
                        innerSources.add((ColumnSource)joinNode.getLeft());
                    }
                    if (joinNode.getRight() instanceof ColumnSource) {
                        innerSources.add((ColumnSource)joinNode.getRight());
                    }
                    equivalenceConditions(joinNode.getJoinConditions());
                }
            }
            else if (firstSelect && (n instanceof Select)) {
                Select select = (Select) n;
                equivalenceConditions(select.getConditions());
                firstSelect = false;
                
            }
            return true;
        }

        private void equivalenceConditions(List<ConditionExpression> conditions) {
            if (conditions != null) {
                for (ConditionExpression condition : conditions)
                    equivalenceCondition(condition);
            }
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

        private void equivalenceCondition(ConditionExpression condition) {
            if (condition instanceof ComparisonCondition) {
                ComparisonCondition comparison = (ComparisonCondition) condition;
                if (comparison.getOperation().equals(Comparison.EQ)
                        && (comparison.getLeft() instanceof ColumnExpression)
                        && (comparison.getRight() instanceof ColumnExpression)
                        ) {
                    ColumnExpression left = (ColumnExpression) comparison.getLeft();
                    ColumnExpression right = (ColumnExpression) comparison.getRight();
                    if (notNull(left) && notNull(right)) {
                        left.markEquivalentTo(right); // also implies right.equivalentTo(left)
                    }
                }
            }
        }

        private boolean notNull(ColumnExpression columnExpression) {
            if (!columnExpression.getSQLtype().isNullable())
                return true;
            if (innerSources.contains(columnExpression.getTable())) {
                DataTypeDescriptor notNullable = columnExpression.getSQLtype().getNullabilityType(false);
                columnExpression.setSQLtype(notNullable);
                return true;
            }
            return false;
        }
    }

    @Override
    public void apply(PlanContext plan) {
        plan.getPlan().accept(new ColumnEquivalenceVisitor());
    }
}
