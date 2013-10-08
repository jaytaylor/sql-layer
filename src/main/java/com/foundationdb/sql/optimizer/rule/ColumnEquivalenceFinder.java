/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ComparisonCondition;
import com.foundationdb.sql.optimizer.plan.ConditionExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.ExpressionVisitor;
import com.foundationdb.sql.optimizer.plan.JoinNode;
import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;
import com.foundationdb.sql.optimizer.plan.Select;
import com.foundationdb.sql.types.DataTypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class ColumnEquivalenceFinder extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(ColumnEquivalenceFinder.class);
    
    @Override
    protected Logger getLogger() {
        return logger;
    }
    
    private static class ColumnEquivalenceVisitor implements PlanVisitor, ExpressionVisitor {

        private ColumnEquivalenceStack equivs = new ColumnEquivalenceStack();

        @Override
        public boolean visitEnter(PlanNode n) {
            equivs.enterNode(n);
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            equivs.leaveNode(n);
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof JoinNode) {
                JoinNode joinNode = (JoinNode)n;
                if (joinNode.isInnerJoin())
                    equivalenceConditions(joinNode.getJoinConditions());
            }
            else if (n instanceof Select) {
                Select select = (Select) n;
                equivalenceConditions(select.getConditions());
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
                    if (!left.equals(right)) {
                        markNotNull(left);
                        markNotNull(right);
                        equivs.get().markEquivalent(left, right); // also implies right.equivalentTo(left)
                    }
                }
            }
        }
    }

    private static void markNotNull(ColumnExpression columnExpression) {
        DataTypeDescriptor sqLtype = columnExpression.getSQLtype();
        if (sqLtype.isNullable()) {
            DataTypeDescriptor notNullable = sqLtype.getNullabilityType(false);
            columnExpression.setSQLtype(notNullable);
        }
    }

    @Override
    public void apply(PlanContext plan) {
        plan.getPlan().accept(new ColumnEquivalenceVisitor());
    }
}
