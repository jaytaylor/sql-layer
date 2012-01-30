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
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.ExpressionVisitor;
import com.akiban.sql.optimizer.plan.PlanContext;
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.PlanVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ColumnEquivalenceFinder extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(ColumnEquivalenceFinder.class);
    
    @Override
    protected Logger getLogger() {
        return logger;
    }
    
    private static class ColumnEquivalenceVisitor implements PlanVisitor, ExpressionVisitor {

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
            if (n instanceof ComparisonCondition) {
                ComparisonCondition comparision = (ComparisonCondition) n;
                if (comparision.getOperation().equals(Comparison.EQ)
                        && (comparision.getLeft() instanceof ColumnExpression)
                        && (comparision.getRight() instanceof ColumnExpression)
                        ) {
                    ColumnExpression left = (ColumnExpression) comparision.getLeft();
                    ColumnExpression right = (ColumnExpression) comparision.getRight();
                    if (!(left.getSQLtype().isNullable() || right.getSQLtype().isNullable())) {
                        left.markEquivalentTo(right); // also implies right.equivalentTo(left)
                    }
                    return false; // already looked at this branch
                }
            }
            return true;
        }
    }

    @Override
    public void apply(PlanContext plan) {
        plan.getPlan().accept(new ColumnEquivalenceVisitor());
    }
}
