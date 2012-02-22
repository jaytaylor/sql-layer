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

package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.IntValueSource;
import com.akiban.server.types.util.ValueHolder;

import java.util.List;

public final class RankExpression extends CompareExpression {

    // AbstractTwoArgExpression interface
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(RANKING_DESCRIPTION);
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations(), op);
    }

    @Override
    protected boolean nullIsContaminating() {
        return false;
    }

    public RankExpression(Expression lhs, Expression rhs) {
        super(lhs, rhs);
    }

    // overriding protected methods

    @Override
    protected void buildToString(StringBuilder sb) {
        sb.append(left()).append(' ').append(RANKING_DESCRIPTION).append(' ').append(right());
    }

    // consts

    private static final String RANKING_DESCRIPTION = ">=<";

    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation {
        @Override
        public ValueSource eval() {
            int comparison;
            ValueSource left = scratch.copyFrom(left());
            ValueSource right = right();
            boolean leftNull = left.isNull();
            boolean rightNull = right.isNull();
            if (!leftNull && !rightNull) {
                comparison = op.compare(left, right);
            } else if (!leftNull) {
                comparison = -1;
            } else if (!rightNull) {
                comparison = 1;
            } else {
                comparison = 0;
            }
            return IntValueSource.of(comparison);
        }

        private InnerEvaluation(List<? extends ExpressionEvaluation> children, CompareOp op) {
            super(children);
            this.op = op;
            this.scratch = new ValueHolder();
        }

        private final CompareOp op;
        private final ValueHolder scratch;
    }
}
