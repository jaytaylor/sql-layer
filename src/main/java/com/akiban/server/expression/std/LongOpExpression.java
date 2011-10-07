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
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.util.AbstractLongValueSource;
import com.akiban.util.ArgumentValidation;

import java.util.List;

final class LongOpExpression extends AbstractBinaryExpression {
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(longOp);
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(longOp, childrenEvaluations());
    }

    public LongOpExpression(Expression lhs, LongOp longOp, Expression rhs) {
        super(longOp.opType(), lhs, rhs);
        this.longOp = longOp;
    }

    private final LongOp longOp;

    // nested classes

    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation {

        @Override
        public ValueSource eval() {
            valueSource.operands(left(), right());
            return valueSource;
        }

        private InnerEvaluation(LongOp op, List<? extends ExpressionEvaluation> children) {
            super(children);
            this.valueSource = new InnerValueSource(op);
        }

        private final InnerValueSource valueSource;
    }

    private static class InnerValueSource extends AbstractLongValueSource {

        // LongOpValueSource interface
        public void operands(ValueSource left, ValueSource right) {
            this.left = left;
            this.right = right;
        }

        // AbstractLongValueSource interface

        @Override
        protected long rawLong() {
            if (left == null || right == null) {
                throw new IllegalStateException("left or right operand not set");
            }
            AkType opType = op.opType();
            LongExtractor extractor = Extractors.getLongExtractor(opType);
            long leftLong = extractor.getLong(left);
            long rightLong = extractor.getLong(right);
            return op.evaluate(leftLong, rightLong);
        }

        @Override
        public boolean isNull() {
            return left.isNull() || right.isNull();
        }

        @Override
        public AkType getConversionType() {
            return op.opType();
        }

        public InnerValueSource(LongOp op) {
            ArgumentValidation.notNull("operator", op);
            this.op = op;
        }

        private final LongOp op;
        private ValueSource left;
        private ValueSource right;
    }
}
