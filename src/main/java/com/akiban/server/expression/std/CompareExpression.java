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
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.util.ArgumentValidation;

import java.util.List;

public final class CompareExpression extends AbstractTwoArgExpression {

    // AbstractTwoArgExpression interface
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(comparison);
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations(), comparison, op);
    }

    public CompareExpression(List<? extends Expression> children, Comparison comparison, CompareOp op) {
        super(op.opType(), children);
        this.comparison = comparison;
        this.op = op;
        if (left().valueType() != op.opType() || right().valueType() != op.opType()) {
            throw new IllegalArgumentException(
                    String.format("incompatible types: (%s, %s) for comparison of type %s",
                            left().valueType(),
                            right().valueType(),
                            op.opType()
            ));
        }
    }

    // object state

    private final Comparison comparison;
    private final CompareOp op;

    // consts

    private final CompareOp LONG_COMPARE = new LongCompareOp(AkType.LONG);

    private final CompareOp DOUBLE_COMPARE = new CompareOp() {
        @Override
        public CompareResult compare(ValueSource a, ValueSource b) {
            double aDouble = a.getDouble();
            double bDouble = b.getDouble();
            if (aDouble < bDouble)
                return CompareResult.LT;
            return aDouble > bDouble ? CompareResult.GT : CompareResult.EQ;
        }

        @Override
        public AkType opType() {
            return AkType.DOUBLE;
        }
    };

    // nested classes
    public static abstract class CompareOp {
        abstract CompareResult compare(ValueSource a, ValueSource b);
        abstract AkType opType();

        private CompareOp() {}
    }

    private static class LongCompareOp extends CompareOp {
        @Override
        CompareResult compare(ValueSource a, ValueSource b) {
            long aLong = getLong(a);
            long bLong = getLong(b);
            if (aLong < bLong)
                return CompareResult.LT;
            return aLong > bLong ? CompareResult.GT : CompareResult.EQ;
        }

        @Override
        AkType opType() {
            return type;
        }

        private long getLong(ValueSource source) {
            switch (type) {
            case DATE:      return source.getDate();
            case DATETIME:  return source.getDateTime();
            case INT:       return source.getInt();
            case LONG:      return source.getLong();
            case TIME:      return source.getTime();
            case TIMESTAMP: return source.getTimestamp();
            case U_INT:     return source.getUInt();
            case YEAR:      return source.getYear();
            default: throw new UnsupportedOperationException("can't get long for type " + type);
            }
        }

        private LongCompareOp(AkType type) {
            this.type = type;
            ArgumentValidation.notNull("type", this.type);
        }

        private final AkType type;
    }

    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation {
        @Override
        public ValueSource eval() {
            if (left().isNull() || right().isNull())
                return BoolValueSource.OF_NULL;
            CompareResult compareResult = op.compare(left(), right());
            return BoolValueSource.of(compareResult.checkAgainstComparison(comparison));
        }

        private InnerEvaluation(List<? extends ExpressionEvaluation> children, Comparison comparison, CompareOp op) {
            super(children);
            this.op = op;
            this.comparison = comparison;
        }

        private final Comparison comparison;
        private final CompareOp op;
    }
}
