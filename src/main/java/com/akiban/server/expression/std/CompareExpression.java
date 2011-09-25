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
import com.akiban.server.types.util.ValueHolder;
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
    private final CompareOp VARCHAR_COMPARE = new ObjectCompareOp(AkType.VARCHAR);

    private final CompareOp DOUBLE_COMPARE = new CompareOp() {
        @Override
        public int compare(ValueSource a, ValueSource b) {
            double aDouble = a.getDouble();
            double bDouble = b.getDouble();
            return Double.compare(aDouble, bDouble);
        }

        @Override
        public AkType opType() {
            return AkType.DOUBLE;
        }
    };

    // nested classes
    public static abstract class CompareOp {
        abstract int compare(ValueSource a, ValueSource b);
        abstract AkType opType();

        private CompareOp() {}
    }

    private static class LongCompareOp extends CompareOp {
        @Override
        int compare(ValueSource a, ValueSource b) {
            long aLong = getLong(a);
            long bLong = getLong(b);
            return (int)(aLong - bLong);
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
            ArgumentValidation.isEQ(
                    "require underlying type", AkType.UnderlyingType.LONG_AKTYPE,
                    "given type", this.type.underlyingType()
            );
        }

        private final AkType type;
    }

    private static class ObjectCompareOp extends CompareOp {
        @Override
        int compare(ValueSource a, ValueSource b) {
            switch (type) {
            case DECIMAL:   return doCompare(a.getDecimal(), b.getDecimal());
            case VARCHAR:   return doCompare(a.getString(), b.getString());
            case TEXT:      return doCompare(a.getText(), b.getText());
            case U_BIGINT:  return doCompare(a.getUBigInt(), b.getUBigInt());
            case VARBINARY: return doCompare(a.getVarBinary(), b.getVarBinary());
            default: throw new UnsupportedOperationException("can't get comparable for type " + type);
            }
        }

        private <T extends Comparable<T>> int doCompare(T one, T two) {
            return one.compareTo(two);
        }

        @Override
        AkType opType() {
            return type;
        }

        private ObjectCompareOp(AkType type) {
            this.type = type;
            ArgumentValidation.isEQ(
                    "require underlying type", AkType.UnderlyingType.OBJECT_AKTYPE,
                    "given type", this.type.underlyingType()
            );
        }

        private final AkType type;
    }

    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation {
        @Override
        public ValueSource eval() {
            ValueSource left = scratch.copyFrom(left());
            ValueSource right = right();
            if (left.isNull() || right.isNull())
                return BoolValueSource.OF_NULL;
            int compareResult = op.compare(left, right);
            return BoolValueSource.of(comparison.matchesCompareTo(compareResult));
        }

        private InnerEvaluation(List<? extends ExpressionEvaluation> children, Comparison comparison, CompareOp op) {
            super(children);
            this.op = op;
            this.comparison = comparison;
            this.scratch = new ValueHolder();
        }

        private final Comparison comparison;
        private final CompareOp op;
        private final ValueHolder scratch;
    }
}
