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

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.StandardException;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class CompareExpression extends AbstractBinaryExpression {

    @Scalar("equals") public static final ExpressionComposer EQ_COMPOSER = new InnerComposer(Comparison.EQ);
    @Scalar("greaterOrEquals") public static final ExpressionComposer GE_COMPOSER = new InnerComposer(Comparison.GE);
    @Scalar("greaterThan") public static final ExpressionComposer GT_COMPOSER = new InnerComposer(Comparison.GT);
    @Scalar("lessOrEquals") public static final ExpressionComposer LE_COMPOSER = new InnerComposer(Comparison.LE);
    @Scalar("lessThan") public static final ExpressionComposer LT_COMPOSER = new InnerComposer(Comparison.LT);
    @Scalar("notEquals") public static final ExpressionComposer NE_COMPOSER = new InnerComposer(Comparison.NE);

    // AbstractTwoArgExpression interface
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(comparison);
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations(), comparison, op);
    }

    @Override
    protected boolean nullIsContaminating() {
        return true;
    }

    public CompareExpression(Expression lhs, Comparison comparison, Expression rhs) {
        this(AkType.BOOL, lhs, comparison, rhs);
    }
    
    // For use by RankExpression
    protected CompareExpression(Expression lhs, Expression rhs) {
        this(AkType.INT, lhs, null, rhs);
    }
    
    // overriding protected methods

    @Override
    protected void buildToString(StringBuilder sb) {//Field(2) < Literal(8888)
        sb.append(left()).append(' ').append(comparison).append(' ').append(right());
    }


    // for use in this class

    private CompareExpression(AkType outputType, Expression lhs, Comparison comparison, Expression rhs)
    {
        super(outputType, lhs, rhs);
        this.comparison = comparison;
        AkType childType = childrenType(children());
        assert childType != null;
        this.op = readOnlyCompareOps.get(childType);
        if (this.op == null)
            throw new AkibanInternalException("couldn't find internal comparator for " + childType);
    }

    private static Map<AkType,CompareOp> createCompareOpsMap() {
        Map<AkType,CompareOp> map = new EnumMap<AkType, CompareOp>(AkType.class);
        for (AkType type : AkType.values()) {
            if (type == AkType.NULL || type == AkType.UNSUPPORTED)
                continue;
            switch (type.underlyingType()) {
                case BOOLEAN_AKTYPE:
                    map.put(type, createBoolCompareOp());
                    break;
                case LONG_AKTYPE:
                    map.put(type, createLongCompareOp(type));
                    break;
                case FLOAT_AKTYPE:
                    map.put(type, createFloatCompareOp());
                    break;
                case DOUBLE_AKTYPE:
                    map.put(type, createDoubleCompareOp());
                    break;
                case OBJECT_AKTYPE:
                    map.put(type, createObjectCompareOp(type));
                    break;
            }
        }
        map.put(AkType.NULL, createNullCompareOp());
        return map;
    }

    private static CompareOp createBoolCompareOp() {
        return new CompareOp() {
            @Override
            public int compare(ValueSource a, ValueSource b) {
                boolean aBool = Extractors.getBooleanExtractor().getBoolean(a, null);
                boolean bBool = Extractors.getBooleanExtractor().getBoolean(b, null);
                return Boolean.valueOf(aBool).compareTo(bBool);
            }
        };
    }

    private static CompareOp createDoubleCompareOp() {
        return new CompareOp() {
            @Override
            public int compare(ValueSource a, ValueSource b) {
                double aDouble = Extractors.getDoubleExtractor().getDouble(a);
                double bDouble = Extractors.getDoubleExtractor().getDouble(b);
                return Double.compare(aDouble, bDouble);
            }
        };
    }

    private static CompareOp createFloatCompareOp() {
        return new CompareOp() {
            @Override
            public int compare(ValueSource a, ValueSource b) {
                // There are no float extractors. But still need to be
                // comparison with only float precision.
                float aFloat = (float)Extractors.getDoubleExtractor().getDouble(a);
                float bFloat = (float)Extractors.getDoubleExtractor().getDouble(b);
                return Float.compare(aFloat, bFloat);
            }
        };
    }

    private static CompareOp createLongCompareOp(final AkType type) {
        return new CompareOp() {
            @Override
            public int compare(ValueSource a, ValueSource b) {
                long aLong = Extractors.getLongExtractor(type).getLong(a);
                long bLong = Extractors.getLongExtractor(type).getLong(b);
                if (aLong < bLong)
                    return -1;
                else if (aLong > bLong)
                    return +1;
                else
                    return 0;
            }
        };
    }

    private static CompareOp createObjectCompareOp(final AkType type) {
        return new CompareOp() {
            @Override
            public int compare(ValueSource a, ValueSource b) {
                switch (type) {
                case DECIMAL:   return doCompare(Extractors.getDecimalExtractor(), a, b);
                case VARCHAR:   return doCompare(Extractors.getStringExtractor(), a, b);
                case TEXT:      return doCompare(Extractors.getStringExtractor(), a, b);
                case U_BIGINT:  return doCompare(Extractors.getUBigIntExtractor(), a, b);
                case VARBINARY: return doCompare(a.getVarBinary(), b.getVarBinary());
                default: throw new UnsupportedOperationException("can't get comparable for type " + type);
                }
            }

            private <T extends Comparable<T>> int doCompare(ObjectExtractor<T> extractor, ValueSource one, ValueSource two) {
                return doCompare(extractor.getObject(one), extractor.getObject(two));
            }

            private <T extends Comparable<T>> int doCompare(T one, T two) {
                return one.compareTo(two);
            }
        };
    }

    private static CompareOp createNullCompareOp() {
        return new CompareOp() {
            @Override
            public int compare(ValueSource a, ValueSource b) {
                throw new AssertionError("nulls are not comparable");
            }
        };
    }

    // object state

    private final Comparison comparison;
    protected final CompareOp op;

    // consts

    private static final Map<AkType, CompareOp> readOnlyCompareOps = createCompareOpsMap();

    // nested classes
    protected interface CompareOp {
        abstract int compare(ValueSource a, ValueSource b);
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

    private static final class InnerComposer extends BinaryComposer {

        @Override
        protected Expression compose(Expression first, Expression second) {
            return new CompareExpression(first, comparison, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            argumentTypes.setType(1, argumentTypes.get(0).getType());
            return ExpressionTypes.BOOL;
        }

        private InnerComposer(Comparison comparison) {
            this.comparison = comparison;
        }

        private final Comparison comparison;
    }
}
