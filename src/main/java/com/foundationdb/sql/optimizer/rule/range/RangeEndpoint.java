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

package com.foundationdb.sql.optimizer.rule.range;

import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;

public abstract class RangeEndpoint implements Comparable<RangeEndpoint> {

    public abstract boolean isUpperWild();
    public abstract ConstantExpression getValueExpression();
    public abstract Object getValue();
    public abstract boolean isInclusive();
    public abstract String describeValue();

    @Override
    public int compareTo(RangeEndpoint o) {
        ComparisonResult comparison = compareEndpoints(this, o);
        switch (comparison) {
        case LT:
        case LT_BARELY:
            return -1;
        case GT:
        case GT_BARELY:
            return 1;
        case EQ:
            return 0;
        case INVALID:
        default:
            throw new IllegalComparisonException(this.getValue(), o.getValue());
        }
    }

    public ComparisonResult comparePreciselyTo(RangeEndpoint other) {
        return compareEndpoints(this, other);
    }

    public static ValueEndpoint inclusive(ConstantExpression value) {
        return new ValueEndpoint(value, true);
    }

    public static  ValueEndpoint exclusive(ConstantExpression value) {
        return new ValueEndpoint(value, false);
    }

    public static ValueEndpoint nullExclusive(ExpressionNode nodeOfMatchingType) {
        return exclusive(nullConstantExpression(nodeOfMatchingType));
    }

    public static RangeEndpoint nullInclusive(ExpressionNode nodeOfMatchingType) {
        return inclusive(nullConstantExpression(nodeOfMatchingType));
    }

    public static RangeEndpoint of(ConstantExpression value, boolean inclusive) {
        return new ValueEndpoint(value, inclusive);
    }

    private RangeEndpoint() {}

    /**
     * Returns whether the two endpoints are LT, GT or EQ to each other.
     * @param point1 the first point
     * @param point2 the second point
     * @return LT if point1 is less than point2; GT if point1 is greater than point2; EQ if point1 is greater than
     * point2; and INVALID if the two points can't be compared
     */
    private static ComparisonResult compareEndpoints(RangeEndpoint point1, RangeEndpoint point2)
    {
        if (point1.equals(point2))
            return ComparisonResult.EQ;
        // At this point we know they're not both upper wild. If either one is, we know the answer.
        if (point1.isUpperWild())
            return ComparisonResult.GT;
        if (point2.isUpperWild())
            return ComparisonResult.LT;

        // neither is wild
        ComparisonResult comparison = compareObjects(point1.getValue(), point2.getValue());
        if (comparison == ComparisonResult.EQ && (point1.isInclusive() != point2.isInclusive())) {
            if (point1.isInclusive())
                return ComparisonResult.LT_BARELY;
            assert point2.isInclusive() : point2;
            return ComparisonResult.GT_BARELY;
        }
        return comparison;
    }

    private static ConstantExpression nullConstantExpression(ExpressionNode nodeOfMatchingType) {
        return ConstantExpression.typedNull(nodeOfMatchingType.getSQLtype(), nodeOfMatchingType.getSQLsource(),
                nodeOfMatchingType.getType());
    }

    @SuppressWarnings("unchecked") // We know that oneT and twoT are both Comparables of the same class.
    private static ComparisonResult compareObjects(Object one, Object two) {
        // if both are null, they're equal. Otherwise, at most one can be null; if either is null, we know the
        // answer. Otherwise, we know neither is null, and we can test their values (after checking the classes)
        if (one == two)
            return ComparisonResult.EQ;
        if (one == null)
            return ComparisonResult.LT;
        if (two == null)
            return ComparisonResult.GT;
        int compareResult;
        if (one.getClass().equals(two.getClass())) {
            if (!(one instanceof Comparable))
                return ComparisonResult.INVALID;
            Comparable oneT = (Comparable) one;
            Comparable twoT = (Comparable) two;
            compareResult = (oneT).compareTo(twoT);
        }
        else if (((one.getClass() == Byte.class) || (one.getClass() == Short.class) ||
                  (one.getClass() == Integer.class) || (one.getClass() == Long.class)) &&
                 ((two.getClass() == Byte.class) || (two.getClass() == Short.class) ||
                  (two.getClass() == Integer.class) || (two.getClass() == Long.class))) {
            Number oneT = (Number) one;
            Number twoT = (Number) two;
            // TODO: JDK 7 this is in Long.
            compareResult = com.google.common.primitives.Longs.compare(oneT.longValue(),
                                                                       twoT.longValue());
        }
        else
            return ComparisonResult.INVALID;
        if (compareResult < 0)
            return ComparisonResult.LT;
        else if (compareResult > 0)
            return ComparisonResult.GT;
        else
            return ComparisonResult.EQ;
    }

    public static final RangeEndpoint UPPER_WILD = new Wild();


    private static class Wild extends RangeEndpoint {

        @Override
        public boolean isUpperWild() {
            return true;
        }

        @Override
        public Object getValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConstantExpression getValueExpression() {
            return null;
        }

        @Override
        public boolean isInclusive() {
            return false;
        }

        @Override
        public String toString() {
            return "(*)";
        }

        @Override
        public String describeValue() {
            return "*";
        }
    }

    private static class ValueEndpoint extends RangeEndpoint {

        @Override
        public ConstantExpression getValueExpression() {
            return valueExpression;
        }

        @Override
        public Object getValue() {
            return valueExpression.getValue();
        }

        @Override
        public boolean isInclusive() {
            return inclusive;
        }

        @Override
        public boolean isUpperWild() {
            return false;
        }

        @Override
        public String toString() {
            return valueExpression + (inclusive ? " inclusive" : " exclusive");
        }

        @Override
        public String describeValue() {
            Object value = getValue();
            return value == null ? "NULL" : value.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ValueEndpoint that = (ValueEndpoint) o;
            return inclusive == that.inclusive && !(valueExpression != null ? !valueExpression.equals(that.valueExpression) : that.valueExpression != null);

        }

        @Override
        public int hashCode() {
            int result = valueExpression != null ? valueExpression.hashCode() : 0;
            result = 31 * result + (inclusive ? 1 : 0);
            return result;
        }

        private ValueEndpoint(ConstantExpression valueExpression, boolean inclusive) {
            this.valueExpression = valueExpression;
            this.inclusive = inclusive;
        }

        private ConstantExpression valueExpression;
        private boolean inclusive;
    }

    static class IllegalComparisonException extends RuntimeException {
        private IllegalComparisonException(Object one, Object two) {
            super(String.format("couldn't sort objects <%s> and <%s>",
                    one,
                    two
            ));
        }
    }

    enum RangePointComparison {
        MIN() {
            @Override
            protected Object select(Object one, Object two, ComparisonResult comparison) {
                return comparison == ComparisonResult.LT ? one : two;
            }
        },
        MAX() {
            @Override
            protected Object select(Object one, Object two, ComparisonResult comparison) {
                return comparison == ComparisonResult.GT ? one : two;
            }
        }
        ;

        protected abstract Object select(Object one, Object two, ComparisonResult comparison);

        public Object get(Object one, Object two) {
            ComparisonResult comparisonResult = compareObjects(one, two);
            switch (comparisonResult) {
            case EQ:
                return one;
            case LT_BARELY:
            case LT:
            case GT_BARELY:
            case GT:
                return select(one, two, comparisonResult.normalize());
            case INVALID:
                return null;
            default:
                throw new AssertionError(comparisonResult.name());
            }
        }

        public static final Object INVALID_COMPARISON = new Object();
    }
}
