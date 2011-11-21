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

package com.akiban.sql.optimizer.rule.range;

import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.plan.ConstantExpression;

public abstract class RangeEndpoint {
    public abstract boolean isUpperWild();
    public abstract ConstantExpression getValueExpression();
    public abstract Object getValue();
    public abstract boolean isInclusive();
    public abstract String describeValue();

    private RangeEndpoint() {}

    public static ValueEndpoint inclusive(ConstantExpression value) {
        return new ValueEndpoint(value, true);
    }

    public static  ValueEndpoint exclusive(ConstantExpression value) {
        return new ValueEndpoint(value, false);
    }

    public static RangeEndpoint of(ConstantExpression value, boolean inclusive) {
        return new ValueEndpoint(value, inclusive);
    }

    public static final RangeEndpoint UPPER_WILD = new Wild();
    public static final RangeEndpoint NULL_EXCLUSIVE = exclusive(new ConstantExpression(null, AkType.NULL));
    public static final RangeEndpoint NULL_INCLUSIVE = inclusive(NULL_EXCLUSIVE.getValueExpression());

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
}
