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

public abstract class RangeEndpoint {

    public abstract boolean isWild();
    public abstract ValueEndpoint asValueEndpoint();
    public final boolean hasValue() {
        return !isWild();
    }

    private RangeEndpoint() {}

    public static ValueEndpoint inclusive(Object value) {
        return new ValueEndpoint(value, true);
    }

    public static  ValueEndpoint exclusive(Object value) {
        return new ValueEndpoint(value, false);
    }

    public static RangeEndpoint of(Object value, boolean inclusive) {
        return new ValueEndpoint(value, inclusive);
    }

    public static final RangeEndpoint WILD = new RangeEndpoint() {
        @Override
        public boolean isWild() {
            return true;
        }

        @Override
        public ValueEndpoint asValueEndpoint() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "*";
        }
    };

    public static class ValueEndpoint extends RangeEndpoint {

        public Object getValue() {
            return value;
        }

        public boolean isInclusive() {
            return inclusive;
        }

        @Override
        public boolean isWild() {
            return false;
        }

        @Override
        public ValueEndpoint asValueEndpoint() {
            return this;
        }

        @Override
        public String toString() {
            return value + (inclusive ? " inclusive" : " exclusive");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ValueEndpoint that = (ValueEndpoint) o;
            return inclusive == that.inclusive && !(value != null ? !value.equals(that.value) : that.value != null);

        }

        @Override
        public int hashCode() {
            int result = value != null ? value.hashCode() : 0;
            result = 31 * result + (inclusive ? 1 : 0);
            return result;
        }

        private ValueEndpoint(Object value, boolean inclusive) {
            this.value = value;
            this.inclusive = inclusive;
        }

        private Object value;
        private boolean inclusive;
    }
}
