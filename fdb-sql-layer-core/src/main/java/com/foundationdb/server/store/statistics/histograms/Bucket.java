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

package com.foundationdb.server.store.statistics.histograms;

import java.util.Objects;

public final class Bucket<T> {

    // Bucket interface

    public T value() {
        return value;
    }

    public long getEqualsCount() {
        return equalsCount;
    }

    public long getLessThanCount() {
        return ltCount;
    }

    public long getLessThanDistinctsCount() {
        return ltDistinctCount;
    }

    public void addEquals() {
        ++equalsCount;
    }

    public void addLessThans(long value) {
        ltCount += value;
    }

    public void addLessThanDistincts(long value) {
        ltDistinctCount += value;
    }

    public void markMinKeyBucket() {
        minKeyBucket = true;
    }

    public boolean isMinKeyBucket() {
        return minKeyBucket;
    }

    // for use in this class

    void init(T value, int count) {
        this.value = value;
        this.equalsCount = count;
        this.ltCount = 0;
        this.ltDistinctCount = 0;
    }

    Bucket() {
    }

    // object interface

    @Override
    public String toString() {
        return String.format("<%s: %d,%d,%d>", value, equalsCount, ltCount, ltDistinctCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Bucket bucket = (Bucket) o;

        return equalsCount == bucket.equalsCount
                && ltCount == bucket.ltCount
                && ltDistinctCount == bucket.ltDistinctCount
                && Objects.deepEquals(value, bucket.value);

    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (int) (equalsCount ^ (equalsCount >>> 32));
        result = 31 * result + (int) (ltCount ^ (ltCount >>> 32));
        result = 31 * result + (int) (ltDistinctCount ^ (ltDistinctCount >>> 32));
        return result;
    }

    private T value;
    private long equalsCount;
    private long ltCount;
    private long ltDistinctCount;
    private boolean minKeyBucket = false;
}
