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

package com.akiban.server.store.histograms;

final class Bucket<A> {

    // Bucket interface

    public A value() {
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

    // object interface

    @Override
    public String toString() {
        return String.format("<%s: %d,%d,%d>", value, equalsCount, ltCount, ltDistinctCount);
    }

    // ctor

    public Bucket(A value) {
        this.value = value;
        this.equalsCount = 1;
    }

    private final A value;
    private long equalsCount;
    private long ltCount;
    private long ltDistinctCount;
}
