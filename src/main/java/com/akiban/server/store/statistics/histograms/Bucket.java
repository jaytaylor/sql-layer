
package com.akiban.server.store.statistics.histograms;

import com.akiban.util.Equality;

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
                && Equality.areEqual(value, bucket.value);

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
