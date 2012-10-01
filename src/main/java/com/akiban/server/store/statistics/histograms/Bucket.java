/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
