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

package com.akiban.server.service.dxl;

import com.akiban.ais.model.IndexName;

import java.io.Serializable;

public final class IndexCheckResult implements Serializable {

    public String getIndexName() {
        return String.valueOf(indexName);
    }

    public long getExpectedCount() {
        return expectedCount;
    }

    public long getSawCount() {
        return sawCount;
    }

    public long getVerifiedCount() {
        return verifiedCount;
    }

    public BottomLine getBottomLine() {
        if (expectedCount == sawCount && sawCount == verifiedCount)
            return BottomLine.OK;
        else if (expectedCount != sawCount && sawCount == verifiedCount)
            return BottomLine.FIXED;
        else
            return BottomLine.BROKEN;
    }

    @Override
    public String toString() {
        return String.format("%s: %s expected %d, saw %d, verified %d", getBottomLine(),
                indexName, expectedCount, sawCount, verifiedCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexCheckResult that = (IndexCheckResult) o;

        return expectedCount == that.expectedCount
                && sawCount == that.sawCount
                && verifiedCount == that.verifiedCount
                && indexName.equals(that.indexName);
    }

    @Override
    public int hashCode() {
        int result = indexName.hashCode();
        result = 31 * result + (int) (expectedCount ^ (expectedCount >>> 32));
        result = 31 * result + (int) (sawCount ^ (sawCount >>> 32));
        result = 31 * result + (int) (verifiedCount ^ (verifiedCount >>> 32));
        return result;
    }

    public IndexCheckResult(IndexName indexName, long expectedCount, long sawCount, long verifiedCount) {
        this.indexName = indexName;
        this.expectedCount = expectedCount;
        this.sawCount = sawCount;
        this.verifiedCount = verifiedCount;
    }

    private final IndexName indexName;
    private final long expectedCount;
    private final long sawCount;
    private final long verifiedCount;

    public enum BottomLine {
        OK, FIXED, BROKEN
    }
}
