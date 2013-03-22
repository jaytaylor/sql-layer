
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
