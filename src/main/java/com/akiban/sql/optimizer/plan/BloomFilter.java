
package com.akiban.sql.optimizer.plan;

import java.util.List;

/** A Bloom filter. */
public class BloomFilter extends BaseHashTable
{
    private long estimatedSize;
    private double selectivity;

    public BloomFilter(long estimatedSize, double selectivity) {
        this.estimatedSize = estimatedSize;
        this.selectivity = selectivity;
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }
    public double getSelectivity() {
        return selectivity;
    }

}
