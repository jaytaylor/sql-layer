
package com.akiban.sql.optimizer.plan;

/** A context with a Bloom filter. */
public class UsingBloomFilter extends UsingLoaderBase
{
    private BloomFilter bloomFilter;

    public UsingBloomFilter(BloomFilter bloomFilter, PlanNode loader, PlanNode input) {
        super(loader, input);
        this.bloomFilter = bloomFilter;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(bloomFilter);
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
    }

}
