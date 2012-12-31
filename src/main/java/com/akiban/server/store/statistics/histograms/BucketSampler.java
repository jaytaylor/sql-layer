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


import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;

import java.util.ArrayList;
import java.util.List;

final class BucketSampler<T> {
    
    public boolean add(Bucket<T> bucket) {
        long bucketEqualsCount = bucket.getEqualsCount();
        long bucketsRepresented = (bucketEqualsCount + bucket.getLessThanCount());
        inputsCount += bucketsRepresented;
        // last bucket is always in
        if (inputsCount > expectedCount)
            throw new IllegalStateException("expected " + expectedCount + " elements, but saw " + inputsCount);
        // Form a bucket if:
        // 1) we've crossed a median point,
        // 2) we're at the end, or
        // 3) we're at the beginning (see bug 1052606)
        boolean insertIntoResults = false;
        if (buckets.isEmpty()) {
            // Bucket with min value of index
            bucket.markMinKeyBucket();
            insertIntoResults = true;
            // We want to keep the min-value bucket no matter what. If we were going to keep it anyway, due to
            // crossing a median point, then our median markers are OK as is. Otherwise, they need to be recomputed.
            if (inputsCount >= nextMedianPoint) {
                while (inputsCount >= nextMedianPoint) {
                    nextMedianPoint += medianPointDistance;
                }
            } else {
                computeMedianPointBoundaries(maxSize - 1);
            }
        } else if (inputsCount == expectedCount) {
            // end
            insertIntoResults = true;
        } else {
            // Did we cross a median point?
            while (inputsCount >= nextMedianPoint) {
                insertIntoResults = true;
                nextMedianPoint += medianPointDistance;
            }
        }
        if (insertIntoResults) {
            appendToResults(bucket);
        }
        else {
            runningLessThans += bucketsRepresented;
            runningLessThanDistincts += bucket.getLessThanDistinctsCount() + 1;
        }

        // stats
        if (stdDev != null)
            stdDev.increment(bucketEqualsCount);
        ++bucketsSeen;
        equalsSeen += bucketEqualsCount;
        return insertIntoResults;
    }
    
    public void appendToResults(Bucket<T> bucket) {
        bucket.addLessThanDistincts(runningLessThanDistincts);
        bucket.addLessThans(runningLessThans);
        buckets.add(bucket);
        runningLessThanDistincts = 0;
        runningLessThans = 0;
    }

    List<Bucket<T>> buckets() {
        if (expectedCount != inputsCount)
            throw new IllegalStateException("expected " + expectedCount + " inputs but saw " + inputsCount);
        return buckets;
    }
    
    public double getEqualsStdDev() {
        if (stdDev == null)
            throw new IllegalStateException("standard deviation not computed");
        return stdDev.getResult();
    }

    public double getEqualsMean() {
        return ((double)equalsSeen) / ((double)bucketsSeen);
    }

    BucketSampler(int bucketCount, long expectedInputs) {
        this(bucketCount, expectedInputs, true);
    }

    BucketSampler(int maxSize, long expectedInputs, boolean calculateStandardDeviation) {
        if (maxSize < 1)
            throw new IllegalArgumentException("max must be at least 1");
        if (expectedInputs < 0)
            throw new IllegalArgumentException("expectedInputs must be non-negative: " + expectedInputs);
        this.maxSize = maxSize;
        this.expectedCount = expectedInputs;
        this.buckets = new ArrayList<Bucket<T>>(maxSize + 1);
        this.stdDev = calculateStandardDeviation ? new StandardDeviation() : null;
        computeMedianPointBoundaries(maxSize);
    }

    private void computeMedianPointBoundaries(int maxSize)
    {
        double medianPointDistance = ((double)expectedCount) / maxSize;
        this.medianPointDistance = medianPointDistance == 0 ? 1 : medianPointDistance;
        this.nextMedianPoint = this.medianPointDistance;
        assert this.nextMedianPoint > 0 : this.nextMedianPoint;
    }

    private final int maxSize;
    private final long expectedCount;
    private double medianPointDistance;
    private StandardDeviation stdDev;
    private double nextMedianPoint;
    private long inputsCount;
    private long runningLessThans;
    private long runningLessThanDistincts;
    private long bucketsSeen;
    private long equalsSeen;
    
    private final List<Bucket<T>> buckets;
    
}