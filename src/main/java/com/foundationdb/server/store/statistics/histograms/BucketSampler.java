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


import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;

import java.util.ArrayList;
import java.util.List;

final class BucketSampler<T> {
    
    public boolean add(Bucket<T> bucket) {
        long bucketEqualsCount = bucket.getEqualsCount();
        long bucketsRepresented = (bucketEqualsCount + bucket.getLessThanCount());
        inputsCount += bucketsRepresented;
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
        } else if (inputsCount == estimatedInputs) {
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

    BucketSampler(int bucketCount, long estimatedInputs) {
        this(bucketCount, estimatedInputs, true);
    }

    BucketSampler(int maxSize, long estimatedInputs, boolean calculateStandardDeviation) {
        if (maxSize < 1)
            throw new IllegalArgumentException("max must be at least 1");
        if (estimatedInputs < 0)
            throw new IllegalArgumentException("estimatedInputs must be non-negative: " + estimatedInputs);
        this.maxSize = maxSize;
        this.estimatedInputs = estimatedInputs;
        this.buckets = new ArrayList<>(maxSize + 1);
        this.stdDev = calculateStandardDeviation ? new StandardDeviation() : null;
        computeMedianPointBoundaries(maxSize);
    }

    private void computeMedianPointBoundaries(int maxSize)
    {
        double medianPointDistance = ((double)estimatedInputs) / maxSize;
        this.medianPointDistance = medianPointDistance == 0 ? 1 : medianPointDistance;
        this.nextMedianPoint = this.medianPointDistance;
        assert this.nextMedianPoint > 0 : this.nextMedianPoint;
    }

    private final int maxSize;
    private final long estimatedInputs;
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
