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
        // see if we've crossed a median point (or are at the end)
        boolean insertIntoResults = (inputsCount == expectedCount);
        if (!insertIntoResults) {
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

    BucketSampler(int maxSize, long expectedInputs) {
        this(maxSize, expectedInputs, true);
    }

    BucketSampler(int maxSize, long expectedInputs, boolean calculateStandardDeviation) {
        if (maxSize < 1)
            throw new IllegalArgumentException("max must be at least 1");
        if (expectedInputs < 0)
            throw new IllegalArgumentException("expectedInputs must be non-negative: " + expectedInputs);
        this.expectedCount = expectedInputs;
        this.buckets = new ArrayList<Bucket<T>>(maxSize + 1);
        long medianPointDistance = Math.round(((double)expectedCount) / maxSize);
        this.medianPointDistance = medianPointDistance == 0 ? 1 : medianPointDistance;
        this.nextMedianPoint = this.medianPointDistance;
        assert this.nextMedianPoint > 0 : this.nextMedianPoint;
        this.stdDev = calculateStandardDeviation ? new StandardDeviation() : null;
    }

    private final long expectedCount;
    private final long medianPointDistance;
    private final StandardDeviation stdDev;

    private long nextMedianPoint;
    private long inputsCount;
    private long runningLessThans;
    private long runningLessThanDistincts;
    private long bucketsSeen;
    private long equalsSeen;
    
    private final List<Bucket<T>> buckets;
    
}