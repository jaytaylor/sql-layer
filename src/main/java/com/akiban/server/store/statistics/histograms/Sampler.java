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

import com.akiban.util.ArgumentValidation;
import com.akiban.util.Flywheel;
import com.akiban.util.Recycler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Combines the SplitHandler (T visitor) with Buckets (T aggregator).
 * @param <T> the thing to be sampled
 */
public class Sampler<T extends Comparable<? super T>> extends SplitHandler<T> {
    @Override
    protected void handle(int segmentIndex, T input, int count) {
        BucketSampler<T> bucketSampler = bucketSamplerList.get(segmentIndex);
        Bucket<T> bucket = bucketsFlywheel.get();
        bucket.init(input, count);
        if (!bucketSampler.add(bucket))
            bucketsFlywheel.recycle(bucket);
    }

    @Override
    public void finish() {
        super.finish();
        finished = true;
    }

    public List<List<Bucket<T>>> toBuckets() {
        if (!finished) {
            throw new IllegalStateException("never called finish() after visiting");
        }
        List<PopularitySplit<T>> popularitySplits = splitStreamsByPopularity();
        return mergePopularitySplitStreams(popularitySplits);
    }

    private List<PopularitySplit<T>> splitStreamsByPopularity() {
        List<PopularitySplit<T>> results = new ArrayList<PopularitySplit<T>>(segments);
        for (BucketSampler<T> sampler : bucketSamplerList) {
            PopularitySplit<T> popularitySplit = splitByPopularity(sampler);
            results.add(popularitySplit);
        }
        return results;
    }

    private PopularitySplit<T> splitByPopularity(BucketSampler<T> sampler) {
        List<Bucket<T>> samples = sampler.buckets();
        List<Bucket<T>> popular = new ArrayList<Bucket<T>>(samples.size());

        // a bucket is "exceptionally popular" if its popularity (equals-count) is more than one standard dev
        // above average
        long popularityCutoff = Math.round(sampler.getEqualsMean() + sampler.getEqualsStdDev());
        for (Iterator<Bucket<T>> iter = samples.iterator(); iter.hasNext(); ) {
            Bucket<T> sample = iter.next();
            // the last bucket is also treated as exceptionally popular
            if (!iter.hasNext() || (sample.getEqualsCount() >= popularityCutoff)) {
                iter.remove();
                popular.add(sample);
            }
        }
        return new PopularitySplit<T>(samples, popular);
    }

    private List<List<Bucket<T>>> mergePopularitySplitStreams(List<PopularitySplit<T>> popularitySplits) {
        List<List<Bucket<T>>> results = new ArrayList<List<Bucket<T>>>(popularitySplits.size());
        for (PopularitySplit<T> split : popularitySplits) {
            List<Bucket<T>> merged = mergePopularitySplit(split);
            results.add(merged);
        }
        return results;
    }


    private List<Bucket<T>> mergePopularitySplit(PopularitySplit<T> split) {
        List<Bucket<T>> populars = split.popularBuckets;
        if (populars.size() == maxSize)
            return populars;
        if (populars.size() > maxSize)
            return trimmed(populars);
        throw new UnsupportedOperationException("TODO");
    }

    private List<Bucket<T>> trimmed(List<Bucket<T>> buckets) {
        throw new UnsupportedOperationException("TODO");
    }

    public Sampler(Splitter<T> splitter, int maxSize, Recycler<? super T> recycler) {
        this(splitter, maxSize, null, recycler);
    }

    Sampler(Splitter<T> splitter, int maxSize, MyLong expectedInputs, Recycler<? super T> recycler) {
        super(splitter);
        int segments = splitter.segments();
        ArgumentValidation.isGT("segments", segments, 0);
        bucketSamplerList =  new ArrayList<BucketSampler<T>>(segments);
        this.maxSize = maxSize;
        int oversampleSize = maxSize * OVERSAMPLE_FACTOR;
        for (int i=0; i < segments; ++i) {
            bucketSamplerList.add(new BucketSampler<T>(oversampleSize, expectedInputs));
        }
        this.segments = segments;
        this.bucketsFlywheel = new BucketFlywheel<T>(oversampleSize, segments, recycler);
    }

    private final List<BucketSampler<T>> bucketSamplerList;
    private final int segments;
    private final int maxSize;
    private boolean finished = false;
    private final Flywheel<Bucket<T>> bucketsFlywheel;

    private static final int OVERSAMPLE_FACTOR = 50;

    private static class PopularitySplit<T> {
        private PopularitySplit(List<Bucket<T>> regularBuckets, List<Bucket<T>> popularBuckets) {
            this.regularBuckets = regularBuckets;
            this.popularBuckets = popularBuckets;
        }

        private final List<Bucket<T>> regularBuckets;
        private final List<Bucket<T>> popularBuckets;
    }

    private static class BucketFlywheel<T> extends Flywheel<Bucket<T>> {
        @Override
        protected Bucket<T> createNew() {
            ++created;
            assert created <= createdLimit : created + " > " + createdLimit;
            return new Bucket<T>();
        }

        @Override
        public void recycle(Bucket<T> element) {
            super.recycle(element);
            valueRecycler.recycle(element.value());
        }

        private BucketFlywheel(int maxSize, int segments, Recycler<? super T> valueRecycler) {
            this.createdLimit = (maxSize+1) * segments;
            this.valueRecycler = valueRecycler;
        }

        private final Recycler<? super T> valueRecycler;
        private final int createdLimit;
        private int created;
    }
}
