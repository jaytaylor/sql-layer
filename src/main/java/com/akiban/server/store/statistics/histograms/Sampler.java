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
import java.util.List;

/**
 * Combines the SplitHandler (T visitor) with Buckets (T aggregator).
 * @param <T> the thing to be sampled
 */
public class Sampler<T> extends SplitHandler<T> {
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
        List<List<Bucket<T>>> results = new ArrayList<List<Bucket<T>>>(segments);
        for (int i=0; i < segments; ++i ) {
            results.add(bucketSamplerList.get(i).buckets());
        }
        return results;
    }

    public Sampler(Splitter<T> splitter, int maxSize, Recycler<? super T> recycler) {
        this(splitter, maxSize, null, recycler);
    }

    Sampler(Splitter<T> splitter, int maxSize, MyLong expectedInputs, Recycler<? super T> recycler) {
        super(splitter);
        int segments = splitter.segments();
        ArgumentValidation.isGT("segments", segments, 0);
        bucketSamplerList =  new ArrayList<BucketSampler<T>>(segments);
        for (int i=0; i < segments; ++i) {
            bucketSamplerList.add(new BucketSampler<T>(maxSize, expectedInputs));
        }
        this.maxSize = maxSize;
        this.segments = segments;
        this.bucketsFlywheel = new BucketFlywheel<T>(maxSize, segments, recycler);
    }

    final List<BucketSampler<T>> bucketSamplerList;
    final int maxSize;
    final int segments;
    boolean finished = false;
    private final Flywheel<Bucket<T>> bucketsFlywheel;

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
