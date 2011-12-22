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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Combines the SplitHandler (T visitor) with Buckets (T aggregator).
 * @param <T> the thing to be sampled
 */
public class Sampler<T> extends SplitHandler<T> {
    @Override
    protected void handle(int segmentIndex, T input, int count) {
        Buckets<T> buckets = bucketsList.get(segmentIndex);
        Bucket<T> bucket = bucketsFlywheel.get();
        bucket.init(input, count);
        buckets.add(bucket, bucketsFlywheel);
    }

    public List<List<Bucket<T>>> toBuckets() {
        List<List<Bucket<T>>> results = new ArrayList<List<Bucket<T>>>(segments);
        for (int i=0; i < segments; ++i ) {
            results.add(bucketsList.get(i).buckets());
        }
        return results;
    }

    public Sampler(Splitter<T> splitter, int maxSize) {
        this(splitter, maxSize, System.nanoTime());
    }

    Sampler(Splitter<T> splitter, int maxSize, long randSeed) {
        super(splitter);
        int segments = splitter.segments();
        ArgumentValidation.isGT("segments", segments, 0);
        bucketsList =  new ArrayList<Buckets<T>>(segments);
        Random random = new Random(randSeed);
        for (int i=0; i < segments; ++i) {
            bucketsList.add(new Buckets<T>(maxSize, random));
        }
        this.maxSize = maxSize;
        this.segments = segments;
    }

    final List<Buckets<T>> bucketsList;
    final int maxSize;
    final int segments;
    final Flywheel<Bucket<T>> bucketsFlywheel = new Flywheel<Bucket<T>>() {
        @Override
        protected Bucket<T> createNew() {
            ++created;
            assert created <= (maxSize+1) * segments : created + " > " + (maxSize+1)*segments;
            return new Bucket<T>();
        }

        private int created;
    };
}
