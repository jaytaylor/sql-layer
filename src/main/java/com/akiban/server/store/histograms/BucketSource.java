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

package com.akiban.server.store.histograms;

import com.akiban.util.ArgumentValidation;
import com.akiban.util.Flywheel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

final class BucketSource<T> implements Iterable<List<Bucket<T>>> {

    // Iterable interface

    @Override
    public Iterator<List<Bucket<T>>> iterator() {
        return new InternalIterator<T>(iterable.iterator(), bucketPool, listsPool);
    }
    
    public void release(List<Bucket<T>> list) {
        listsPool.release(list);
    }

    public void release(Bucket<T> bucket) {
        bucketPool.release(bucket);
    }

    // ctor

    public BucketSource(Iterable<? extends List<? extends T>> iterable, int segments, int maxCreates) {
        ArgumentValidation.notNull("input", iterable);
        this.iterable = iterable;
        this.maxCreates = maxCreates;
        this.segments = segments;
    }

    private final Iterable<? extends List<? extends T>> iterable;
    private final int segments;
    private final int maxCreates;
    private final Flywheel<Bucket<T>> bucketPool = new Flywheel<Bucket<T>>() {
        @Override
        protected Bucket<T> createNew() {
            ++creates;
            assert creates <= (maxCreates*segments) : creates + " > " + (maxCreates*segments);
            return new Bucket<T>();
        }

        @Override
        public void release(Bucket<T> element) {
            super.release(element);
        }

        int creates = 0;
    };
    private final Flywheel<List<Bucket<T>>> listsPool = new Flywheel<List<Bucket<T>>>() {
        @Override
        protected List<Bucket<T>> createNew() {
            ++creates;
            assert creates <= 1 : creates + " > " + 1;
            return new ArrayList<Bucket<T>>(segments);
        }
        
        int creates = 0;
    };

    // nested classes

    private static class InternalIterator<T> implements Iterator<List<Bucket<T>>> {

        @Override
        public boolean hasNext() {
            return last != null || source.hasNext();
        }

        @Override
        public List<Bucket<T>> next() {
            List<? extends T> input;
            if (last == null) {
                input = source.next();
            }
            else {
                input = last;
                last = null;
            }
            List<Bucket<T>> result = listsPool.get();
            insertInto(input, result);
            while (source.hasNext()) {
                last = source.next();
                if (!last.equals(input))
                    return result;
                addEquals(result);
            }
            // saw last element from source
            last = null;
            return result;
        }

        private void addEquals(List<Bucket<T>> list) {
            for (Bucket<T> bucket : list)
                bucket.addEquals();
        }

        private void insertInto(List<? extends T> inputs, List<? super Bucket<T>> outputs) {
            outputs.clear();
            for (T input : inputs) {
                Bucket<T> bucket = bucketsPool.get();
                bucket.init(input);
                outputs.add(bucket);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        InternalIterator(
                Iterator<? extends List<? extends T>> source,
                Flywheel<? extends Bucket<T>> flywheel,
                Flywheel<? extends List<Bucket<T>>> listsPool
        ) {
            this.source = source;
            this.bucketsPool = flywheel;
            this.listsPool = listsPool;
        }

        private List<? extends T> last;
        private final Iterator<? extends List<? extends T>> source;
        private final Flywheel<? extends Bucket<T>> bucketsPool;
        private final Flywheel<? extends List<Bucket<T>>> listsPool;
    }
}