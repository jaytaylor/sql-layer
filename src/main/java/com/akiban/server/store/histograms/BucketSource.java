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

import java.util.Iterator;

final class BucketSource<T> implements Iterable<Bucket<T>> {

    // Iterable interface

    @Override
    public Iterator<Bucket<T>> iterator() {
        return new InternalIterator<T>(iterable.iterator(), bucketPool);
    }
    
    public void release(Bucket<T> bucket) {
        bucketPool.release(bucket);
    }

    // ctor

    public BucketSource(Iterable<? extends T> iterable, int maxCreates) {
        ArgumentValidation.notNull("input", iterable);
        this.iterable = iterable;
        this.maxCreates = maxCreates;
    }

    private final Iterable<? extends T> iterable;
    private final int maxCreates;
    private final Flywheel<Bucket<T>> bucketPool = new Flywheel<Bucket<T>>() {
        @Override
        protected Bucket<T> createNew() {
            ++creates;
            assert creates <= maxCreates : creates + " > " + maxCreates;
            return new Bucket<T>();
        }
        
        int creates = 0;
    };

    // nested classes

    private static class InternalIterator<T> implements Iterator<Bucket<T>> {

        @Override
        public boolean hasNext() {
            return last != null || source.hasNext();
        }

        @Override
        public Bucket<T> next() {
            Bucket<T> result = flywheel.get();
            if (last == null) {
                result.init(source.next());
            }
            else {
                result.init(last);
                last = null;
            }
            while (source.hasNext()) {
                last = source.next();
                if (!last.equals(result.value()))
                    return result;
                result.addEquals();
            }
            // saw last element from source
            last = null;
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        InternalIterator(Iterator<? extends T> source, Flywheel<? extends Bucket<T>> flywheel) {
            this.source = source;
            this.flywheel = flywheel;
        }

        private T last;
        private final Iterator<? extends T> source;
        private final Flywheel<? extends Bucket<T>> flywheel;
    }
}