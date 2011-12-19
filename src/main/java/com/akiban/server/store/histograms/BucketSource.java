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

import java.util.Iterator;

final class BucketSource<T> implements Iterable<Bucket<T>> {

    // Iterable interface

    @Override
    public Iterator<Bucket<T>> iterator() {
        return new InternalIterator<T>(iterable.iterator());
    }

    // ctor

    public BucketSource(Iterable<? extends T> iterable) {
        ArgumentValidation.notNull("input", iterable);
        this.iterable = iterable;
    }

    private final Iterable<? extends T> iterable;

    // nested classes

    private static class InternalIterator<T> implements Iterator<Bucket<T>> {

        @Override
        public boolean hasNext() {
            return last != null || source.hasNext();
        }

        @Override
        public Bucket<T> next() {
            Bucket<T> result;
            if (last == null) {
                result = new Bucket<T>(source.next());
            }
            else {
                result = new Bucket<T>(last);
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

        InternalIterator(Iterator<? extends T> source) {
            this.source = source;
        }

        private T last;
        private final Iterator<? extends T> source;
    }
}