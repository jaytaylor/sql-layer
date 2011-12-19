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

final class BucketSource<A> implements Iterable<Bucket<A>> {

    // Iterable interface

    @Override
    public Iterator<Bucket<A>> iterator() {
        return new InternalIterator<A>(iterable.iterator());
    }

    // ctor

    public BucketSource(Iterable<? extends A> iterable) {
        ArgumentValidation.notNull("input", iterable);
        this.iterable = iterable;
    }

    private final Iterable<? extends A> iterable;

    // nested classes

    private static class InternalIterator<A> implements Iterator<Bucket<A>> {

        @Override
        public boolean hasNext() {
            return last != null || source.hasNext();
        }

        @Override
        public Bucket<A> next() {
            Bucket<A> result;
            if (last == null) {
                result = new Bucket<A>(source.next());
            }
            else {
                result = new Bucket<A>(last);
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

        InternalIterator(Iterator<? extends A> source) {
            this.source = source;
        }

        private A last;
        private final Iterator<? extends A> source;
    }
}