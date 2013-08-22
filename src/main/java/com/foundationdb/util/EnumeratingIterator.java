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

package com.foundationdb.util;

import java.util.Iterator;

public final class EnumeratingIterator<T> implements Iterable<Enumerated<T>>
{
    private static class InternalEnumerated<T> implements Enumerated<T>
    {
        private final T elem;
        private final int count;

        private InternalEnumerated(T elem, int count)
        {
            this.elem = elem;
            this.count = count;
        }

        @Override
        public T get()
        {
            return elem;
        }

        @Override
        public int count()
        {
            return count;
        }
    }

    private final class InternalIterator<T> implements Iterator<Enumerated<T>>
    {
        private int counter = 0;
        private final Iterator<T> iter;

        private InternalIterator(Iterator<T> iter)
        {
            this.iter = iter;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public Enumerated<T> next()
        {
            return new InternalEnumerated<>(iter.next(), counter++);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("removal within an enumerating iterator is not defined");
        }
    }

    private final Iterable<T> iterable;

    public static <T> EnumeratingIterator<T> of(Iterable<T> iterable)
    {
        return new EnumeratingIterator<>(iterable);
    }

    public EnumeratingIterator(Iterable<T> iterable)
    {
        this.iterable = iterable;
    }

    @Override
    public Iterator<Enumerated<T>> iterator()
    {
        return new InternalIterator<>(iterable.iterator());
    }
}
