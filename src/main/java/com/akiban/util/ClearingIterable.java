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

package com.akiban.util;

import java.util.Collection;
import java.util.Iterator;

/**
 * <p>An iterable that <tt>clear()</tt>s its underlying {@linkplain Collection} once all of the items
 * have been iterated over.</p>
 *
 * <p>Note that the underlying <tt>Collection</tt> must support removal of items, specifically, via {@code clear()}.</p>
 *
 * <p>The Iterator you get from this Iterable works just like the Iterator you would get from the underlying
 * Collection, except that it will call the collection's {@code clear()} method after the last item has been retrieved,
 * before it returns from {@code next()}. Subsequent calls to {@code hasNext()} will always return {@code false},
 * and {@code remove()} will be treated as a no-op.</p>
 *
 * @param <T> the item to be iterated over
 */
public final class ClearingIterable<T> implements Iterable<T>
{
    private final Collection<T> collection;

    public static <T> ClearingIterable<T> from(Collection<T> collection)
    {
        return new ClearingIterable<T>(collection);
    }

    private ClearingIterable(Collection<T> collection)
    {
        this.collection = collection;
    }

    @Override
    public Iterator<T> iterator()
    {
        return new ClearAtEndIterator<T>( collection );
    }

    private static class ClearAtEndIterator<T> implements Iterator<T>
    {
        private final Iterator<T> iter;
        private final Collection<T> collection;
        boolean done = false;

        public ClearAtEndIterator(Collection<T> collection)
        {
            this.collection = collection;
            this.iter = collection.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return (!done) && iter.hasNext();
        }

        @Override
        public T next()
        {
            final T ret = iter.next();
            if (!iter.hasNext())
            {
                collection.clear();
                done = true;
            }
            return ret;
        }

        @Override
        public void remove()
        {
            if (!done)
            {
                iter.remove();
            }
        }
    }
}
