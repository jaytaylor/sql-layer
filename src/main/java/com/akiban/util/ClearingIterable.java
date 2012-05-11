/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
