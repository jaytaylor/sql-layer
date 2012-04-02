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
            return new InternalEnumerated<T>(iter.next(), counter++);
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
        return new EnumeratingIterator<T>(iterable);
    }

    public EnumeratingIterator(Iterable<T> iterable)
    {
        this.iterable = iterable;
    }

    @Override
    public Iterator<Enumerated<T>> iterator()
    {
        return new InternalIterator<T>(iterable.iterator());
    }
}
