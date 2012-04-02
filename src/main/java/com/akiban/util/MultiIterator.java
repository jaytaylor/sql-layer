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

// For combining multiple iterators into one. Iterators are consumed in the order supplied by the constructor.

import java.util.Iterator;
import java.util.NoSuchElementException;

public class MultiIterator<T> implements Iterator<T>
{
    // Iterator interface

    @Override
    public boolean hasNext()
    {
        if (currentIterator == null) {
            throw new NoSuchElementException();
        }
        boolean hasNext = false;
        while (!currentIterator.hasNext() && nextIterator < iterators.length) {
            currentIterator = iterators[nextIterator++];
        }
        if (currentIterator != null) {
            if (currentIterator.hasNext()) {
                hasNext = true;
            } else {
                currentIterator = null;
            }
        }
        return hasNext;
    }

    @Override
    public T next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return currentIterator.next();
    }

    @Override
    public void remove()
    {
        if (currentIterator == null) {
            throw new IllegalStateException();
        }
        currentIterator.remove();
    }

    // MultiIterator interface

    public MultiIterator(Iterator<T> ... iterators)
    {
        this.iterators = iterators;
        this.currentIterator =
            new Iterator<T>()
            {
                @Override
                public boolean hasNext()
                {
                    return false;
                }

                @Override
                public T next()
                {
                    return null;
                }

                @Override
                public void remove()
                {
                    throw new IllegalStateException();
                }
            };
    }

    // Object state

    private final Iterator<T>[] iterators;
    private Iterator<T> currentIterator;
    private int nextIterator = 0;
}
