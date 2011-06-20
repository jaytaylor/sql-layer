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
