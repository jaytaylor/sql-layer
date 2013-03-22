
package com.akiban.server.types3;

import java.util.Iterator;

public abstract class LazyListBase<T> implements LazyList<T>
{
    private class LazyIterator implements Iterator<T>
    {
        private int index;
        LazyIterator()
        {
            index = 0;
        }

        @Override
        public boolean hasNext()
        {
            return index < size();
        }

        @Override
        public T next()
        {
            return get(index++);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return new LazyIterator();
    }
}