package com.foundationdb.server.spatial;

import java.util.Iterator;

public abstract class TransformingIterator<FROM, TO> implements Iterator<TO>
{
    // Iterator interface

    @Override
    public boolean hasNext()
    {
        return input.hasNext();
    }

    @Override
    public TO next()
    {
        return transform(input.next());
    }

    @Override
    public void remove()
    {
        input.remove();
    }

    // TransformingIterator interface

    public abstract TO transform(FROM x);

    public TransformingIterator(Iterator<FROM> input)
    {
        this.input = input;
    }

    // Object state

    private final Iterator<FROM> input;
}
