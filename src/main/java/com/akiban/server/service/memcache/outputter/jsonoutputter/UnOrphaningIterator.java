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

package com.akiban.server.service.memcache.outputter.jsonoutputter;

import java.util.*;

class UnOrphaningIterator<T> implements Iterator<T>
{
    // Iterator interface

    @Override
    public boolean hasNext()
    {
        advanceIfNecessary();
        return !done;
    }

    @Override
    public T next()
    {
        T next;
        advanceIfNecessary();
        if (done) {
            throw new NoSuchElementException();
        } else {
            next = nextOutput;
            nextOutput = null;
        }
        return next;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // UnOrphaningIterator interface

    public UnOrphaningIterator(Iterator<T> input, Genealogist<T> genealogist)
    {
        this.genealogist = genealogist;
        this.input = input;
        if (input.hasNext()) {
            nextOutput = input.next();
            previousInput = nextOutput;
            done = false;
        } else {
            done = true;
        }
    }

    // For use by this class

    private void advanceIfNecessary()
    {
        if (!done && nextOutput == null) {
            nextOutput = queue.poll();
            if (nextOutput == null && input.hasNext()) {
                nextOutput = input.next();
                genealogist.fillInDescendents(previousInput, nextOutput, queue);
                // Get ready for the next time we have to check whether an item from input is an orphan.
                previousInput = nextOutput;
                if (!queue.isEmpty()) {
                    // nextOutput is an orphan. We know this because fillInDescendents just filled in some descendents.
                    // Put nextOutput at the end of the queue so that its filled-in ancestors are seen first.
                    queue.add(nextOutput);
                    nextOutput = queue.poll();
                    assert nextOutput != null;
                }
            }
            if (nextOutput == null) {
                done = true;
            }
        }
    }

    // Object state

    private final Genealogist<T> genealogist;
    // input provides a sequence of T, instances of which can be arranged hierarchically. The sequence represents
    // a preorder traversal of the hierarchy.
    private final Iterator<T> input;
    // When an orphan is discovered in the input sequence, the queue is filled with the orphan's ancestors, and then
    // the orphan itself. This queue is drained to output before any more input is taken.
    private final Queue<T> queue = new LinkedList<T>();
    // previousInput is the last element taken from input. It is compared with the next element taken from input,
    // and these are compared to see if the newer element is an orphan.
    private T previousInput;
    // nextOutput is the next item to be returned by next. It comes from input or queue.
    private T nextOutput;
    // done becomes true when there is no more input and no rows in queue.
    private boolean done;
}
