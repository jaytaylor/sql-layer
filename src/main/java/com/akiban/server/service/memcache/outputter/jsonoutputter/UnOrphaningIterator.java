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
        done = false;
        advanceIfNecessary();
    }

    // For use by this class

    private void advanceIfNecessary()
    {
        if (!done && nextOutput == null) {
            nextOutput = queue.poll();
            if (nextOutput == null && input.hasNext()) {
                nextOutput = input.next();
                genealogist.fillInMissing(previousInput, nextOutput, queue);
                // Get ready for the next time we have to check whether an item from input is an orphan.
                previousInput = nextOutput;
                if (!queue.isEmpty()) {
                    // nextOutput is an orphan. We know this because fillInMissing just filled in some descendents.
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
    // to determine if the newer element is an orphan.
    private T previousInput;
    // nextOutput is the next item to be returned by next. It comes from input or queue.
    private T nextOutput;
    // done becomes true when there is no more input and no rows in queue.
    private boolean done;
}
