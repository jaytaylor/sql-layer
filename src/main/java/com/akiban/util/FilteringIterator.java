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
import java.util.NoSuchElementException;

/**
 * Iterator which filters out another iterator. The filtering is done via an abstract method, which lets us
 * avoid unnecessary allocations (of a seperate predicate object). This method comes in two flavors, immutable
 * and mutable; for obvious reasons, mutability is only supported if the delegate iterator is also mutable.
 * @param <T> the type to iterate
 */
public abstract class FilteringIterator<T> implements Iterator<T> {

    private static enum State {
        /**
         * This iterator is freshly created. It hasn't asked its delegate anything.
         */
        FRESH,
        /**
         * An item has been requested of the delegate, and it's pending to be delivered to this iterator's user.
         */
        NEXT_PENDING,
        /**
         * An item has been requested of the delegate, but it's already been delivered to this iterator's user.
         */
        NEXT_RETRIEVED,
        /**
         * There are no items left to deliver.
         */
        DONE
    }

    private final Iterator<T> delegate;
    private final boolean isMutable;
    private T next;
    private State state;

    public FilteringIterator(Iterator<T> delegate, boolean isMutable) {
        this.delegate = delegate;
        this.isMutable = isMutable;
        this.state = State.FRESH;

    }

    protected abstract boolean allow(T item);

    @Override
    public boolean hasNext() {
        switch (state) {
            case NEXT_PENDING:
                return true;
            case FRESH:
            case NEXT_RETRIEVED:
                advance();
                assert (state == State.NEXT_PENDING) || (state == State.DONE) : state;
                return state == State.NEXT_PENDING;
            case DONE:
                return false;
            default:
                throw new AssertionError("FilteringIterator has unknown internal state: " + state);
        }
    }

    @Override
    public T next() {
        if (state == State.FRESH || state == State.NEXT_RETRIEVED) {
            advance();
        }
        if (state == State.DONE) {
            throw new NoSuchElementException();
        }
        assert state == State.NEXT_PENDING : state;
        state = State.NEXT_RETRIEVED;
        return next;
    }

    @Override
    public void remove() {
        if (!isMutable) {
            throw new UnsupportedOperationException();
        }
        if (state != State.NEXT_RETRIEVED) {
            throw new IllegalStateException(state.name());
        }
        delegate.remove();
    }

    /**
     * Advances the delegate until we get an element which passes the filter,
     *
     * Coming into this method, this iterator's state must be FRESH or NEXT_RETRIEVED. When this method completes
     * successfully, the state will be DONE or NEXT_PENDING.
     *
     * At the end of this method, this FilteringIterator's state will be either DONE or NEXT_KNOWN. This method
     * should only be invoked when the state is NEEDS_NEXT. If the state of this iterator is NEXT_KNOWN on return,
     * the "next" field will point to the correct item.
     */
    private void advance() {
        assert (state == State.FRESH) || (state == State.NEXT_RETRIEVED) : state;
        while (state != State.NEXT_PENDING) {
            if (!delegate.hasNext()) {
                state = State.DONE;
                return;
            } else {
                T item = delegate.next();
                if (allow(item)) {
                    next = item;
                    state = State.NEXT_PENDING;
                }
            }
        }
    }
}
