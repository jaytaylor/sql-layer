/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.util;

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
