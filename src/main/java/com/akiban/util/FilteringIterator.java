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
        NEEDS_NEXT,
        NEXT_KNOWN,
        DONE
    }

    private final Iterator<T> delegate;
    private final boolean isMutable;
    private T next;
    private State state;

    public FilteringIterator(Iterator<T> delegate, boolean isMutable) {
        this.delegate = delegate;
        this.isMutable = isMutable;
        this.state = State.NEEDS_NEXT;

    }

    protected abstract boolean allow(T item);

    @Override
    public boolean hasNext() {
        switch (state) {
            case NEXT_KNOWN:
                return true;
            case NEEDS_NEXT:
                advance();
                assert (state == State.NEXT_KNOWN) || (state == State.DONE) : state;
                return state == State.NEXT_KNOWN;
            case DONE:
                return false;
            default:
                throw new AssertionError("FilteringIterator has unknown internal state: " + state);
        }
    }

    @Override
    public T next() {
        if (state == State.NEEDS_NEXT) {
            advance();
        }
        if (state == State.DONE) {
            throw new NoSuchElementException();
        }
        assert state == State.NEXT_KNOWN : state;
        state = State.NEEDS_NEXT;
        return next;
    }

    @Override
    public void remove() {
        if (!isMutable) {
            throw new UnsupportedOperationException();
        }
        if (state != State.NEXT_KNOWN) {
            throw new IllegalStateException(state.name());
        }
        delegate.remove();
        state = State.NEEDS_NEXT;
    }

    /**
     * Advances the delegate until we get an element which passes the filter,
     * At the end of this method, this FilteringIterator's state will be either DONE or NEXT_KNOWN. This method
     * should only be invoked when the state is NEEDS_NEXT. If the state of this iterator is NEXT_KNOWN on return,
     * the "next" field will point to the correct item.
     */
    private void advance() {
        assert state == State.NEEDS_NEXT : state;
        while (state == State.NEEDS_NEXT) {
            if (!delegate.hasNext()) {
                state = State.DONE;
            } else {
                T item = delegate.next();
                if (allow(item)) {
                    next = item;
                    state = State.NEXT_KNOWN;
                }
            }
        }
    }
}
