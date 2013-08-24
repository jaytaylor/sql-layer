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

package com.foundationdb.qp.operator;

/**
 * A single {@link QueryBindings} stream.
 */
public class SingletonQueryBindingsCursor implements QueryBindingsCursor
{
    enum State { CLOSED, PENDING, EXHAUSTED };
    private QueryBindings bindings;
    private State state;

    public SingletonQueryBindingsCursor(QueryBindings bindings) {
        reset(bindings);
    }

    @Override
    public void openBindings() {
        state = State.PENDING;
    }

    @Override
    public QueryBindings nextBindings() {
        switch (state) {
        case CLOSED:
            throw new IllegalStateException("Bindings cursor not open");
        case PENDING:
            state = State.EXHAUSTED;
            return bindings;
        case EXHAUSTED:
        default:
            return null;
        }
    }

    @Override
    public void closeBindings() {
        state = State.CLOSED;
    }

    @Override
    public void cancelBindings(QueryBindings ancestor) {
        if ((state == State.PENDING) && bindings.isAncestor(ancestor)) {
            state = State.EXHAUSTED;
        }
    }

    public void reset(QueryBindings bindings) {
        this.bindings = bindings;
        this.state = State.CLOSED;
    }
}
