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
 * A stream of {@link QueryBindings}.
 * 
 * The QueryBindingsCursor has three (implied) states: 
 * 
 * - CLOSED : when created, or after closeBindings() is called
 * - PENDING: after openBindings() is called, or nextBindings() is called
 * - EXHAUSTED: after nextBindings() is called and no new bindings are available
 * 
 * cancelBindings() may be called on either a PENDING or EXHAUSTED cursor
 *  - It skips forward in the set of bindings to the calling point. 
 *  - It leaves the cursor in an unknown state, you must call nextBindings() 
 *    to get the next set of bindings. 
 * 
 * In the cases where this cursor is interleaved with a @see RowCursor, 
 * the asserted assumption is calling nextBindings() or cancelBindings() 
 * must be done only with the associated cursor in a CLOSED state. 
 * 
 * The order of calls should be: 
 * 
 * bCursor,openBindings();
 * bCursor.nextBindings();
 * rCursor.open();
 * rCursor.next();
 * rCursor.close();
 * bCursor.cancelBindings(binding)
 * bCursor.nextBindings();
 * rCursor.open();
 * rCursor.next();
 * rCursor.close();
 * bCursor.closeBindings();
 * 
 */
public interface QueryBindingsCursor
{
    /** Open stream of bindings. */
    public void openBindings();

    /** Get (and make current for <code>open</code>) the next set of bindings.
     *  returns null if no new bindings
     */
    public QueryBindings nextBindings();

    /** Close stream of bindings. */
    public void closeBindings();

    /** Advance stream past given bindings. */
    public void cancelBindings(QueryBindings bindings);
}
