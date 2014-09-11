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

/*

A Cursor is used to scan a sequence of rows resulting from the execution of an operator.
The same cursor may be used for multiple executions of the operator, possibly with different bindings.

A Cursor is always in one of four states:

- CLOSED: The cursor is not currently involved in a scan. Invocations of next() on 
        the cursor will raise an exception. The cursor is in this state when one of
        the following is true:
        - open() has never been called.
        - The most recent method invocation was to close().

- ACTIVE: The cursor is currently involved in a scan. The cursor is in this state when one
      of the following is true:
      - The most recent method invocation was to open().
      - The most recent method invocation was to next(), which returned a non-null value.

- IDLE: The cursor is currently involved in a scan. The cursor is in this state when one
      of the following is true:
      - The most recent method invocation was to next(), which returned null.

The Cursor lifecycle is as follows:

                close                  
        +-------------------+          
        |                   |          
        v       open        +          
        CLOSED +-----> ACTIVE <-------+
        ^                   +         |
        |close      next    | next    |
        |           == null | != null |
        +-+ IDLE <----------v---------+
 */

public interface CursorBase<T>
{
    /**
     * Starts a cursor scan.
     */
    void open();

    /**
     * Advances to and returns the next object.
     * @return The next object, or <code>null</code> if at the end.
     */
    T next();

    /**
     * Terminates the current cursor scan.
     */
    void close();

    /**
     * Indicates whether the cursor is in the IDLE state.
     * @return true iff the cursor is IDLE.
     */
    boolean isIdle();

    /**
     * Indicates whether the cursor is in the ACTIVE state.
     * @return true iff the cursor is ACTIVE.
     */
    boolean isActive();

    /**
     * Indicates whether the cursor is in the CLOSED state.
     * @return true iff the cursor is CLOSED.
     */
    boolean isClosed();
}
