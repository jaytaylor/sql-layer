/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

A Cursor is always in one of three states:

- IDLE: The cursor is not currently involved in a scan. The cursor is in this state when one
      of the following is true:
      - open() has never been called.
      - The most recent method invocation was to next(), which returned null.
      - The most recent method invocation was to close().

- ACTIVE: The cursor is currently involved in a scan. The cursor is in this state when one
      of the following is true:
      - The most recent method invocation was to open().
      - The most recent method invocation was to next(), which returned a non-null value.

- DESTROYED: The cursor has been destroyed. Invocations of open, next, or close on such a cursor
      will raise an exception. The only way to get into this state is to call destroy().

The Cursor lifecycle is as follows:

   next/jump == null  next/jump != null
        +-+               +-+
        | |               | |
        | V     open      | V     destroy
    --> IDLE ----------> ACTIVE ----------> DESTROYED
         |   <----------                        ^
         |      next/jump = null,                    |
         |      close                           |
         |                                      |
         +--------------------------------------+
                         destroy
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
     * Destroys the cursor. No further operations on the cursor are permitted.
     */
    void destroy();

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
     * Indicates whether the cursor is in the DESTROYED state.
     * @return true iff the cursor is DESTROYED.
     */
    boolean isDestroyed();
}
