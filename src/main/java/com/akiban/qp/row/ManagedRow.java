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

package com.akiban.qp.row;

// ManagedRow and RowHolder implement a reference-counting scheme for Rows. Operators should hold onto
// ManagedRows using a RowHolder. Assignments to RowHolder (using RowHolder.set) cause reference counting
// to be done, via ManagedRow.share() and release(). isShared() is used by data sources (a btree cursor)
// when the row filled in by the cursor is about to be changed. If isShared() is true, then the cursor
// allocates a new row and writes into it, otherwise the existing row is used. (isShared() is true iff the reference
// count is > 1. If the reference count is = 1, then presumably the reference is from the btree cursor itself.)
//
// This implementation is NOT threadsafe. It assumes that all access to a ManagedRow is within one thread.
// When a row is passed from one operator to another, there are times when the reference count drops temporarily.
// For example, when Flatten_HKeyOrdered removes a pending row and passes it to
// SingleRowCachingCursor.outputRow(ManagedRow), the ManagedRow is removed from a RowHolder in the pending queue
// before it is assigned to another RowHolder in the SingleRowCachingCursor. As long as we're in one thread,
// the cursor can't be writing new data into the row while this is happening.

public interface ManagedRow extends Row
{
    void share();
    boolean isShared();
    void release();
}
