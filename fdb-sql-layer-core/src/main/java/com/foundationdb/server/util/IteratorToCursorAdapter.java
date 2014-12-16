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

package com.foundationdb.server.util;

import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;

import java.util.Iterator;

public class IteratorToCursorAdapter implements BindingsAwareCursor
{
    // CursorBase interface

    @Override
    public void open()
    {
        state = CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public Row next()
    {
        Row next;
        if (iterator.hasNext()) {
            next = (Row) iterator.next();
        } else {
            next = null;
            state = CursorLifecycle.CursorState.IDLE;
        }
        return next;
    }

    @Override
    public void close()
    {
        state = CursorLifecycle.CursorState.CLOSED;
    }

    @Override
    public boolean isIdle()
    {
        return state == CursorLifecycle.CursorState.IDLE;
    }

    @Override
    public boolean isActive()
    {
        return state == CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public boolean isClosed()
    {
        return state == CursorLifecycle.CursorState.CLOSED;
    }

    @Override
    public void setIdle()
    {
        state = CursorLifecycle.CursorState.IDLE;
    }

    // RowOrientedCursorBase interface

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        throw new UnsupportedOperationException();
    }

    // BindingsAwareCursor interface

    @Override
    public void rebind(QueryBindings bindings)
    {
        throw new UnsupportedOperationException();
    }

    // IteratorToCursorAdapter interface

    public IteratorToCursorAdapter(Iterator iterator)
    {
        this.iterator = iterator;
    }

    // Object state

    private final Iterator iterator;
    private CursorLifecycle.CursorState state = CursorLifecycle.CursorState.CLOSED;
}
