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

package com.foundationdb.server.spatial;

import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.CursorBase;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.geophile.z.space.SpaceImpl;

// Allows cursor to be reset to the beginning, as long as next hasn't been called at least
// CACHE_SIZE times. Useful for wrapping IndexCursorUnidirectional's for use by geophile with
// CACHE_SIZE = 1. Geophile may do a random access, then probe the same key as an ancestor
// (retrieving one record), and then probe it again to prepare for sequential accesses.

public class CachingCursor implements BindingsAwareCursor
{
    // CursorBase interface

    @Override
    public void open()
    {
        input.open();
    }

    @Override
    public Row next()
    {
        Row next;
        if (cachePosition < cachePositionsFilled) {
            next = cachedRecord(cachePosition++);
        } else {
            next = input.next();
            if (cachePosition < CACHE_SIZE) {
                recordCache[cachePosition++] = next;
                cachePositionsFilled = cachePosition;
            } else if (cachePosition == CACHE_SIZE) {
                resettable = false;
            }
        }
        return next;
    }

    public void close()
    {
        input.close();
    }

    @Override
    public boolean isIdle()
    {
        return input.isIdle();
    }

    @Override
    public boolean isActive()
    {
        return input.isActive();
    }

    @Override
    public boolean isClosed()
    {
        return input.isClosed();
    }

    @Override
    public void setIdle()
    {
        input.setIdle();
    }

    // BindingsAwareCursor interface

    @Override
    public void rebind(QueryBindings bindings)
    {
        assert input instanceof BindingsAwareCursor;
        ((BindingsAwareCursor) input).rebind(bindings);
    }


    // RowOrientedCursorBase interface

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        if (row.z() != z) {
            throw new NotResettableException(row.z());
        }
        if (!resettable) {
            throw new NotResettableException();
        }
        cachePosition = 0;
    }

    // CachingCursor interface

    public CachingCursor(long z, CursorBase<? extends Row> indexCursor)
    {
        // input is actually an IndexCursorUnidirectional. But this class only uses next(),
        // and declaring CursorBase greatly simplifies testing.
        this.z = z;
        this.input = indexCursor;
    }

    // For use by this class

    Row cachedRecord(int i)
    {
        return (Row) recordCache[i];
    }

    // Class state

    private static final int CACHE_SIZE = 1;

    // Object state

    private final long z;
    private final CursorBase<? extends Row> input;
    private final Object[] recordCache = new Object[CACHE_SIZE];
    private int cachePosition = 0;
    private int cachePositionsFilled = 0;
    private boolean resettable = true;

    // Inner classes

    public static class NotResettableException extends RuntimeException
    {
        public NotResettableException()
        {}

        public NotResettableException(long z)
        {
            super(SpaceImpl.formatZ(z));
        }
    }
}
