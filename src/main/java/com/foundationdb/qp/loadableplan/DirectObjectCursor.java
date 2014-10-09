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

package com.foundationdb.qp.loadableplan;

import com.foundationdb.qp.operator.CursorBase;
import java.util.List;

/** A cursor that returns column values directly.
 * Return columns from <code>next</code>. If an empty list is
 * returned, any buffered rows will be flushed and <code>next</code>
 * will be called again. If <code>null</code> is returned, the cursor
 * is exhausted and will be closed.
 */
public abstract class DirectObjectCursor implements CursorBase<List<?>>
{
    // These cursors are used outside of execution plans. These methods should not be called.

    @Override
    public void close()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean isIdle()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean isActive()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean isClosed()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }
    @Override
    public void setIdle()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
