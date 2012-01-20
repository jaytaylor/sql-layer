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

package com.akiban.qp.persistitadapter.sort;

import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.akiban.util.Tap;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public abstract class SortCursor implements Cursor
{
    // Cursor interface

    @Override
    public final void close()
    {
        iterationHelper.close();
    }

    // SortCursor interface

    public static SortCursor create(PersistitAdapter adapter,
                                    IndexKeyRange keyRange,
                                    API.Ordering ordering,
                                    IterationHelper iterationHelper)
    {
        return
            ordering.allAscending() || ordering.allDescending()
            ? (keyRange != null && keyRange.lexicographic()
               ? SortCursorUnidirectionalLexicographic.create(adapter, iterationHelper, keyRange, ordering)
               : SortCursorUnidirectional.create(adapter, iterationHelper, keyRange, ordering))
            : SortCursorMixedOrder.create(adapter, iterationHelper, keyRange, ordering);
    }

    // For use by subclasses

    protected SortCursor(PersistitAdapter adapter, IterationHelper iterationHelper)
    {
        this.adapter = adapter;
        this.iterationHelper = iterationHelper;
        this.exchange = iterationHelper.exchange();
    }

    protected Row row() throws PersistitException
    {
        return iterationHelper.row();
    }

    // Object state

    protected final PersistitAdapter adapter;
    protected final Exchange exchange;
    protected final IterationHelper iterationHelper;
    
    static final Tap.PointTap SORT_TRAVERSE = Tap.createCount("traverse_sort");
}
