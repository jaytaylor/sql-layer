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

import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.util.tap.InOutTap;

/**
 * Cursors are reusable but Sorters are not.
 * This class creates a new Sorter each time a new cursor scan is started.
 */
class SorterToCursorAdapter implements RowCursor
{
    // RowCursor interface

    @Override
    public void open()
    {
        CursorLifecycle.checkIdle(this);
        sorter = adapter.createSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
        cursor = sorter.sort();
        input.close();
        cursor.open();
    }

    @Override
    public Row next()
    {
        CursorLifecycle.checkIdleOrActive(this);
        return cursor == null ? null : cursor.next();
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void close()
    {
        CursorLifecycle.checkIdleOrActive(this);
        if (cursor != null) {
            cursor.close();
            // Destroy here because Sorters can only be used once.
            cursor.destroy();
            cursor = null;
        }
        if (sorter != null) {
            sorter.close();
            sorter = null;
        }
    }

    @Override
    public void destroy()
    {
        close();
        destroyed = true;
    }

    @Override
    public boolean isIdle()
    {
        return !destroyed && cursor == null;
    }

    @Override
    public boolean isActive()
    {
        return !destroyed && cursor != null;
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    // SorterToCursorAdapter interface

    public SorterToCursorAdapter(StoreAdapter adapter,
                                 QueryContext context,
                                 QueryBindings bindings,
                                 RowCursor input,
                                 RowType rowType,
                                 API.Ordering ordering,
                                 API.SortOption sortOption,
                                 InOutTap loadTap)
    {
        this.adapter = adapter;
        this.context = context;
        this.bindings = bindings;
        this.input = input;
        this.rowType = rowType;
        this.ordering = ordering;
        this.sortOption = sortOption;
        this.loadTap = loadTap;
    }

    private final StoreAdapter adapter;
    private final QueryContext context;
    private final QueryBindings bindings;
    private final RowCursor input;
    private final RowType rowType;
    private final API.Ordering ordering;
    private final API.SortOption sortOption;
    private final InOutTap loadTap;
    private Sorter sorter;
    private RowCursor cursor;
    private boolean destroyed = false;
}
