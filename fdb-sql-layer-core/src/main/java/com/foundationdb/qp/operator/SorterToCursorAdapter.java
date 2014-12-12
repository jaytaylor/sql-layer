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
import com.foundationdb.util.tap.InOutTap;

/**
 * Cursors are reusable but Sorters are not.
 * This class creates a new Sorter each time a new cursor scan is started.
 */
class SorterToCursorAdapter extends RowCursorImpl
{
    // RowCursor interface

    @Override
    public void open()
    {
        super.open();
        try {
            sorter = adapter.createSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
            cursor = sorter.sort();
        } finally {
            input.close();
        }
        cursor.open();
        state = CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public Row next()
    {
        CursorLifecycle.checkIdleOrActive(this);
        return cursor == null ? null : cursor.next();
    }

    @Override
    public void close()
    {
        try {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
            if (sorter != null) {
                sorter.close();
                sorter = null;
            }
        } finally {
            super.close();
        }
    }

    // SorterToCursorAdapter interface

    /**
     * input must be open before calling open() on this cursor.
     */
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
}
