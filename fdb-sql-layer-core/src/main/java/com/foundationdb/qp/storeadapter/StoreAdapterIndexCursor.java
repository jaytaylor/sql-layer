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

package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.*;
import com.foundationdb.qp.storeadapter.indexcursor.IndexCursor;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.server.api.dml.ColumnSelector;

/** Wraps an {@link IndexCursor}, providing {@link #jump} and {@link IndexScanSelector} support. */
class StoreAdapterIndexCursor extends RowCursorImpl implements BindingsAwareCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
        indexCursor.open(); // Does iterationHelper.openIteration, where iterationHelper = rowState
    }

    @Override
    public Row next()
    {
        IndexRow next;
        CursorLifecycle.checkIdleOrActive(this);
        boolean needAnother;
        do {
            if ((next = (IndexRow) indexCursor.next()) != null) {
                needAnother = !(isTableIndex ||
                                selector.matchesAll() ||
                                !next.keyEmpty() && selector.matches(next.tableBitmap()));
            } else {
                setIdle();
                needAnother = false;
            }
        } while (needAnother);
        assert (next == null) == isIdle() : "next: " + next + " vs idle " + isIdle();
        return next;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        CursorLifecycle.checkIdleOrActive(this);
        Index index = indexRowType.index();
        assert !index.isSpatial(); // Jump not yet supported for spatial indexes
        // TODO: Couldn't IndexCursor handle this?
        rowState.openIteration();
        indexCursor.jump(row, columnSelector);
        state = CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public void close()
    {
        try {
            indexCursor.close(); // IndexCursor.close() closes the rowState (IndexCursor.iterationHelper)
        } finally {
            super.close();
        }
    }

    @Override
    public void rebind(QueryBindings bindings)
    {
        indexCursor.rebind(bindings);
    }

    // For use by this package

    StoreAdapterIndexCursor(QueryContext context,
                            IndexRowType indexRowType,
                            IndexKeyRange keyRange,
                            API.Ordering ordering,
                            IndexScanSelector selector,
                            boolean openAllSubCursors)
    {
        this.indexRowType = indexRowType;
        this.isTableIndex = indexRowType.index().isTableIndex();
        this.selector = selector;
        this.rowState = context.getStore().createIterationHelper(indexRowType);
        this.indexCursor = IndexCursor.create(context, keyRange, ordering, rowState,  openAllSubCursors);
    }

    // For use by this class

    // Object state

    private final IndexRowType indexRowType;
    private final boolean isTableIndex;
    private final IterationHelper rowState;
    private IndexCursor indexCursor;
    private final IndexScanSelector selector;
}
