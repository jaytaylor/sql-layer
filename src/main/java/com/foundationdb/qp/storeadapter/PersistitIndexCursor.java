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
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.storeadapter.indexcursor.IndexCursor;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.server.api.dml.ColumnSelector;

class PersistitIndexCursor implements BindingsAwareCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        CursorLifecycle.checkIdle(this);
        indexCursor.open(); // Does iterationHelper.openIteration, where iterationHelper = rowState
        idle = false;
    }

    @Override
    public Row next()
    {
        PersistitIndexRow next;
        CursorLifecycle.checkIdleOrActive(this);
        boolean needAnother;
        do {
            if ((next = (PersistitIndexRow) indexCursor.next()) != null) {
                needAnother = !(isTableIndex ||
                                selector.matchesAll() ||
                                !next.keyEmpty() && selector.matches(next.tableBitmap()));
            } else {
                close();
                needAnother = false;
            }
        } while (needAnother);
        assert (next == null) == idle : "next: " + next + " vs idle " + idle;
        return next;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        Index index = indexRowType.index();
        assert !index.isSpatial(); // Jump not yet supported for spatial indexes
        rowState.openIteration();
        idle = false;
        indexCursor.jump(row, columnSelector);
    }

    @Override
    public void close()
    {
        CursorLifecycle.checkIdleOrActive(this);
        indexCursor.close(); // IndexCursor.close() closes the rowState (IndexCursor.iterationHelper)
        idle = true;
    }

    @Override
    public void destroy()
    {
        destroyed = true;
        indexCursor.destroy();
    }

    @Override
    public boolean isIdle()
    {
        return !destroyed && idle;
    }

    @Override
    public boolean isActive()
    {
        return !destroyed && !idle;
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    @Override
    public void rebind(QueryBindings bindings)
    {
        indexCursor.rebind(bindings);
    }

    // For use by this package

    PersistitIndexCursor(QueryContext context,
                         IndexRowType indexRowType,
                         IndexKeyRange keyRange,
                         API.Ordering ordering,
                         IndexScanSelector selector,
                         boolean openAllSubCursors)
    {
        this.keyRange = keyRange;
        this.ordering = ordering;
        this.context = context;
        this.indexRowType = indexRowType;
        this.isTableIndex = indexRowType.index().isTableIndex();
        this.selector = selector;
        this.idle = true;
        this.rowState = context.getStore().createIterationHelper(indexRowType);
        this.indexCursor = IndexCursor.create(context, keyRange, ordering, rowState,  openAllSubCursors);
    }

    // For use by this class

    // Object state

    private final QueryContext context;
    private final IndexRowType indexRowType;
    private final IndexKeyRange keyRange;
    private final API.Ordering ordering;
    private final boolean isTableIndex;
    private final IterationHelper rowState;
    private IndexCursor indexCursor;
    private final IndexScanSelector selector;
    private boolean idle;
    private boolean destroyed = false;
}
