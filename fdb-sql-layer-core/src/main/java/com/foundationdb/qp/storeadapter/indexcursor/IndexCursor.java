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

package com.foundationdb.qp.storeadapter.indexcursor;

import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;
import com.persistit.Key;
import com.persistit.Key.Direction;

public abstract class IndexCursor extends RowCursorImpl implements BindingsAwareCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
        iterationHelper.openIteration();
    }

    @Override
    public Row next()
    {
        CursorLifecycle.checkIdleOrActive(this);
        return null;
    }

    @Override
    public void close()
    {
        iterationHelper.closeIteration();
        super.close();
    }

    @Override
    public void rebind(QueryBindings bindings)
    {
        this.bindings = bindings;
    }
    
    // For use by subclasses

    protected boolean nextInternal(boolean deep)
    {
        return iterationHelper.next(deep);
    }

    protected boolean prevInternal(boolean deep)
    {
        return iterationHelper.prev(deep);
    }

    protected boolean traverse(Direction dir, boolean deep)
    {
        return iterationHelper.traverse(dir, deep);
    }

    protected void clear()
    {
        iterationHelper.clear();
    }

    protected Key key()
    {
        return iterationHelper.key();
    }

    // IndexCursor interface

    public static IndexCursor create(QueryContext context,
                                     IndexKeyRange keyRange,
                                     API.Ordering ordering,
                                     IterationHelper iterationHelper,
                                     boolean openAllSubCursors)
    {
        IndexCursor indexCursor;
        if (keyRange != null && keyRange.spatialCoordsIndex()) {
            if (keyRange.hi() == null) {
                indexCursor = IndexCursorSpatial_NearPoint.create(context, iterationHelper, keyRange);
            } else {
                indexCursor = IndexCursorSpatial_InBox.create(context, iterationHelper, keyRange, openAllSubCursors);
            }
        } else if (keyRange != null && keyRange.spatialObjectIndex()) {
            indexCursor = IndexCursorSpatial_InBox.create(context, iterationHelper, keyRange, openAllSubCursors);
        } else {
            SortKeyAdapter<?, ?> adapter = ValueSortKeyAdapter.INSTANCE;
            if(ordering.allAscending() || ordering.allDescending()) {
                indexCursor = IndexCursorUnidirectional.create(context, iterationHelper, keyRange, ordering, adapter);
            } else {
                indexCursor = IndexCursorMixedOrder.create(context, iterationHelper, keyRange, ordering, adapter);
            }
        }
        return indexCursor;
    }

    // For use by subclasses

    protected IndexCursor(QueryContext context, IterationHelper iterationHelper)
    {
        this.context = context;
        this.adapter = context.getStore();
        this.iterationHelper = iterationHelper;
    }

    protected Row row()
    {
        return iterationHelper.row();
    }

    // Object state

    protected final QueryContext context;
    protected final StoreAdapter adapter;
    protected final IterationHelper iterationHelper;
    protected QueryBindings bindings;

    static final PointTap INDEX_TRAVERSE = Tap.createCount("traverse: index cursor");
}
