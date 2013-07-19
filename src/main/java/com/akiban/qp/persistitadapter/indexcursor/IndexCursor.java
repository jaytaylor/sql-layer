/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.BindingsAwareCursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
import com.persistit.Key;
import com.persistit.Key.Direction;
import com.persistit.exception.PersistitException;

public abstract class IndexCursor implements BindingsAwareCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        CursorLifecycle.checkIdle(this);
        iterationHelper.openIteration();
        idle = false;
    }

    @Override
    public Row next()
    {
        CursorLifecycle.checkIdleOrActive(this);
        return null;
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
        iterationHelper.closeIteration();
        idle = true;
    }

    @Override
    public void destroy()
    {
        destroyed = true;
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
                                     boolean usePValues,
                                     boolean openAllSubCursors)
    {
        SortKeyAdapter<?, ?> adapter =
            usePValues
            ? PValueSortKeyAdapter.INSTANCE
            : OldExpressionsSortKeyAdapter.INSTANCE;
        return
            keyRange != null && keyRange.spatial()
            ? keyRange.hi() == null
                ? IndexCursorSpatial_NearPoint.create(context, iterationHelper, keyRange)
                : IndexCursorSpatial_InBox.create(context, iterationHelper, keyRange, openAllSubCursors)
            : ordering.allAscending() || ordering.allDescending()
                ? (keyRange != null && keyRange.lexicographic()
                    ? IndexCursorUnidirectionalLexicographic.create(context, iterationHelper, keyRange, ordering, adapter)
                    : IndexCursorUnidirectional.create(context, iterationHelper, keyRange, ordering, adapter))
                : IndexCursorMixedOrder.create(context, iterationHelper, keyRange, ordering, adapter);
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
    private boolean idle = true;
    private boolean destroyed = false;

    static final PointTap INDEX_TRAVERSE = Tap.createCount("index_traverse");
}
