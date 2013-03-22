
package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public abstract class IndexCursor implements Cursor
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

    // For use by subclasses

    protected Exchange exchange()
    {
        return iterationHelper.exchange();
    }

    // IndexCursor interface

    public static IndexCursor create(QueryContext context,
                                     IndexKeyRange keyRange,
                                     API.Ordering ordering,
                                     IterationHelper iterationHelper,
                                     boolean usePValues)
    {
        SortKeyAdapter<?, ?> adapter =
            usePValues
            ? PValueSortKeyAdapter.INSTANCE
            : OldExpressionsSortKeyAdapter.INSTANCE;
        return
            keyRange != null && keyRange.spatial()
            ? keyRange.hi() == null
                ? IndexCursorSpatial_NearPoint.create(context, iterationHelper, keyRange)
                : IndexCursorSpatial_InBox.create(context, iterationHelper, keyRange)
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
        this.adapter = (PersistitAdapter)context.getStore();
        this.iterationHelper = iterationHelper;
    }

    protected Row row() throws PersistitException
    {
        return iterationHelper.row();
    }

    // Object state

    protected final QueryContext context;
    protected final PersistitAdapter adapter;
    protected final IterationHelper iterationHelper;
    private boolean idle = true;
    private boolean destroyed = false;

    static final PointTap INDEX_TRAVERSE = Tap.createCount("index_traverse");
}
