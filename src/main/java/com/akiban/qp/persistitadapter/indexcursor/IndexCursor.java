/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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

    static final PointTap SORT_TRAVERSE = Tap.createCount("traverse_sort");
}
