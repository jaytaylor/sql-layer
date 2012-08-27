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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.*;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.persistitadapter.indexcursor.IndexCursor;
import com.akiban.qp.persistitadapter.indexcursor.IterationHelper;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.persistit.exception.PersistitException;

class PersistitIndexCursor implements Cursor
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
        assert (next == null) == idle;
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

    // For use by this package

    PersistitIndexCursor(QueryContext context,
                         IndexRowType indexRowType,
                         IndexKeyRange keyRange,
                         API.Ordering ordering,
                         IndexScanSelector selector,
                         boolean usePValues)
        throws PersistitException
    {
        this.keyRange = keyRange;
        this.ordering = ordering;
        this.context = context;
        this.indexRowType = indexRowType;
        this.isTableIndex = indexRowType.index().isTableIndex();
        this.usePValues = usePValues;
        this.selector = selector;
        this.idle = true;
        this.rowState = new IndexScanRowState((PersistitAdapter)context.getStore(), indexRowType);
        this.indexCursor = IndexCursor.create(context, keyRange, ordering, rowState, usePValues);
    }

    // For use by this class

    // Object state

    private final QueryContext context;
    private final IndexRowType indexRowType;
    private final IndexKeyRange keyRange;
    private final API.Ordering ordering;
    private final boolean isTableIndex;
    private final boolean usePValues;
    private final IterationHelper rowState;
    private IndexCursor indexCursor;
    private final IndexScanSelector selector;
    private boolean idle;
    private boolean destroyed = false;
    }
