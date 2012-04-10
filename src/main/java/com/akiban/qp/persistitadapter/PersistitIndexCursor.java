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
import com.akiban.qp.operator.*;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.persistitadapter.sort.IterationHelper;
import com.akiban.qp.persistitadapter.sort.SortCursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.util.ShareHolder;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

class PersistitIndexCursor implements Cursor
{
    // Cursor interface

    @Override
    public void open()
    {
        // CursorLifecycle.checkIdle(this);
        sortCursor = SortCursor.create(context, keyRange, ordering, new IndexScanIterationHelper());
        sortCursor.open();
        idle = false;
    }

    @Override
    public Row next()
    {
        Row next;
        try {
            // CursorLifecycle.checkIdleOrActive(this);
            boolean needAnother;
            do {
                if ((next = sortCursor.next()) != null) {
                    needAnother = !(isTableIndex ||
                                    // The value of a group index is the depth at which it's defined, as an int.
                                    // See OperatorStoreGIHandler, search for "Description of group index entry values"
                                    // TODO: It would be better to limit the use of exchange to SortCursor, which means
                                    // TODO: that the selector would need to be pushed down. Alternatively, the exchange's
                                    // TODO: value could be made available here in PersistitIndexRow.
                                    selector.matchesAll() ||
                                    (exchange.getKey().getEncodedSize() > 0 &&
                                     selector.matches(exchange.fetch().getValue().getLong())));
                } else {
                    close();
                    needAnother = false;
                }
            } while (needAnother);
        } catch (PersistitException e) {
            adapter.handlePersistitException(e);
            throw new AssertionError();
        }
        assert (next == null) == idle;
        return next;
    }

    @Override
    public void close()
    {
        // CursorLifecycle.checkIdleOrActive(this);
        if (!idle) {
            row.release();
            idle = true;
        }
    }

    @Override
    public void destroy()
    {
        adapter.returnExchange(exchange);
        exchange = null;
        sortCursor = null;
    }

    @Override
    public boolean isIdle()
    {
        return exchange != null && idle;
    }

    @Override
    public boolean isActive()
    {
        return exchange != null && !idle;
    }

    @Override
    public boolean isDestroyed()
    {
        return exchange == null;
    }

    // For use by this package

    PersistitIndexCursor(QueryContext context,
                         IndexRowType indexRowType,
                         IndexKeyRange keyRange,
                         API.Ordering ordering,
                         IndexScanSelector selector)
        throws PersistitException
    {
        this.keyRange = keyRange;
        this.adapter = (PersistitAdapter)context.getStore();
        this.ordering = ordering;
        this.context = context;
        this.indexRowType = indexRowType;
        this.row = new ShareHolder<PersistitIndexRow>(adapter.newIndexRow(indexRowType));
        this.isTableIndex = indexRowType.index().isTableIndex();
        this.selector = selector;
        this.exchange = adapter.takeExchange(indexRowType.index());
        this.idle = true;
    }

    // For use by this class

    private ShareHolder<PersistitIndexRow> unsharedRow() throws PersistitException
    {
        if (row.isEmpty() || row.isShared()) {
            row.hold(adapter.newIndexRow(indexRowType));
        }
        return row;
    }

    // Object state

    private final QueryContext context;
    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private final ShareHolder<PersistitIndexRow> row;
    private final IndexKeyRange keyRange;
    private final API.Ordering ordering;
    private final boolean isTableIndex;
    private IndexScanSelector selector;
    private Exchange exchange;
    private SortCursor sortCursor;
    private boolean idle;

    // Inner classes

    private class IndexScanIterationHelper implements IterationHelper
    {
        @Override
        public Row row() throws PersistitException
        {
            unsharedRow().get().copyFromExchange(exchange);
            return row.get();
        }

        @Override
        public void close()
        {
            PersistitIndexCursor.this.close();
        }

        @Override
        public Exchange exchange()
        {
            return PersistitIndexCursor.this.exchange;
        }
    }
}
