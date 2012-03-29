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

package com.akiban.qp.persistitadapter.sort;

import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.persistit.exception.PersistitException;

import java.util.ArrayList;
import java.util.List;

abstract class SortCursorMixedOrder extends SortCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        exchange.clear();
        scanStates.clear();
        try {
            initializeScanStates();
            repositionExchange(0);
            justOpened = true;
        } catch (PersistitException e) {
            close();
            adapter.handlePersistitException(e);
        }
    }

    @Override
    public Row next()
    {
        Row next = null;
        try {
            if (justOpened) {
                // Exchange is already positioned
                justOpened = false;
            } else {
                advance(scanStates.size() - 1);
            }
            if (more) {
                next = row();
            } else {
                close();
            }
        } catch (PersistitException e) {
            close();
            adapter.handlePersistitException(e);
        }
        return next;
    }

    // SortCursorMixedOrder interface

    public static SortCursorMixedOrder create(QueryContext context,
                                              IterationHelper iterationHelper,
                                              IndexKeyRange keyRange,
                                              API.Ordering ordering)
    {
        return
            // keyRange == null occurs when Sorter is used, (to sort an arbitrary input stream). There is no
            // IndexRowType in that case, so an IndexKeyRange can't be created.
            keyRange == null || keyRange.unbounded()
            ? new SortCursorMixedOrderUnbounded(context, iterationHelper, keyRange, ordering)
            : new SortCursorMixedOrderBounded(context, iterationHelper, keyRange, ordering);
    }

    public abstract void initializeScanStates() throws PersistitException;

    // For use by subclasses

    protected SortCursorMixedOrder(QueryContext context,
                                   IterationHelper iterationHelper,
                                   IndexKeyRange keyRange,
                                   API.Ordering ordering)
    {
        super(context, iterationHelper);
        this.keyRange = keyRange;
        this.ordering = ordering;
        keyColumns =
            keyRange == null
            ? ordering.sortColumns()
            : keyRange.indexRowType().index().indexRowComposition().getLength();
    }

    // For use by this package

    IndexKeyRange keyRange()
    {
        return keyRange;
    }

    API.Ordering ordering()
    {
        return ordering;
    }

    // For use by subclasses

    protected int orderingColumns()
    {
        return ordering.sortColumns();
    }

    protected int boundColumns()
    {
        return keyRange.boundColumns();
    }

    protected int keyColumns()
    {
        return keyColumns;
    }

    // For use by this class

    private void advance(int field) throws PersistitException
    {
        MixedOrderScanState scanState = scanStates.get(field);
        if (scanState.advance()) {
            if (field < scanStates.size() - 1) {
                repositionExchange(field + 1);
            }
        } else {
            exchange.cut();
            if (field == 0) {
                more = false;
            } else {
                advance(field - 1);
            }
        }
    }

    private void repositionExchange(int field) throws PersistitException
    {
        more = true;
        for (int f = field; more && f < scanStates.size(); f++) {
            more = scanStates.get(f).startScan();
        }
    }

    // Object state

    protected final IndexKeyRange keyRange;
    protected final API.Ordering ordering;
    protected final List<MixedOrderScanState> scanStates = new ArrayList<MixedOrderScanState>();
    private final int keyColumns; // Number of columns in the key. keyFields >= orderingColumns.
    private boolean more;
    private boolean justOpened;
}
