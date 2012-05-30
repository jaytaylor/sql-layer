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

import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.types.ValueSource;
import com.persistit.exception.PersistitException;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;

class SortCursorMixedOrder extends SortCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
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
        super.next();
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
        return new SortCursorMixedOrder(context, iterationHelper, keyRange, ordering);
    }

    public void initializeScanStates() throws PersistitException
    {
        int f = 0;
        BoundExpressions lo = keyRange.lo().boundExpressions(context);
        BoundExpressions hi = keyRange.hi().boundExpressions(context);
        while (f < boundColumns) {
            ValueSource loSource = lo.eval(f);
            ValueSource hiSource = hi.eval(f);
            MixedOrderScanStateSingleSegment scanState = new
                MixedOrderScanStateSingleSegment(this, f, f >= orderingColumns() || ordering.ascending(f));
            scanState.setRange(loSource, hiSource);
            scanStates.add(scanState);
            f++;
        }
        while (f < orderingColumns()) {
            MixedOrderScanStateSingleSegment scanState =
                new MixedOrderScanStateSingleSegment(this, f);
            scanStates.add(scanState);
            f++;
        }
        if (f < keyColumns()) {
            MixedOrderScanStateRemainingSegments scanState =
                new MixedOrderScanStateRemainingSegments(this, orderingColumns());
            scanStates.add(scanState);
        }
        if (boundColumns > 0) {
            /*
                * An index restriction is described by an IndexKeyRange which contains
                * two IndexBounds. The IndexBound wraps an index row. The fields of the row that are being restricted are
                * described by the IndexBound's ColumnSelector. The only index restrictions supported specify:
                * a) equality for zero or more fields of the index,
                * b) 0-1 inequality, and
                * c) any remaining columns unbounded.
                *
                * The key range's loInclusive and hiInclusive flags apply to b. For a, the comparisons
                * are always inclusive. E.g. if the range is >(x, y, p) and <(x, y, q), then the bounds
                * on the individual fields are (>=x, <=x), (>=y, <=y) and (>p, <q). So we want inclusive for
                * a, and whatever the key range specified for inclusivity for b. Checking the type of scan state f + 1
                * is how we distinguish cases a and b.
                *
                * The observant reader will wonder: what about >(x, y, p) and <(x, z, q)? This is a violation of condition
                * b since there are two inequalities (y != z, p != q) and it should not be possible to get this far with
                * such an IndexKeyRange.
                *
                * So for scanStates:
                * - lo(f) = hi(f), f < boundColumns - 1
                * - lo(f) - hi(f) defines a range, with limits described by keyRange.lo/hiInclusive,
                *   f = boundColumns - 1
                * The last argument to setRangeLimits determines which condition is checked.
                */
            for (f = 0; f < boundColumns() - 1; f++) {
                scanState(f).setRangeLimits(true, true, false);
            }
            scanState(boundColumns() - 1).setRangeLimits(keyRange().loInclusive(), keyRange().hiInclusive(), true);
        }
    }

    // For use by subclasses

    protected SortCursorMixedOrder(QueryContext context,
                                   IterationHelper iterationHelper,
                                   IndexKeyRange keyRange,
                                   API.Ordering ordering)
    {
        super(context, iterationHelper);
        this.keyRange = keyRange;
        this.ordering = ordering;
        // keyRange == null occurs when Sorter is used, (to sort an arbitrary input stream). There is no
        // IndexRowType in that case, so an IndexKeyRange can't be created.
        if (keyRange == null) {
            keyColumns = ordering.sortColumns();
            boundColumns = 0;
        } else {
            keyColumns = keyRange.indexRowType().index().indexRowComposition().getLength();
            boundColumns = keyRange.boundColumns();
        }
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
        return boundColumns;
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

    private MixedOrderScanStateSingleSegment scanState(int field)
    {
        return (MixedOrderScanStateSingleSegment) scanStates.get(field);
    }

    // Object state

    protected final IndexKeyRange keyRange;
    protected final API.Ordering ordering;
    protected final List<MixedOrderScanState> scanStates = new ArrayList<MixedOrderScanState>();
    private final int keyColumns; // Number of columns in the key. keyFields >= orderingColumns.
    private final int boundColumns;
    private boolean more;
    private boolean justOpened;
}
