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
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.types.ValueSource;
import com.persistit.exception.PersistitException;

class SortCursorMixedOrderBounded extends SortCursorMixedOrder
{
    // SortCursorMixedOrder interface

    @Override
    public void initializeScanStates() throws PersistitException
    {
        BoundExpressions lo = keyRange.lo().boundExpressions(context);
        BoundExpressions hi = keyRange.hi().boundExpressions(context);
        // Set lo and hi bounds for each key segment
        int f = 0;
        while (f < boundColumns()) {
            ValueSource loSource = lo.eval(f);
            ValueSource hiSource = hi.eval(f);
            MixedOrderScanStateBounded scanState = new MixedOrderScanStateBounded
                (this,
                 f,
                 f >= orderingColumns() || ordering.ascending(f),
                 f == boundColumns() - 1);
            scanState.setRange(loSource, hiSource);
            scanStates.add(scanState);
            f++;
        }
        while (f < orderingColumns()) {
            MixedOrderScanStateUnbounded scanState = new MixedOrderScanStateUnbounded(this, f);
            scanStates.add(scanState);
            f++;
        }
        if (f < keyColumns()) {
            MixedOrderScanStateRestOfKey scanState = new MixedOrderScanStateRestOfKey(this, orderingColumns());
            scanStates.add(scanState);
        }
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

    // SortCursorMixedOrderBounded interface

    public SortCursorMixedOrderBounded(QueryContext context,
                                       IterationHelper iterationHelper,
                                       IndexKeyRange keyRange,
                                       API.Ordering ordering)
    {
        super(context, iterationHelper, keyRange, ordering);
    }

    // For use by this class

    private MixedOrderScanStateBounded scanState(int field)
    {
        return (MixedOrderScanStateBounded) scanStates.get(field);
    }
}
