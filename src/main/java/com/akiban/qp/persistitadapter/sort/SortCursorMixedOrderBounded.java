/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.persistitadapter.sort;

import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.types.ValueSource;
import com.persistit.exception.PersistitException;

import static java.lang.Math.min;

class SortCursorMixedOrderBounded extends SortCursorMixedOrder
{
    // SortCursorMixedOrder interface

    @Override
    public void initializeScanStates() throws PersistitException
    {
        BoundExpressions lo = keyRange.lo().boundExpressions(bindings, adapter);
        BoundExpressions hi = keyRange.hi().boundExpressions(bindings, adapter);
        // Set lo and hi bounds for each key segment
        int f = 0;
        while (f < boundColumns()) {
            ValueSource loSource = lo.eval(f);
            ValueSource hiSource = hi.eval(f);
            MixedOrderScanStateBounded scanState = new MixedOrderScanStateBounded
                (this,
                 f,
                 f >= orderingColumns() || ordering.ascending(f));
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

    public SortCursorMixedOrderBounded(PersistitAdapter adapter,
                                       IterationHelper iterationHelper,
                                       IndexKeyRange keyRange,
                                       API.Ordering ordering)
    {
        super(adapter, iterationHelper, keyRange, ordering);
    }

    // For use by this class

    private MixedOrderScanStateBounded scanState(int field)
    {
        return (MixedOrderScanStateBounded) scanStates.get(field);
    }
}
