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
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.persistit.exception.PersistitException;

class SortCursorMixedOrderBounded extends SortCursorMixedOrder
{
    // SortCursorMixedOrder interface

    @Override
    public void initializeScanStates() throws PersistitException
    {
        BoundExpressions lo = keyRange.lo().boundExpressions(bindings, adapter);
        ColumnSelector loSelector = keyRange.lo().columnSelector();
        BoundExpressions hi = keyRange.hi().boundExpressions(bindings, adapter);
        ColumnSelector hiSelector = keyRange.hi().columnSelector();
        // Set lo and hi bounds for each key segment
        for (int f = 0; f < sortFields; f++) {
            boolean loBounded = loSelector.includesColumn(f);
            boolean hiBounded = hiSelector.includesColumn(f);
            ValueSource loSource = loBounded ? lo.eval(f) : NullValueSource.only();
            ValueSource hiSource = hiBounded ? hi.eval(f) : NullValueSource.only();
            if (loBounded || hiBounded) {
                MixedOrderScanStateBounded scanState = new MixedOrderScanStateBounded(adapter, this, f);
                scanStates.add(scanState);
                scanState.setRange(loSource, hiSource);
            } else {
                scanStates.add(new MixedOrderScanStateRestOfKey(this, f));
                break;
            }
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
         * b since there are two inequalities (y != z, p != q). The optimizer shouldn't ask for such things.
         *
         * So for scanStates:
         * - lo(f) = hi(f), f < scanStates.size() - 2
         * - lo(f) - hi(f) defines a range, with limits described by keyRange.lo/hiInclusive, f = scanStates.size() - 2
         * - unbounded, f = scanStates.size() - 1
         */
        int f = 0;
        while (f < scanStates.size() - 2) {
            scanState(f++).setRangeLimits(true, true);
        }
        scanState(f).setRangeLimits(keyRange().loInclusive(), keyRange().hiInclusive());
    }

    // SortCursorMixedOrderBounded interface

    public SortCursorMixedOrderBounded(PersistitAdapter adapter,
                                       RowGenerator rowGenerator,
                                       IndexKeyRange keyRange,
                                       API.Ordering ordering)
    {
        super(adapter, rowGenerator, keyRange, ordering);
    }

    // For use by this class

    private MixedOrderScanStateBounded scanState(int field)
    {
        return (MixedOrderScanStateBounded) scanStates.get(field);
    }
}
