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
import com.persistit.exception.PersistitException;

class SortCursorMixedOrderBounded extends SortCursorMixedOrder
{
    // SortCursorMixedOrder interface

    @Override
    public MixedOrderScanState createScanState(SortCursorMixedOrder cursor, int field) throws PersistitException
    {
        return new MixedOrderScanStateBounded(adapter, cursor, field);
    }

    @Override
    public void computeBoundaries()
    {
        BoundExpressions lo = keyRange.lo().boundExpressions(bindings, adapter);
        BoundExpressions hi = keyRange.hi().boundExpressions(bindings, adapter);
        // Set lo and hi bounds for each key segment
        int fields = scanStates.size();
        for (int f = 0; f < fields; f++) {
            scanState(f).setRange(lo.eval(f), hi.eval(f));
        }
        // Determine whether the bound at each level is inclusive or exclusive at each end
        int f = 0;
        while (f < fields && scanState(f).loEqualsHi()) {
            scanState(f++).setRangeLimits(true, true);
        }
        if (f < fields) {
            scanState(f++).setRangeLimits(keyRange.loInclusive(), keyRange.hiInclusive());
        }
        while (f < fields) {
            scanState(f++).setRangeLimits(false, false);
        }
    }

    public SortCursorMixedOrderBounded(PersistitAdapter adapter,
                                       RowGenerator rowGenerator,
                                       IndexKeyRange keyRange,
                                       API.Ordering ordering)
    {
        super(adapter, rowGenerator, keyRange, ordering);
    }

    private MixedOrderScanStateBounded scanState(int field)
    {
        return (MixedOrderScanStateBounded) scanStates.get(field);
    }
}
