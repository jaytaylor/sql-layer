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

import com.akiban.util.Tap;
import com.persistit.exception.PersistitException;

abstract class MixedOrderScanState
{
    public abstract boolean startScan() throws PersistitException;

    public boolean advance() throws PersistitException
    {
        TRAVERSE_COUNT.hit();
        return ascending ? cursor.exchange.next(false) : cursor.exchange.previous(false);
    }

    protected MixedOrderScanState(SortCursorMixedOrder cursor, int field, boolean ascending) throws PersistitException
    {
        this.cursor = cursor;
        this.field = field;
        this.ascending = ascending;
    }

    protected final SortCursorMixedOrder cursor;
    protected final int field;
    protected final boolean ascending;
    
    private static final Tap.PointTap TRAVERSE_COUNT = SortCursor.SORT_TRAVERSE;
}
