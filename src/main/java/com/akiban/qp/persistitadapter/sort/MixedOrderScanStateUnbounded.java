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
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class  MixedOrderScanStateUnbounded extends MixedOrderScanState
{
    @Override
    public boolean startScan() throws PersistitException
    {
        Key.Direction direction;
        if (ascending) {
            cursor.exchange.append(Key.BEFORE);
            direction = Key.GT;
        } else {
            cursor.exchange.append(Key.AFTER);
            direction = Key.LT;
        }
        TRAVERSE_COUNT.hit();
        return cursor.exchange.traverse(direction, false);
    }

    public MixedOrderScanStateUnbounded(SortCursorMixedOrder cursor, int field) throws PersistitException
    {
        super(cursor, field, cursor.ordering().ascending(field));
    }
    
    private static final Tap.PointTap TRAVERSE_COUNT = Tap.createCount("traverse_moss_u");
}
