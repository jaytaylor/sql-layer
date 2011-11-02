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

import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class MixedOrderScanStateBounded extends MixedOrderScanState
{
    @Override
    public boolean startScan() throws PersistitException
    {
        Key.Direction direction;
        if (ascending) {
            if (loSource.isNull()) {
                cursor.exchange.append(Key.BEFORE);
                direction = Key.GT;
            } else {
                keyTarget.expectingType(loSource.getConversionType());
                Converters.convert(loSource, keyTarget);
                direction = loInclusive ? Key.GTEQ : Key.GT;
            }
        } else {
            if (hiSource.isNull()) {
                // About null handling: See comment in SortCursorUnidirectional.evaluateBoundaries.
                if (loSource.isNull()) {
                    cursor.exchange.append(null);
                } else {
                    cursor.exchange.append(Key.AFTER);
                }
                direction = Key.LT;
            } else {
                keyTarget.expectingType(hiSource.getConversionType());
                Converters.convert(hiSource, keyTarget);
                direction = hiInclusive ? Key.LTEQ : Key.LT;
            }
        }
        boolean x = cursor.exchange.traverse(direction, false);
        return x;
    }

    public void setRange(ValueSource lo, ValueSource hi)
    {
        this.loSource = lo;
        this.hiSource = hi;
    }

    public void setRangeLimits(boolean loInclusive, boolean hiInclusive)
    {
        this.loInclusive = loInclusive;
        this.hiInclusive = hiInclusive;
    }

    public MixedOrderScanStateBounded(StoreAdapter adapter, SortCursorMixedOrder cursor, int field)
        throws PersistitException
    {
        super(cursor, field, cursor.ordering().ascending(field));
        this.keyTarget = new PersistitKeyValueTarget();
        this.keyTarget.attach(cursor.exchange.getKey());
    }

    private final PersistitKeyValueTarget keyTarget;
    private ValueSource loSource;
    private ValueSource hiSource;
    private boolean loInclusive;
    private boolean hiInclusive;
}
