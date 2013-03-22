/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.qp.persistitadapter.indexcursor;

import com.persistit.exception.PersistitException;

import static com.akiban.qp.persistitadapter.indexcursor.IndexCursor.INDEX_TRAVERSE;

abstract class MixedOrderScanState<S>
{
    public abstract boolean startScan() throws PersistitException;

    public abstract boolean jump(S fieldValue) throws PersistitException;

    public final int field()
    {
        return field;
    }

    public boolean advance() throws PersistitException
    {
        INDEX_TRAVERSE.hit();
        return ascending ? cursor.exchange().next(false) : cursor.exchange().previous(false);
    }

    protected MixedOrderScanState(IndexCursorMixedOrder cursor, int field, boolean ascending) throws PersistitException
    {
        this.cursor = cursor;
        this.field = field;
        this.ascending = ascending;
    }

    protected final IndexCursorMixedOrder cursor;
    protected final int field;
    protected final boolean ascending;
}
