/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.qp.storeadapter.indexcursor;

import static com.foundationdb.qp.storeadapter.indexcursor.IndexCursor.INDEX_TRAVERSE;

abstract class MixedOrderScanState<S>
{
    public abstract boolean startScan();

    public abstract boolean jump(S fieldValue);

    public final int field()
    {
        return field;
    }

    public boolean advance()
    {
        INDEX_TRAVERSE.hit();
        return ascending ? cursor.nextInternal(false) : cursor.prevInternal(false);
    }

    protected MixedOrderScanState(IndexCursorMixedOrder cursor, int field, boolean ascending)
    {
        this.cursor = cursor;
        this.field = field;
        this.ascending = ascending;
    }

    protected final IndexCursorMixedOrder cursor;
    protected final int field;
    protected final boolean ascending;
}
