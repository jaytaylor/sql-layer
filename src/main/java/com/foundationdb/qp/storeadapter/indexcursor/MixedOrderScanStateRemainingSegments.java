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

import com.persistit.Key;

import static com.foundationdb.qp.storeadapter.indexcursor.IndexCursor.INDEX_TRAVERSE;

class MixedOrderScanStateRemainingSegments<S> extends MixedOrderScanState<S>
{
    @Override
    public boolean startScan()
    {
        if (subtreeRootKey == null) {
            subtreeRootKey = new Key(cursor.key());
        } else {
            cursor.key().copyTo(subtreeRootKey);
        }
        INDEX_TRAVERSE.hit();
        return cursor.traverse(Key.GT, true);
    }

    @Override
    public boolean advance()
    {
        INDEX_TRAVERSE.hit();
        boolean more = ascending ? cursor.nextInternal(true) : cursor.prevInternal(true);
        if (more) {
            more = cursor.key().firstUniqueByteIndex(subtreeRootKey) >= subtreeRootKey.getEncodedSize();
        }
        if (!more) {
            // Restore exchange key to where it was before exploring this subtree. But also attach one
            // more key segment since IndexCursorMixedOrder is going to cut one.
            subtreeRootKey.copyTo(cursor.key());
            cursor.key().append(Key.BEFORE);
        }
        return more;
    }

    @Override
    public boolean jump(S fieldValue)
    {
        return startScan();
    }

    public MixedOrderScanStateRemainingSegments(IndexCursorMixedOrder indexCursor, int field)
    {
        super(indexCursor, field, true);
    }

    private Key subtreeRootKey;
}
