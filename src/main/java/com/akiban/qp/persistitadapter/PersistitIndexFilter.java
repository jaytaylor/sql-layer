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

package com.akiban.qp.persistitadapter;

import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.IndexBound;
import com.akiban.server.FieldDef;
import com.akiban.server.IndexDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.persistit.Key;
import com.persistit.KeyFilter;

// Adapted from PersistitStoreRowCollector and PersistitStore

class PersistitIndexFilter
{
    public static KeyFilter computeIndexFilter(Key key, IndexDef indexDef, IndexKeyRange keyRange)
    {
        int[] fields = indexDef.getFields();
        KeyFilter.Term[] terms = new KeyFilter.Term[fields.length];
        for (int index = 0; index < fields.length; index++) {
            terms[index] = computeKeyFilterTerm(key, indexDef.getRowDef(), keyRange, fields[index]);
        }
        key.clear();
        return new KeyFilter(terms, terms.length, Integer.MAX_VALUE);
    }

    // Returns a KeyFilter term if the specified field of either the start or end RowData is non-null, else null.
    private static KeyFilter.Term computeKeyFilterTerm(Key key, RowDef rowDef, IndexKeyRange keyRange, int fieldIndex)
    {
        RowData start = rowData(keyRange.lo());
        RowData end = rowData(keyRange.hi());
        if (fieldIndex < 0 || fieldIndex >= rowDef.getFieldCount()) {
            return KeyFilter.ALL;
        }
        long lowLoc = start == null ? 0 : rowDef.fieldLocation(start, fieldIndex);
        long highLoc = end == null ? 0 : rowDef.fieldLocation(end, fieldIndex);
        if (lowLoc != 0 || highLoc != 0) {
            key.clear();
            key.reset();
            if (lowLoc != 0) {
                appendKeyField(key, rowDef.getFieldDef(fieldIndex), start);
            } else {
                key.append(Key.BEFORE);
                key.setEncodedSize(key.getEncodedSize() + 1);
            }
            if (highLoc != 0) {
                appendKeyField(key, rowDef.getFieldDef(fieldIndex), end);
            } else {
                key.append(Key.AFTER);
            }
            //
            // Tricky: termFromKeySegments reads successive key segments when
            // called this way.
            //
            return KeyFilter.termFromKeySegments(key, key, keyRange.loInclusive(), keyRange.hiInclusive());
        } else {
            return KeyFilter.ALL;
        }
    }

    private static void appendKeyField(Key key, FieldDef fieldDef, RowData rowData)
    {
        fieldDef.getEncoding().toKey(fieldDef, rowData, key);
    }

    private static RowData rowData(IndexBound bound)
    {
        return ((PersistitGroupRow)bound.row()).rowData();
    }
}
