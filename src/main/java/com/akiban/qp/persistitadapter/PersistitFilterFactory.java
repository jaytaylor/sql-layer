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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.server.FieldDef;
import com.akiban.server.IndexDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.persistit.Key;
import com.persistit.KeyFilter;

import java.util.List;

// Adapted from PersistitStoreRowCollector and PersistitStore

class PersistitFilterFactory
{
    public KeyFilter computeIndexFilter(Key key, IndexDef indexDef, IndexKeyRange keyRange)
    {
        int[] fields = indexDef.getFields();
        KeyFilter.Term[] terms = new KeyFilter.Term[fields.length];
        for (int index = 0; index < fields.length; index++) {
            terms[index] = computeKeyFilterTerm(key, indexDef.getRowDef(), keyRange, fields[index]);
        }
        key.clear();
        return new KeyFilter(terms, terms.length, Integer.MAX_VALUE);
    }

    public KeyFilter computeHKeyFilter(Key key, RowDef leafRowDef, IndexKeyRange keyRange)
    {
        KeyFilter.Term[] terms = new KeyFilter.Term[leafRowDef.getHKeyDepth()];
        HKey hKey = leafRowDef.userTable().hKey();
        int t = 0;
        List<HKeySegment> segments = hKey.segments();
        for (int s = 0; s < segments.size(); s++) {
            HKeySegment hKeySegment = segments.get(s);
            RowDef def = adapter.rowDef(hKeySegment.table().getTableId());
            key.clear().reset().append(def.getOrdinal()).append(def.getOrdinal());
            // using termFromKeySegments avoids allocating a new Key object
            terms[t++] = KeyFilter.termFromKeySegments(key, key, true, true);
            List<HKeyColumn> segmentColumns = hKeySegment.columns();
            for (int c = 0; c < segmentColumns.size(); c++) {
                HKeyColumn segmentColumn = segmentColumns.get(c);
                KeyFilter.Term filterTerm;
                // A group table row has columns that are constrained to be equals, e.g. customer$cid and order$cid.
                // The non-null values in start/end could restrict one or the other, but the hkey references one
                // or the other. For the current segment column, use a literal for any of the equivalent columns.
                // For a user table, segmentColumn.equivalentColumns() == segmentColumn.column().
                filterTerm = KeyFilter.ALL;
                // Must end loop as soon as term other than ALL is found because computeKeyFilterTerm has
                // side effects if it returns anything else.
                List<Column> matchingColumns = segmentColumn.equivalentColumns();
                for (int m = 0; filterTerm == KeyFilter.ALL && m < matchingColumns.size(); m++) {
                    Column column = matchingColumns.get(m);
                    filterTerm = computeKeyFilterTerm(key, leafRowDef, keyRange, column.getPosition());
                }
                terms[t++] = filterTerm;
            }
        }
        key.clear();
        return new KeyFilter(terms, terms.length, terms.length);
    }

    public PersistitFilterFactory(PersistitAdapter adapter)
    {
        this.adapter = adapter;
    }

    // For use by this class

    // Returns a KeyFilter term if the specified field of either the start or end RowData is non-null, else null.
    private KeyFilter.Term computeKeyFilterTerm(Key key, RowDef rowDef, IndexKeyRange keyRange, int fieldIndex)
    {
        RowData start = keyRange.lo() == null ? null : rowData(keyRange.lo());
        RowData end = keyRange.hi() == null ? null : rowData(keyRange.hi());
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

    private void appendKeyField(Key key, FieldDef fieldDef, RowData rowData)
    {
        fieldDef.getEncoding().toKey(fieldDef, rowData, key);
    }

    private RowData rowData(IndexBound bound)
    {
        return ((PersistitGroupRow)bound.row()).rowData();
    }

    // Object state

    private final PersistitAdapter adapter;
}
