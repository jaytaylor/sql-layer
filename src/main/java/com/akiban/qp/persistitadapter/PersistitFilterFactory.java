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
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.RowBase;
import com.akiban.server.FieldDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.persistit.Key;
import com.persistit.KeyFilter;

import java.util.List;

// Adapted from PersistitStoreRowCollector and PersistitStore

class PersistitFilterFactory
{
    interface InternalHook {
        void reportKeyFilter(KeyFilter keyFilter);
    }

    public KeyFilter computeIndexFilter(Key key, Index index, IndexKeyRange keyRange, Bindings bindings)
    {
        List<IndexColumn> indexColumns = index.getColumns();
        KeyFilter.Term[] terms = new KeyFilter.Term[indexColumns.size()];
        for(int i = 0; i < indexColumns.size(); ++i) {
            terms[i] = computeKeyFilterTerm(key, indexColumns.get(i).getColumn(), i, keyRange, bindings);
        }
        key.clear();
        KeyFilter keyFilter = new KeyFilter(terms, terms.length, Integer.MAX_VALUE);
        if (hook != null) {
            hook.reportKeyFilter(keyFilter);
        }
        return keyFilter;
    }

    public KeyFilter computeHKeyFilter(Key key, RowDef leafRowDef, IndexKeyRange keyRange, Bindings bindings)
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
                    int columnPos = column.getPosition();
                    if(columnPos < leafRowDef.getFieldCount()) {
                        filterTerm = computeKeyFilterTerm(key, column, columnPos, keyRange, bindings);
                    }
                }
                terms[t++] = filterTerm;
            }
        }
        key.clear();
        KeyFilter keyFilter = new KeyFilter(terms, terms.length, terms.length);
        if (hook != null) {
            hook.reportKeyFilter(keyFilter);
        }
        return keyFilter;
    }

    PersistitFilterFactory(PersistitAdapter adapter, InternalHook hook)
    {
        this.adapter = adapter;
        this.hook = hook;
    }

    // For use by this class

    // Returns a KeyFilter term if the specified field of either the start or
    private KeyFilter.Term computeKeyFilterTerm(Key key, Column column, int position, IndexKeyRange keyRange,
                                                Bindings bindings)
    {
        boolean hasStart = (keyRange.lo() != null) && keyRange.lo().columnSelector().includesColumn(position);
        boolean hasEnd = (keyRange.hi() != null) && keyRange.hi().columnSelector().includesColumn(position);
        if (!hasStart && !hasEnd) {
            return KeyFilter.ALL;
        }
        key.clear();
        key.reset();
        if (hasStart) {
            appendKeyField(key, column, position, keyRange.lo().row(), bindings);
        } else {
            key.append(Key.BEFORE);
            key.setEncodedSize(key.getEncodedSize() + 1);
        }
        if (hasEnd) {
            appendKeyField(key, column, position, keyRange.hi().row(), bindings);
        } else {
            key.append(Key.AFTER);
        }
        //
        // Tricky: termFromKeySegments reads successive key segments when
        // called this way.
        //
        return KeyFilter.termFromKeySegments(key, key, keyRange.loInclusive(), keyRange.hiInclusive());
    }

    private void appendKeyField(Key key, Column column, int position, RowBase row, Bindings bindings)
    {
        FieldDef fieldDef = (FieldDef) column.getFieldDef();
        Object value = row.field(position, bindings);
        fieldDef.getEncoding().toKey(fieldDef, value, key);
    }


    // Object state

    private final PersistitAdapter adapter;
    private final InternalHook hook;
}
