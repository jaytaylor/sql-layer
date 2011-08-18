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
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.RowBase;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.Converters;
import com.persistit.Key;
import com.persistit.KeyFilter;

import java.util.List;

// Adapted from PersistitStoreRowCollector and PersistitStore

class PersistitFilterFactory
{
    interface InternalHook
    {
        void reportKeyFilter(KeyFilter keyFilter);
    }

    public KeyFilter computeIndexFilter(Key key, Index index, IndexKeyRange keyRange, Bindings bindings)
    {
        PersistitKeyValueTarget target = new PersistitKeyValueTarget();
        target.attach(key);

        List<IndexColumn> indexColumns = index.getColumns();
        KeyFilter.Term[] terms = new KeyFilter.Term[indexColumns.size()];
        for (int i = 0; i < indexColumns.size(); ++i) {
            terms[i] = computeKeyFilterTerm(target, key, indexColumns.get(i).getColumn(), i, keyRange, bindings);
        }
        key.clear();
        KeyFilter keyFilter = new KeyFilter(terms, terms.length, Integer.MAX_VALUE);
        if (hook != null) {
            hook.reportKeyFilter(keyFilter);
        }
        return keyFilter;
    }

    PersistitFilterFactory(PersistitAdapter adapter, InternalHook hook)
    {
        this.hook = hook;
    }

    // For use by this class

    // Returns a KeyFilter term if the specified field of either the start or
    private KeyFilter.Term computeKeyFilterTerm(PersistitKeyValueTarget tuple, Key key, Column column, int position,
                                                IndexKeyRange keyRange, Bindings bindings)
    {
        boolean hasStart = (keyRange.lo() != null) && keyRange.lo().columnSelector().includesColumn(position);
        boolean hasEnd = (keyRange.hi() != null) && keyRange.hi().columnSelector().includesColumn(position);
        if (!hasStart && !hasEnd) {
            return KeyFilter.ALL;
        }
        key.clear();
        key.reset();
        if (hasStart) {
            appendKeyField(tuple, column, position, keyRange.lo().row(), bindings);
        } else {
            key.append(Key.BEFORE);
            key.setEncodedSize(key.getEncodedSize() + 1);
        }
        if (hasEnd) {
            appendKeyField(tuple, column, position, keyRange.hi().row(), bindings);
        } else {
            key.append(Key.AFTER);
        }
        //
        // Tricky: termFromKeySegments reads successive key segments when
        // called this way.
        //
        return KeyFilter.termFromKeySegments(key, key, keyRange.loInclusive(), keyRange.hiInclusive());
    }

    private void appendKeyField(PersistitKeyValueTarget target, Column column, int position, RowBase row, Bindings bindings)
    {
        target.expectingType(column);
        ValueSource source = row.bindSource(position, bindings);
        Converters.convert(source, target);
    }


    // Object state

    private final InternalHook hook;
}
