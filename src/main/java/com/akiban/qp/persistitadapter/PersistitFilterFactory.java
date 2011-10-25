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
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Bindings;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
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
        int ncols = indexColumns.size();
        int maxlo = -1, maxhi = -1;
        for (int i = 0; i < ncols; ++i) {
          if ((keyRange.lo() != null) && 
              keyRange.lo().columnSelector().includesColumn(i))
            maxlo = i;
          if ((keyRange.hi() != null) && 
              keyRange.hi().columnSelector().includesColumn(i))
            maxhi = i;
        }
        KeyFilter.Term[] terms = new KeyFilter.Term[ncols];
        for (int i = 0; i < ncols; ++i) {
            terms[i] = computeKeyFilterTerm(target, key, indexColumns.get(i).getColumn(),
                                            i, keyRange, bindings,
                                            (i < maxlo) || ((i == maxlo) && keyRange.loInclusive()),
                                            (i < maxhi) || ((i == maxhi) && keyRange.hiInclusive()));
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
        this.adapter = adapter;
        this.hook = hook;
    }

    // For use by this class

    // Returns a KeyFilter term if the specified field of either the start or
    private KeyFilter.Term computeKeyFilterTerm(PersistitKeyValueTarget tuple, Key key, Column column, 
                                                int position, IndexKeyRange keyRange, Bindings bindings,
                                                boolean loInclusive, boolean hiInclusive)
    {
        boolean hasStart = (keyRange.lo() != null) && keyRange.lo().columnSelector().includesColumn(position);
        boolean hasEnd = (keyRange.hi() != null) && keyRange.hi().columnSelector().includesColumn(position);
        if (!hasStart && !hasEnd) {
            return KeyFilter.ALL;
        }
        key.clear();
        key.reset();
        if (hasStart) {
            appendKeyField(tuple, column, position, keyRange.lo().boundExpressions(bindings, adapter));
        } else {
            key.append(Key.BEFORE);
            key.setEncodedSize(key.getEncodedSize() + 1);
        }
        if (hasEnd) {
            appendKeyField(tuple, column, position, keyRange.hi().boundExpressions(bindings, adapter));
        } else {
            key.append(Key.AFTER);
        }
        //
        // Tricky: termFromKeySegments reads successive key segments when
        // called this way.
        //
        return KeyFilter.termFromKeySegments(key, key, loInclusive, hiInclusive);
    }

    private void appendKeyField(PersistitKeyValueTarget target, Column column, int position, BoundExpressions row)
    {
        target.expectingType(column);
        ValueSource source = row.eval(position);
        Converters.convert(source, target);
    }


    // Object state

    private final PersistitAdapter adapter;
    private final InternalHook hook;
}
