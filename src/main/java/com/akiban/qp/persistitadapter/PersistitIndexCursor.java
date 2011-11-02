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

import com.akiban.ais.model.Index;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.StoreAdapterRuntimeException;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.util.ShareHolder;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;

import java.io.InterruptedIOException;

class PersistitIndexCursor implements Cursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        assert exchange == null;
        exchange = adapter.takeExchange(indexRowType.index()).clear().append(boundary);
        if (keyRange != null) {
            indexFilter = adapter.filterFactory.computeIndexFilter(exchange.getKey(), index(), keyRange, bindings);
        }
    }

    @Override
    public Row next()
    {
        try {
            boolean needAnother;
            do {
                if (exchange != null &&
                    (indexFilter == null
                     ? exchange.traverse(direction, true)
                     : exchange.traverse(direction, indexFilter, FETCH_NO_BYTES))) {
                    if (selector.matchesAll() || selector.matches(exchange.fetch().getValue().getLong())) {
                        // The value of a group index is the depth at which it's defined, as an int.
                        // See OperatorStoreGIHandler, search for "Description of group index entry values"
                        unsharedRow().get().copyFromExchange(exchange);
                        needAnother = false;
                    }
                    else {
                        needAnother = true;
                    }
                } else {
                    close();
                    needAnother = false;
                }
            } while (needAnother);
        } catch (PersistitException e) {
            adapter.handlePersistitException(e);
        }
        PersistitIndexRow next = exchange == null ? null : row.get();
        assert (next == null) == (exchange == null);
        return next;
    }

    @Override
    public void close()
    {
        if (exchange != null) {
            adapter.returnExchange(exchange);
            exchange = null;
            indexFilter = null;
            row.release();
        }
    }

    // For use by this package

    PersistitIndexCursor(PersistitAdapter adapter,
                         IndexRowType indexRowType,
                         boolean reverse,
                         IndexKeyRange keyRange,
                         IndexScanSelector selector)
        throws PersistitException
    {
        this.keyRange = keyRange;
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        this.row = new ShareHolder<PersistitIndexRow>(adapter.newIndexRow(indexRowType));
        this.selector = selector;
        if (reverse) {
            boundary = Key.AFTER;
            direction = Key.LT;
        } else {
            boundary = Key.BEFORE;
            direction = Key.GT;
        }
    }

    // For use by this class

    private ShareHolder<PersistitIndexRow> unsharedRow() throws PersistitException
    {
        if (row.isEmpty() || row.isShared()) {
            row.hold(adapter.newIndexRow(indexRowType));
        }
        return row;
    }

    private Index index()
    {
        return indexRowType.index();
    }

    PersistitAdapter adapter() {
        return adapter;
    }

    Exchange exchange() {
        return exchange;
    }

    // Object state

    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private final ShareHolder<PersistitIndexRow> row;
    private final Key.EdgeValue boundary;
    private final Key.Direction direction;
    private final IndexKeyRange keyRange;
    private final IndexScanSelector selector;
    private Exchange exchange;
    private KeyFilter indexFilter;

    // consts
    private static final int FETCH_NO_BYTES = 0;
}
