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
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.StoreAdapterRuntimeException;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.persistitadapter.sort.RowGenerator;
import com.akiban.qp.persistitadapter.sort.SortCursor;
import com.akiban.qp.persistitadapter.sort.SortCursorAscending;
import com.akiban.qp.persistitadapter.sort.SortCursorDescending;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.util.ShareHolder;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;
import sun.nio.cs.Surrogate;

class PersistitIndexCursor implements Cursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        assert exchange == null;
        exchange = adapter.takeExchange(indexRowType.index());
        sortCursor = SortCursor.create(keyRange, ordering, new IndexScanRowGenerator());
        sortCursor.open(bindings);
    }

    @Override
    public Row next()
    {
        Row next;
        try {
            boolean needAnother;
            do {
                if ((next = sortCursor.next()) != null) {
                    needAnother = !(isTableIndex ||
                                    // The value of a group index is the depth at which it's defined, as an int.
                                    // See OperatorStoreGIHandler, search for "Description of group index entry values"
                                    exchange.fetch().getValue().getInt() >= minimumDepth);
                } else {
                    close();
                    needAnother = false;
                }
            } while (needAnother);
        } catch (PersistitException e) {
            throw new StoreAdapterRuntimeException(e);
        }
        return next;
    }

    @Override
    public void close()
    {
        if (exchange != null) {
            adapter.returnExchange(exchange);
            exchange = null;
            row.release();
        }
    }

    // For use by this package

    PersistitIndexCursor(PersistitAdapter adapter,
                         IndexRowType indexRowType,
                         IndexKeyRange keyRange,
                         API.Ordering ordering,
                         UserTable innerJoinUntil)
        throws PersistitException
    {
        this.keyRange = keyRange;
        this.ordering = ordering;
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        this.row = new ShareHolder<PersistitIndexRow>(adapter.newIndexRow(indexRowType));
        this.isTableIndex = indexRowType.index().isTableIndex();
        this.minimumDepth = innerJoinUntil.getDepth();
    }

    // For use by this class

    private ShareHolder<PersistitIndexRow> unsharedRow() throws PersistitException
    {
        if (row.get().isShared()) {
            row.hold(adapter.newIndexRow(indexRowType));
        }
        return row;
    }

    private Index index()
    {
        return indexRowType.index();
    }

    // Object state

    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private final ShareHolder<PersistitIndexRow> row;
    private final IndexKeyRange keyRange;
    private final API.Ordering ordering;
    private final boolean isTableIndex;
    private final int minimumDepth;
    private Exchange exchange;
    private SortCursor sortCursor;

    // Inner classes

    private class IndexScanRowGenerator implements RowGenerator
    {
        @Override
        public Row row() throws PersistitException
        {
            unsharedRow().get().copyFromExchange(exchange);
            return row.get();
        }

        @Override
        public void close()
        {
            PersistitIndexCursor.this.close();
        }

        @Override
        public Exchange exchange()
        {
            return PersistitIndexCursor.this.exchange;
        }

        @Override
        public KeyFilter keyFilter(Bindings bindings)
        {
            return
                keyRange.unbounded()
                ? null
                : adapter.filterFactory.computeIndexFilter(exchange.getKey(), index(), keyRange, bindings);
        }
    }
}
