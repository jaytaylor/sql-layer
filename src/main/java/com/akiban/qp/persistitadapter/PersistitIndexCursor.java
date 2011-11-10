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
import com.akiban.qp.operator.*;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.persistitadapter.sort.IterationHelper;
import com.akiban.qp.persistitadapter.sort.SortCursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.util.ShareHolder;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

class PersistitIndexCursor implements Cursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        assert exchange == null;
        exchange = adapter.takeExchange(indexRowType.index());
        sortCursor = SortCursor.create(adapter, keyRange, ordering, new IndexScanIterationHelper());
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
                                    // TODO: It would be better to limit the use of exchange to SortCursor, which means
                                    // TODO: that the selector would need to be pushed down. Alternatively, the exchange's
                                    // TODO: value could be made available here in PersistitIndexRow.
                                    selector.matchesAll() || selector.matches(exchange.fetch().getValue().getLong()));
                } else {
                    close();
                    needAnother = false;
                }
            } while (needAnother);
        } catch (PersistitException e) {
            adapter.handlePersistitException(e);
            throw new AssertionError();
        }
        assert (next == null) == (exchange == null);
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
                         IndexScanSelector selector)
        throws PersistitException
    {
        this.keyRange = keyRange;
        this.ordering = ordering;
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        this.row = new ShareHolder<PersistitIndexRow>(adapter.newIndexRow(indexRowType));
        this.isTableIndex = indexRowType.index().isTableIndex();
        this.selector = selector;
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

    // Object state

    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private final ShareHolder<PersistitIndexRow> row;
    private final IndexKeyRange keyRange;
    private final API.Ordering ordering;
    private final boolean isTableIndex;
    private IndexScanSelector selector;
    private Exchange exchange;
    private SortCursor sortCursor;

    // Inner classes

    private class IndexScanIterationHelper implements IterationHelper
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
    }
}
