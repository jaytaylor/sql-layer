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

import com.akiban.qp.physicaloperator.StoreAdapterRuntimeException;
import com.akiban.qp.physicaloperator.IndexCursor;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.IndexDef;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;

class PersistitIndexCursor implements IndexCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        assert exchange == null;
        try {
            exchange = adapter.takeExchange(indexRowType.index()).clear().append(Key.BEFORE);
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
        if (keyRange != null) {
            indexFilter = adapter.filterFactory.computeIndexFilter(exchange.getKey(), indexDef(), keyRange);
        }
    }

    @Override
    public boolean next()
    {
        try {
            if (exchange != null &&
                (indexFilter == null
                 ? exchange.traverse(Key.GT, true)
                 : exchange.traverse(Key.GT, indexFilter, 0))) {
                unsharedRow().managedRow().copyFromExchange(exchange);
            } else {
                close();
            }
        } catch (PersistitException e) {
            throw new StoreAdapterRuntimeException(e);
        }
        return exchange != null;
    }

    @Override
    public void close()
    {
        if (exchange != null) {
            adapter.returnExchange(exchange);
            exchange = null;
            keyRange = null;
            indexFilter = null;
            row.set(null);
        }
    }

    @Override
    public ManagedRow currentRow()
    {
        return row.managedRow();
    }

    // IndexCursor interface

    @Override
    public void bind(IndexKeyRange keyRange)
    {
        this.keyRange = keyRange;
    }

    // For use by this package

    PersistitIndexCursor(PersistitAdapter adapter, IndexRowType indexRowType)
        throws PersistitException
    {
        this.adapter = adapter;
        this.indexRowType = indexRowType;
        this.row = new RowHolder<PersistitIndexRow>(adapter.newIndexRow(indexRowType));
    }

    // For use by this class

    private RowHolder<PersistitIndexRow> unsharedRow() throws PersistitException
    {
        if (row.managedRow().isShared()) {
            row.set(adapter.newIndexRow(indexRowType));
        }
        return row;
    }

    private IndexDef indexDef()
    {
        return (IndexDef) indexRowType.index().indexDef();
    }

    // Object state

    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private final RowHolder<PersistitIndexRow> row;
    private IndexKeyRange keyRange;
    private Exchange exchange;
    private KeyFilter indexFilter;
}
