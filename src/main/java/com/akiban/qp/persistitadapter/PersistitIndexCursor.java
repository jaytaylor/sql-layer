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

import com.akiban.qp.BTreeAdapterRuntimeException;
import com.akiban.qp.Cursor;
import com.akiban.qp.HKey;
import com.akiban.qp.IndexCursor;
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.InvalidOperationException;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class PersistitIndexCursor implements IndexCursor
{
    // Row interface

    @Override
    public RowType rowType()
    {
        return row.rowType();
    }

    @Override
    public Row currentRow()
    {
        return row.managedRow();
    }

    @Override
    public boolean ancestorOf(Row row)
    {
        return this.row.ancestorOf(row);
    }

    @Override
    public Object field(int i)
    {
        return row.field(i);
    }

    @Override
    public HKey hKey()
    {
        return row.hKey();
    }

    @Override
    public ManagedRow managedRow()
    {
        return row.managedRow();
    }

    // Cursor interface

    @Override
    public void open()
    {
        try {
            exchange = adapter.takeExchange(indexRowType.index()).clear().append(Key.BEFORE);
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public boolean next()
    {
        try {
            if (exchange != null && exchange.traverse(Key.GT, true)) {
                unsharedRow().managedRow().copyFromExchange(exchange);
            } else {
                close();
            }
        } catch (PersistitException e) {
            throw new BTreeAdapterRuntimeException(e);
        }
        return exchange != null;
    }

    @Override
    public void close()
    {
        if (exchange != null) {
            adapter.returnExchange(exchange);
            exchange = null;
            row.set(null);
        }
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

    // Object state

    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private final RowHolder<PersistitIndexRow> row;
    private Exchange exchange;
}
