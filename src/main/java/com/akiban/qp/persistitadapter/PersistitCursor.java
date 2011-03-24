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
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class PersistitCursor implements Cursor
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
    public <T> T field(int i)
    {
        return (T) row.field(i);
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
    }

    @Override
    public boolean next()
    {
        try {
            if (!closed && exchange.traverse(Key.GT, true)) {
                exchange.fetch();
                unsharedRow().managedRow().copyFromExchange(exchange);
            } else {
                close();
            }
        } catch (PersistitException e) {
            throw new BTreeAdapterRuntimeException(e);
        } catch (InvalidOperationException e) {
            throw new BTreeAdapterRuntimeException(e);
        }
        return !closed;
    }

    @Override
    public void close()
    {
        if (!closed) {
            persistit.releaseExchange(adapter.session, exchange);
            row.set(null);
            closed = true;
        }
    }

    // For use by this package

    PersistitCursor(PersistitAdapter adapter, PersistitStore persistit, Exchange exchange) throws PersistitException
    {
        this.adapter = adapter;
        this.persistit = persistit;
        this.exchange = exchange.clear().append(Key.BEFORE);
        this.row = new RowHolder<PersistitRow>(adapter.newRow());
    }

    // For use by this class

    private RowHolder<PersistitRow> unsharedRow()
    {
        if (row.managedRow().isShared()) {
            row.set(adapter.newRow());
        }
        return row;
    }

    // Object state

    private final PersistitAdapter adapter;
    private final PersistitStore persistit;
    private final Exchange exchange;
    private final RowHolder<PersistitRow> row;
    private boolean closed = false;
}
