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
import com.akiban.qp.Row;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class PersistitCursor implements Cursor
{
    // Cursor interface


    @Override
    public void open()
    {
    }

    @Override
    public Row next()
    {
        try {
            if (exchange.traverse(direction, true)) {
                exchange.fetch();
                row.copyFromExchange(exchange);
                direction = Key.GT;
            } else {
                row = null;
            }
        } catch (PersistitException e) {
            throw new BTreeAdapterRuntimeException(e);
        } catch (InvalidOperationException e) {
            throw new BTreeAdapterRuntimeException(e);
        }
        return row;
    }

    @Override
    public void close()
    {
        persistit.releaseExchange(adapter.session, exchange);
    }

    // PersistitCursor interface

    PersistitCursor(PersistitAdapter adapter, PersistitStore persistit, Exchange exchange)
    {
        this.adapter = adapter;
        this.persistit = persistit;
        this.exchange = exchange;
        this.direction = Key.GTEQ;
        this.row = new PersistitTableRow(adapter);
    }

    // Object state

    private final PersistitAdapter adapter;
    private final PersistitStore persistit;
    private final Exchange exchange;
    private Key.Direction direction;
    private PersistitTableRow row;
}
