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

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.physicaloperator.StoreAdapterRuntimeException;
import com.akiban.qp.physicaloperator.GroupCursor;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.RowHolder;
import com.akiban.server.InvalidOperationException;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class PersistitGroupCursor implements GroupCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        hKeyRestricted = false;
        try {
            exchange = adapter.takeExchange(groupTable).clear().append(Key.BEFORE);
            direction = Key.GT;
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public boolean next()
    {
        try {
            if (exchange != null && exchange.traverse(direction, true)) {
                unsharedRow().managedRow().copyFromExchange(exchange);
                direction = Key.GT;
                if (hKeyRestricted &&
                    exchange.getKey().firstUniqueByteIndex(restriction) < restriction.getEncodedSize()) {
                    close();
                }
            } else {
                close();
            }
        } catch (PersistitException e) {
            throw new StoreAdapterRuntimeException(e);
        } catch (InvalidOperationException e) {
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
        }
    }

    @Override
    public ManagedRow currentRow()
    {
        return row.managedRow();
    }

    // GroupCursor interface

    @Override
    public void open(HKey hKey)
    {
        hKeyRestricted = true;
        try {
            PersistitHKey persistitHKey = (PersistitHKey) hKey;
            exchange = adapter.takeExchange(groupTable);
            Key exchangeKey = exchange.getKey();
            exchangeKey.clear();
            persistitHKey.copyTo(exchangeKey);
            persistitHKey.copyTo(restriction);
            direction = Key.GTEQ;
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    // For use by this package

    PersistitGroupCursor(PersistitAdapter adapter, GroupTable groupTable) throws PersistitException
    {
        this.adapter = adapter;
        this.groupTable = groupTable;
        this.row = new RowHolder<PersistitGroupRow>(adapter.newGroupRow());
        this.restriction = new Key(adapter.persistit.getDb());
    }

    // For use by this class

    private RowHolder<PersistitGroupRow> unsharedRow()
    {
        if (row.managedRow().isShared()) {
            row.set(adapter.newGroupRow());
        }
        return row;
    }

    // Object state

    private final PersistitAdapter adapter;
    private final GroupTable groupTable;
    private final RowHolder<PersistitGroupRow> row;
    private Exchange exchange;
    private Key.Direction direction;
    // For use in an hkey-restricted scan
    private boolean hKeyRestricted;
    private Key restriction;
}
