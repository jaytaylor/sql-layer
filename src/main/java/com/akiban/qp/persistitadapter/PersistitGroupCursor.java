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
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.GroupCursor;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.util.ShareHolder;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A PersistitGroupCursor can be used in three ways:
 * 1) Scan the entire group: This occurs when there is no binding before open().
 * 2) For a given hkey, find the row and its descendents: This occurs when rebind(HKey, true) is called.
 * 3) For a given hkey, find the row without its descendents: This occurs when rebind(HKey, false) is called.
 */


class PersistitGroupCursor implements GroupCursor
{
    // GroupCursor interface

    @Override
    public void rebind(HKey hKey, boolean deep)
    {
        if (exchange != null) {
            throw new IllegalStateException("can't rebind while PersistitGroupCursor is open");
        }
        this.hKey = (PersistitHKey) hKey;
        this.hKeyDeep = deep;
    }


    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        assert exchange == null;
        try {
            exchange = adapter.takeExchange(groupTable).clear();
            groupScan =
                hKey == null ? new FullScan() :
                hKeyDeep ? new HKeyAndDescendentsScan(hKey) : new HKeyWithoutDescendentsScan(hKey);
        } catch (PersistitException e) {
            adapter.handlePersistitException(e);
        }
    }

    @Override
    public Row next()
    {
        try {
            boolean next = exchange != null;
            if (next) {
                groupScan.advance();
                next = exchange != null;
                if (next) {
                    PersistitGroupRow row = unsharedRow().get();
                    row.copyFromExchange(exchange);
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("PersistitGroupCursor: {}", next ? row : null);
            }
            return next ? row.get() : null;
        } catch (PersistitException e) {
            adapter.handlePersistitException(e);
            throw new AssertionError();
        } catch (InvalidOperationException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public void close()
    {
        if (exchange != null) {
            adapter.returnExchange(exchange);
            exchange = null;
            groupScan = null;
        }
    }

    // For use by this package

    PersistitGroupCursor(PersistitAdapter adapter, GroupTable groupTable)
        throws PersistitException
    {
        this.adapter = adapter;
        this.groupTable = groupTable;
        this.row = new ShareHolder<PersistitGroupRow>(adapter.newGroupRow());
        this.controllingHKey = adapter.newKey();
    }

    // For use by this class

    private ShareHolder<PersistitGroupRow> unsharedRow()
    {
        if (row.isEmpty() || row.get().isShared()) {
            row.hold(adapter.newGroupRow());
        }
        return row;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(PersistitGroupCursor.class);

    // Object state

    /*
     * 1) Scan entire group: Initialize exchange to Key.BEFORE and keep going forward, doing a deep traversal,
     *    until there are no more rows.
     *
     * 2) Scan one hkey and descendents: The key is copied to the exchange, to begin the scan, and to controllingHKey
     *    to determine when the scan should end.
     *
     * 3) Scan one hkey without descendents: The key is copied to the exchange.
     *
     *  General:
     *  - exchange == null iff this cursor is closed
     */

    private final PersistitAdapter adapter;
    private final GroupTable groupTable;
    private final ShareHolder<PersistitGroupRow> row;
    private Exchange exchange;
    private Key controllingHKey;
    private PersistitHKey hKey;
    private boolean hKeyDeep;
    private GroupScan groupScan;

    // Inner classes

    interface GroupScan
    {
        /**
         * Advance the exchange. Close if this causes the exchange to run out of selected rows.
         *
         * @throws PersistitException
         * @throws InvalidOperationException
         */
        void advance() throws PersistitException, InvalidOperationException;
    }

    private class FullScan implements GroupScan
    {
        @Override
        public void advance() throws PersistitException, InvalidOperationException
        {
            if (!exchange.traverse(direction, true)) {
                close();
            }
        }

        public FullScan() throws PersistitException
        {
            exchange.getKey().append(Key.BEFORE);
            direction = Key.GT;
        }

        private final Key.Direction direction;
    }

    private class HKeyAndDescendentsScan implements GroupScan
    {
        @Override
        public void advance() throws PersistitException, InvalidOperationException
        {
            if (!exchange.traverse(direction, true) ||
                exchange.getKey().firstUniqueByteIndex(controllingHKey) < controllingHKey.getEncodedSize()) {
                close();
            }
            direction = Key.GT;
        }

        HKeyAndDescendentsScan(PersistitHKey hKey) throws PersistitException
        {
            hKey.copyTo(exchange.getKey());
            hKey.copyTo(controllingHKey);
        }

        private Key.Direction direction = Key.GTEQ;
    }

    private class HKeyWithoutDescendentsScan implements GroupScan
    {
        @Override
        public void advance() throws PersistitException, InvalidOperationException
        {
            if (first) {
                exchange.fetch();
                if (!exchange.getValue().isDefined()) {
                    close();
                }
                first = false;
            } else {
                close();
            }
        }

        HKeyWithoutDescendentsScan(PersistitHKey hKey) throws PersistitException
        {
            hKey.copyTo(exchange.getKey());
        }

        private boolean first = true;
    }
}
