/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.PersistitAdapterException;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;
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
        CursorLifecycle.checkIdle(this);
        this.hKey = (PersistitHKey) hKey;
        this.hKeyDeep = deep;
    }


    // Cursor interface

    @Override
    public void open()
    {
        try {
            CursorLifecycle.checkIdle(this);
            this.exchange = adapter.takeExchange(group);
            exchange.clear();
            groupScan =
                hKey == null ? new FullScan() :
                hKeyDeep ? new HKeyAndDescendentsScan(hKey) : new HKeyWithoutDescendentsScan(hKey);
            idle = false;
        } catch (PersistitException e) {
            adapter.handlePersistitException(e);
        }
    }

    @Override
    public Row next()
    {
        try {
            CursorLifecycle.checkIdleOrActive(this);
            boolean next = !idle;
            PersistitGroupRow row = null;
            if (next) {
                groupScan.advance();
                next = !idle;
                if (next) {
                    row = adapter.newGroupRow();
                    row.copyFromExchange(exchange);
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("PersistitGroupCursor: {}", row);
            }
            return row;
        } catch (PersistitException e) {
            adapter.handlePersistitException(e);
            throw new AssertionError();
        } catch (InvalidOperationException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void close()
    {
        CursorLifecycle.checkIdleOrActive(this);
        if (!idle) {
            groupScan = null;
            adapter.returnExchange(exchange);
            exchange = null;
            idle = true;
        }
    }

    @Override
    public void destroy()
    {
        destroyed = true;
    }

    @Override
    public boolean isIdle()
    {
        return !destroyed && idle;
    }

    @Override
    public boolean isActive()
    {
        return !destroyed && !idle;
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    // For use by this package

    PersistitGroupCursor(PersistitAdapter adapter, Group group)
        throws PersistitException
    {
        this.adapter = adapter;
        this.group = group;
        this.controllingHKey = adapter.newKey();
        this.idle = true;
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
     *  - exchange == null iff this cursor is idle
     */

    private final PersistitAdapter adapter;
    private final Group group;
    private Exchange exchange;
    private Key controllingHKey;
    private PersistitHKey hKey;
    private boolean hKeyDeep;
    private GroupScan groupScan;
    private boolean idle;
    private boolean destroyed = false;

    // static state
    private static final PointTap TRAVERSE_COUNT = Tap.createCount("traverse_pgc");
    
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
            TRAVERSE_COUNT.hit();
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
            TRAVERSE_COUNT.hit();
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
                TRAVERSE_COUNT.hit();
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
