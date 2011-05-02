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
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.GroupCursor;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A PersistitGroupCursor can be used in three ways:
 * 1) Scan the entire group: This occurs when neither overloading of bind is involed before open()
 * 2) For a given hkey, find the row and its descendents: This occurs when bind(HKey) is called.
 * 3) As an hkey-equivalent index: This occurs when bind(IndexKeyRange) is called. The index restriction is
 *    on columns of the hkey. Find the qualifying rows and all descendents.
 */


class PersistitGroupCursor implements GroupCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        assert exchange == null;
        try {
            exchange = adapter.takeExchange(groupTable).clear();
            groupScan =
                hKeyRange == null && hKey == null ? new FullScan() :
                hKeyRange == null ? new HKeyAndDescendentsScan(hKey) :
                hKeyRange.unbounded() ? new FullScan() : new HKeyRangeAndDescendentsScan(hKeyRange);
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public boolean next()
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
            if (LOG.isInfoEnabled()) {
                LOG.info("PersistitGroupCursor: {}", next ? row : null);
            }
            return next;
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
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
            hKey = null;
            hKeyRange = null;
            groupScan = null;
        }
    }

    @Override
    public Row currentRow()
    {
        return row.get();
    }

    // GroupCursor interface

    @Override
    public void bind(HKey hKey)
    {
        assert this.hKeyRange == null && this.hKey == null;
        this.hKey = (PersistitHKey) hKey;
    }

    @Override
    public void bind(IndexKeyRange hKeyRange)
    {
        assert this.hKeyRange == null && this.hKey == null;
        this.hKeyRange = hKeyRange;
    }

    // For use by this package

    PersistitGroupCursor(PersistitAdapter adapter, GroupTable groupTable, boolean reverse) throws PersistitException
    {
        this.adapter = adapter;
        this.groupTable = groupTable;
        this.reverse = reverse;
        this.row = new RowHolder<PersistitGroupRow>(adapter.newGroupRow());
        this.controllingHKey = new Key(adapter.persistit.getDb());
    }

    // For use by this class

    private RowHolder<PersistitGroupRow> unsharedRow()
    {
        if (row.get().isShared()) {
            row.set(adapter.newGroupRow());
        }
        return row;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(PersistitGroupCursor.class);
    // Used by HKeyRangeAndDescendentsScan.
    // Should be zero, but Exchange.traverse doesn't update the key if we ask for 0 value bytes.
    private static final int VALUE_BYTES = 1;

    // Object state

    /*
     * 1) Scan entire group: Initialize exchange to Key.BEFORE and keep going forward, doing a deep traversal,
     *    until there are no more rows.
     *
     * 2) Scan one hkey and descendents: The binding is stored in singleHKeyRestriction until the scan begins.
     *    Then, the key is copied to the exchange, to begin the scan, and to controllingHKey to determine
     *    when the scan should end.
     *
     * 3) Index range scan: The binding is stored in hKeyRange until the scan begins. The exchange is used with
     *    hKeyRangeFilter to implement the range restriction, alternating with deep traversal. For each
     *    record that hKeyRangeFilter, the current hKey is copied to conrollingHKey, and this is used to
     *    identify descendents, as in (2).
     *
     *  General:
     *  - exchange == null iff this cursor is open
     */

    Exchange exchange() {
        return exchange;
    }

    RowHolder<PersistitGroupRow> currentHeldRow() {
        return row;
    }

    PersistitAdapter adapter() {
        return adapter;
    }

    private final PersistitAdapter adapter;
    private final GroupTable groupTable;
    private final boolean reverse;
    private final RowHolder<PersistitGroupRow> row;
    private Exchange exchange;
    private Key controllingHKey;
    private PersistitHKey hKey;
    private IndexKeyRange hKeyRange;
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
            if (reverse) {
                exchange.getKey().append(Key.AFTER);
                direction = Key.LT;
            } else {
                exchange.getKey().append(Key.BEFORE);
                direction = Key.GT;
            }
        }

        private final Key.Direction direction;
    }

    private class HKeyAndDescendentsScan implements GroupScan
    {
        @Override
        public void advance() throws PersistitException, InvalidOperationException
        {
            if (first) {
                if (!exchange.traverse(firstDirection, true)) {
                    close();
                }
                first = false;
            } else {
                if (!exchange.traverse(subsequentDirection, true) ||
                    exchange.getKey().firstUniqueByteIndex(controllingHKey) < controllingHKey.getEncodedSize()) {
                    close();
                }
            }
        }

        HKeyAndDescendentsScan(PersistitHKey singleHKeyRestriction) throws PersistitException
        {
            singleHKeyRestriction.copyTo(exchange.getKey());
            singleHKeyRestriction.copyTo(controllingHKey);
            if (reverse) {
                firstDirection = Key.LTEQ;
                subsequentDirection = Key.LT;
            } else {
                firstDirection = Key.GTEQ;
                subsequentDirection = Key.GT;
            }
        }

        private final Key.Direction firstDirection;
        private final Key.Direction subsequentDirection;
        private boolean first = true;
    }

    private class HKeyRangeAndDescendentsScan implements GroupScan
    {
        @Override
        public void advance() throws PersistitException, InvalidOperationException
        {
            if (first) {
                if (!exchange.traverse(firstDirection, hKeyRangeFilter, VALUE_BYTES)) {
                    close();
                } else {
                    exchange.getKey().copyTo(controllingHKey);
                }
                first = false;
            } else {
                if (!exchange.traverse(subsequentDirection, true)) {
                    close();
                } else {
                    if (exchange.getKey().firstUniqueByteIndex(controllingHKey) < controllingHKey.getEncodedSize()) {
                        // Current key is not a descendent of the controlling hkey
                        if (hKeyRangeFilter.selected(exchange.getKey())) {
                            // But it is still selected by hKeyRange
                            exchange.getKey().copyTo(controllingHKey);
                        } else {
                            // Not selected. Could be that we need to skip over some orphans.
                            if (!exchange.traverse(subsequentDirection, hKeyRangeFilter, VALUE_BYTES)) {
                                close();
                            }
                        }
                    }
                }
            }
        }

        HKeyRangeAndDescendentsScan(IndexKeyRange hKeyRange) throws PersistitException
        {
            UserTable table = (hKeyRange.lo() == null ? hKeyRange.hi() : hKeyRange.lo()).table();
            RowDef rowDef = (RowDef) table.rowDef();
            hKeyRangeFilter = adapter.filterFactory.computeHKeyFilter(exchange.getKey(), rowDef, hKeyRange);
            if (reverse) {
                firstDirection = Key.LTEQ;
                subsequentDirection = Key.LT;
            } else {
                firstDirection = Key.GTEQ;
                subsequentDirection = Key.GT;
            }
        }

        private final KeyFilter hKeyRangeFilter;
        private final Key.Direction firstDirection;
        private final Key.Direction subsequentDirection;
        private boolean first = true;
    }
}
