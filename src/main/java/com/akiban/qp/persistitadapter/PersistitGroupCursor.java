/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Group;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.GroupCursor;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
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
            if (next) {
                groupScan.advance();
                next = !idle;
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
        this.row = new ShareHolder<PersistitGroupRow>(adapter.newGroupRow());
        this.controllingHKey = adapter.newKey();
        this.idle = true;
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
     *  - exchange == null iff this cursor is idle
     */

    private final PersistitAdapter adapter;
    private final Group group;
    private final ShareHolder<PersistitGroupRow> row;
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
