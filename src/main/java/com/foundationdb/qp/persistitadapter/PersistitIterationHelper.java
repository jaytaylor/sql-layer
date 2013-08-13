/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.qp.persistitadapter;

import com.foundationdb.qp.persistitadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.util.ShareHolder;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Key.Direction;
import com.persistit.exception.PersistitException;

public class PersistitIterationHelper implements IterationHelper
{
    @Override
    public Row row()
    {
        unsharedRow().get().copyFrom(exchange);
        return row.get();
    }

    @Override
    public void openIteration()
    {
        if (exchange == null) {
            exchange = adapter.takeExchange(indexRowType.index());
        }
    }

    @Override
    public void closeIteration()
    {
        if (row.isHolding()) {
            if (!row.isShared())
                adapter.returnIndexRow(row.get());
            row.release();
        }
        if (exchange != null) {
            adapter.returnExchange(exchange);
            exchange = null;
        }
    }

    @Override
    public Key key()
    {
        return exchange.getKey();
    }

    @Override
    public void clear()
    {
        exchange.clear();
    }

    @Override
    public boolean next(boolean deep)
    {
        try {
            return exchange.next(deep);
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(adapter.getSession(), e);
        }
    }

    @Override
    public boolean prev(boolean deep)
    {
        try {
            return exchange.previous(deep);
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(adapter.getSession(), e);
        }
    }

    @Override
    public boolean traverse(Direction dir, boolean deep)
    {
        try {
            return exchange.traverse(dir, deep);
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(adapter.getSession(), e);
        }
    }

    @Override
    public void preload(Direction dir, boolean deep) {
    }

    // PersistitIterationHelper interface

    public PersistitIterationHelper(PersistitAdapter adapter, IndexRowType indexRowType)
    {
        this.adapter = adapter;
        this.indexRowType = indexRowType.physicalRowType(); // In case we have a spatial index
        this.row = new ShareHolder<>(adapter.takeIndexRow(this.indexRowType));
    }

    // For use by this class

    private ShareHolder<PersistitIndexRow> unsharedRow()
    {
        if (row.isEmpty() || row.isShared()) {
            row.hold(adapter.takeIndexRow(indexRowType));
        }
        return row;
    }

    // Object state

    private final PersistitAdapter adapter;
    private final IndexRowType indexRowType;
    private final ShareHolder<PersistitIndexRow> row;
    private Exchange exchange;
}
