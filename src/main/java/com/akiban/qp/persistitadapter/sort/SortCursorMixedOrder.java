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

package com.akiban.qp.persistitadapter.sort;

import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapterException;
import com.akiban.qp.row.Row;
import com.persistit.exception.PersistitException;

import java.util.ArrayList;
import java.util.List;

abstract class SortCursorMixedOrder extends SortCursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        this.bindings = bindings;
        exchange.clear();
        scanStates.clear();
        try {
            initializeScanStates();
            repositionExchange(0);
            justOpened = true;
        } catch (PersistitException e) {
            close();
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public Row next()
    {
        Row next = null;
        try {
            if (justOpened) {
                // Exchange is already positioned
                justOpened = false;
            } else {
                advance(scanStates.size() - 1);
            }
            if (more) {
                next = row();
            } else {
                close();
            }
        } catch (PersistitException e) {
            close();
            throw new PersistitAdapterException(e);
        }
        return next;
    }

    // SortCursorMixedOrder interface

    public static SortCursorMixedOrder create(PersistitAdapter adapter,
                                              IterationHelper iterationHelper,
                                              IndexKeyRange keyRange,
                                              API.Ordering ordering)
    {
        return
            // keyRange == null occurs when Sorter is used, (to sort an arbitrary input stream). There is no
            // IndexRowType in that case, so an IndexKeyRange can't be created.
            keyRange == null || keyRange.unbounded()
            ? new SortCursorMixedOrderUnbounded(adapter, iterationHelper, keyRange, ordering)
            : new SortCursorMixedOrderBounded(adapter, iterationHelper, keyRange, ordering);
    }

    public abstract void initializeScanStates() throws PersistitException;

    // For use by subclasses

    protected SortCursorMixedOrder(PersistitAdapter adapter,
                                   IterationHelper iterationHelper,
                                   IndexKeyRange keyRange,
                                   API.Ordering ordering)
    {
        super(adapter, iterationHelper);
        this.keyRange = keyRange;
        this.ordering = ordering;
        keyColumns =
            keyRange == null
            ? ordering.sortColumns()
            : keyRange.indexRowType().index().indexRowComposition().getLength();
    }

    // For use by this package

    IndexKeyRange keyRange()
    {
        return keyRange;
    }

    API.Ordering ordering()
    {
        return ordering;
    }

    // For use by subclasses

    protected int orderingColumns()
    {
        return ordering.sortColumns();
    }

    protected int boundColumns()
    {
        return keyRange.boundColumns();
    }

    protected int keyColumns()
    {
        return keyColumns;
    }

    // For use by this class

    private void advance(int field) throws PersistitException
    {
        MixedOrderScanState scanState = scanStates.get(field);
        if (scanState.advance()) {
            if (field < scanStates.size() - 1) {
                repositionExchange(field + 1);
            }
        } else {
            exchange.cut();
            if (field == 0) {
                more = false;
            } else {
                advance(field - 1);
            }
        }
    }

    private void repositionExchange(int field) throws PersistitException
    {
        more = true;
        for (int f = field; more && f < scanStates.size(); f++) {
            more = scanStates.get(f).startScan();
        }
    }

    // Object state

    protected final IndexKeyRange keyRange;
    protected final API.Ordering ordering;
    protected final List<MixedOrderScanState> scanStates = new ArrayList<MixedOrderScanState>();
    protected Bindings bindings;
    private final int keyColumns; // Number of columns in the key. keyFields >= orderingColumns.
    private boolean more;
}
