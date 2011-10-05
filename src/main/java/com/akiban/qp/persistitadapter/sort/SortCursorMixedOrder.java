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
import com.akiban.qp.persistitadapter.PersistitAdapterException;
import com.akiban.qp.row.Row;
import com.persistit.exception.PersistitException;

public abstract class SortCursorMixedOrder extends SortCursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        this.bindings = bindings;
        exchange.clear();
        try {
            // TODO: This should be done in the constructor, once bindings are available that early.
            for (int f = 0; f < sortFields; f++) {
                this.scanStates[f] = createScanState(this, f);
            }
            if (sortFields < keyFields) {
                this.scanStates[sortFields] = new MixedOrderScanStateRestOfKey(this, sortFields);
            }
            // TODO: ------------------------------------------------------------------------------------
            computeBoundaries();
            repositionExchange(0);
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
            if (more) {
                next = row();
                advance(lastKeyField);
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

    public abstract MixedOrderScanState createScanState(SortCursorMixedOrder cursor, int field) throws PersistitException;

    public abstract void computeBoundaries();

    // For use by subclasses

    protected SortCursorMixedOrder(RowGenerator rowGenerator, IndexKeyRange keyRange, API.Ordering ordering)
    {
        super(rowGenerator);
        this.keyRange = keyRange;
        this.ordering = ordering;
        sortFields = ordering.sortFields();
        keyFields =
            keyRange == null
            ? sortFields
            : keyRange.indexRowType().index().indexRowComposition().getLength();
        lastKeyField = keyFields - 1;
        scanStates = new MixedOrderScanState[keyFields];
    }

    // For use by this package

    IndexKeyRange keyRange()
    {
        return keyRange;
    }

    Bindings bindings()
    {
        return bindings;
    }

    API.Ordering ordering()
    {
        return ordering;
    }

    // For use by this class

    private void advance(int field) throws PersistitException
    {
        MixedOrderScanState scanState = scanStates[field];
        if (scanState.advance()) {
            if (field < lastKeyField) {
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
        for (int f = field; more && f < scanStates.length; f++) {
            more = scanStates[f].startScan();
        }
    }

    // Object state

    protected final IndexKeyRange keyRange;
    protected final API.Ordering ordering;
    protected final int sortFields; // Number of fields controlling output order.
    protected final int keyFields; // Number of fields in the key. keyFields >= sortFields.
    protected final int lastKeyField;
    protected final MixedOrderScanState[] scanStates;
    protected Bindings bindings;
    private boolean more;
}
