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

import com.akiban.qp.operator.API;
import com.akiban.qp.persistitadapter.PersistitAdapterException;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.row.Row;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class SortCursorMixedOrder extends SortCursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        exchange.clear();
        try {
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
                advance(lastField);
            } else {
                close();
            }
        } catch (PersistitException e) {
            close();
            throw new PersistitAdapterException(e);
        }
        return next;
    }

    // SortCursorAscending interface

    public SortCursorMixedOrder(RowGenerator rowGenerator, API.Ordering ordering)
    {
        super(rowGenerator);
        int sortFields = ordering.sortFields();
        lastField = sortFields - 1;
        scanStates = new ScanState[sortFields];
        try {
            for (int f = 0; f < sortFields; f++) {
                scanStates[f] = new ScanState(ordering.ascending(f), f);
            }
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    // For use by this class

    private void advance(int field) throws PersistitException
    {
        ScanState scanState = scanStates[field];
        if (scanState.advance()) {
            if (field < lastField) {
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
        for (int f = field; f < scanStates.length; f++) {
            scanStates[f].startScan();
        }
    }

    // Object state

    private final int lastField;
    private final ScanState[] scanStates;
    private boolean more;

    // Inner classes

    private class ScanState
    {
        public void startScan() throws PersistitException
        {
            if (ascending) {
                exchange.append(Key.BEFORE);
                more = exchange.next(false);
            } else {
                exchange.append(Key.AFTER);
                more = exchange.previous(false);
            }
        }

        public boolean advance() throws PersistitException
        {
            return ascending ? exchange.next(false) : exchange.previous(false);
        }

        public ScanState(boolean ascending, int field) throws PersistitException
        {
            this.ascending = ascending;
            this.field = field;
            startScan();
        }

        private final int field;
        private final boolean ascending;
    }
}
