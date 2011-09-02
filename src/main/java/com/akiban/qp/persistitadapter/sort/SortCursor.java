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

import com.akiban.qp.persistitadapter.PersistitAdapterException;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.server.PersistitValueValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class SortCursor implements Cursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        sorter.exchange.clear();
        sorter.exchange.append(Key.BEFORE);
    }

    @Override
    public Row next()
    {
        Row next = null;
        try {
            if (sorter.exchange.next(true)) {
                sorter.exchange.fetch();
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

    @Override
    public void close()
    {
        sorter.close();
    }

    // SortCursor interface

    public SortCursor(Sorter sorter)
    {
        this.sorter = sorter;
        this.valueSource = new PersistitValueValueSource();
        this.valueSource.attach(sorter.value);
    }

    // For use by this class

    private Row row()
    {
        ValuesHolderRow row = new ValuesHolderRow(sorter.rowType);
        sorter.value.setStreamMode(true);
        for (int i = 0; i < sorter.rowFields; i++) {
            ValueHolder valueHolder = row.holderAt(i);
            valueSource.expectedType(sorter.fieldTypes[i]);
            valueHolder.copyFrom(valueSource);
        }
        return row;
    }

    // Object state

    private final Sorter sorter;
    private final PersistitValueValueSource valueSource;
}
