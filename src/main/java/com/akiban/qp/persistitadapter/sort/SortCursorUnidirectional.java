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

import com.akiban.qp.operator.Bindings;
import com.akiban.qp.persistitadapter.PersistitAdapterException;
import com.akiban.qp.row.Row;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;

public abstract class SortCursorUnidirectional extends SortCursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        exchange.clear();
        exchange.append(startBoundary);
        keyFilter = rowGenerator.keyFilter(bindings);
    }

    @Override
    public Row next()
    {
        Row next = null;
        if (exchange != null) {
            try {
                if (keyFilter == null
                    ? exchange.traverse(direction, true)
                    : exchange.traverse(direction, keyFilter, FETCH_NO_BYTES)) {
                    next = row();
                } else {
                    close();
                }
            } catch (PersistitException e) {
                close();
                throw new PersistitAdapterException(e);
            }
        }
        return next;
    }

    // SortCursorUnidirectional interface

    protected SortCursorUnidirectional(RowGenerator rowGenerator, Key.EdgeValue startBoundary, Key.Direction direction)
    {
        super(rowGenerator);
        this.startBoundary = startBoundary;
        this.direction = direction;
    }

    // Class state

    private static final int FETCH_NO_BYTES = 0;

    // Object state

    private final Key.EdgeValue startBoundary;
    private final Key.Direction direction;
    private KeyFilter keyFilter;
}
