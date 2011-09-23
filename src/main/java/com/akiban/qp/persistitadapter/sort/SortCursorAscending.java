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
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.server.PersistitValueValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class SortCursorAscending extends SortCursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        exchange.clear();
        exchange.append(Key.BEFORE);
    }

    @Override
    public Row next()
    {
        Row next = null;
        try {
            if (exchange.next(true)) {
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

    // SortCursorAscending interface

    public SortCursorAscending(Sorter sorter)
    {
        super(sorter);
    }
}
