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

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.server.PersistitValueValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.persistit.Exchange;

abstract class SortCursor implements Cursor
{
    // Cursor interface

    @Override
    public final void close()
    {
        rowGenerator.close();
    }

    // For use by subclasses

    protected SortCursor(RowGenerator rowGenerator)
    {
        this.rowGenerator = rowGenerator;
        this.exchange = rowGenerator.exchange();
    }

    protected Row row()
    {
        return rowGenerator.row();
    }

    // Object state

    protected final Exchange exchange;
    private final RowGenerator rowGenerator;
}
