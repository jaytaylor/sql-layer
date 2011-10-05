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
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.server.PersistitValueValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.optimizer.plan.Sort;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public abstract class SortCursor implements Cursor
{
    // Cursor interface

    @Override
    public final void close()
    {
        rowGenerator.close();
    }

    // SortCursor interface

    public static SortCursor create(IndexKeyRange keyRange, API.Ordering ordering, RowGenerator rowGenerator)
    {
        boolean allAscending = true;
        boolean allDescending = true;
        for (int i = 0; i < ordering.sortFields(); i++) {
            if (ordering.ascending(i)) {
                allDescending = false;
            } else {
                allAscending = false;
            }
        }
        return
            allAscending ?
            new SortCursorAscending(rowGenerator) :
            allDescending ?
            new SortCursorDescending(rowGenerator) :
            // keyRange == null occurs when Sorter is used, (to sort an arbitrary input stream). There is no
            // IndexRowType in that case, so an IndexKeyRange can't be created.
            keyRange == null || keyRange.unbounded() ?
            new SortCursorMixedOrderUnbounded(rowGenerator, keyRange, ordering) :
            new SortCursorMixedOrderBounded(rowGenerator, keyRange, ordering);
    }

    // For use by subclasses

    protected SortCursor(RowGenerator rowGenerator)
    {
        this.rowGenerator = rowGenerator;
        this.exchange = rowGenerator.exchange();
    }

    protected Row row() throws PersistitException
    {
        return rowGenerator.row();
    }

    // Object state

    protected final Exchange exchange;
    protected final RowGenerator rowGenerator;
}
