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

package com.akiban.qp.physicaloperator;

import com.akiban.qp.*;
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.RowType;

public abstract class SingleRowCachingCursor extends ExecutionBase implements IndexCursor, GroupCursor
{
    // Object interface

    @Override
    public String toString()
    {
        return row == null ? null : row.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return row.rowType();
    }

    @Override
    public boolean ancestorOf(Row row)
    {
        return this.row.ancestorOf(row);
    }

    @Override
    public Object field(int i)
    {
        return row.field(i);
    }

    @Override
    public HKey hKey()
    {
        return row.hKey();
    }

    @Override
    public ManagedRow managedRow()
    {
        return row.managedRow();
    }

    // Cursor interface

    @Override
    public void open()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void open(HKey hKey)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public abstract boolean next();

    @Override
    public abstract void close();

    @Override
    public final Row currentRow()
    {
        return row;
    }

    // SingleRowCachingCursor interface

    protected ManagedRow outputRow()
    {
        return row.managedRow();
    }

    protected void outputRow(Row newRow)
    {
        row.set(newRow == null ? null : newRow.managedRow());
    }

    protected void outputRow(ManagedRow newRow)
    {
        row.set(newRow);
    }

    protected SingleRowCachingCursor(BTreeAdapter adapter)
    {
        super(adapter);
    }

    // Object state

    private final RowHolder<ManagedRow> row = new RowHolder<ManagedRow>();
}
