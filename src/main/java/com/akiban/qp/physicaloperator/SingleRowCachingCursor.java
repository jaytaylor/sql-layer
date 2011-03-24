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

import com.akiban.qp.Cursor;
import com.akiban.qp.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;

public abstract class SingleRowCachingCursor implements Cursor
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
    public <T> T field(int i)
    {
        return (T) row.field(i);
    }

    @Override
    public HKey hKey()
    {
        return row.hKey();
    }

    // Cursor interface

    @Override
    public abstract void open();

    @Override
    public abstract boolean next();

    @Override
    public abstract void close();

    @Override
    public Row currentRow()
    {
        return row;
    }

    // SingleRowCachingCursor interface

    protected Row outputRow()
    {
        return row;
    }

    protected void outputRow(Row newRow)
    {
        row = newRow;
    }

    // Object state

    private Row row;
}
