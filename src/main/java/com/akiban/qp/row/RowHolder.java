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

package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;

public class RowHolder<MR extends ManagedRow> implements Row
{
    // Object interface

    @Override
    public String toString()
    {
        return row.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return row.rowType();
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
    public boolean ancestorOf(Row that)
    {
        return row.ancestorOf(that);
    }

    @Override
    public MR managedRow()
    {
        return row;
    }

    // RowHolder interface

    public void set(MR newRow)
    {
        if (row != null) {
            row.release();
        }
        if (newRow != null) {
            newRow.share();
        }
        row = newRow;
    }

    public boolean isNull()
    {
        return row == null;
    }

    public boolean isNotNull()
    {
        return row != null;
    }

    public RowHolder(MR row)
    {
        set(row);
    }

    public RowHolder()
    {
        set(null);
    }

    // Object state

    private MR row;
}
