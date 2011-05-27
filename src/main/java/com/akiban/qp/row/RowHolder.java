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

public class RowHolder<MR extends Row>
{
    // Object interface

    @Override
    public String toString()
    {
        return row == null ? null : row.toString();
    }

    // RowHolder interface

    public MR get()
    {
        return row;
    }

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
