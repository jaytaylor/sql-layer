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

import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;

public class FlattenedRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s, %s", parent, child);
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public Object field(int i)
    {
        Object field;
        if (i < nParentFields) {
            field = parent.get().field(i);
        } else {
            field = child.get().field(i - nParentFields);
        }
        return field;
    }

    @Override
    public HKey hKey()
    {
        return child.get().hKey();
    }

// FlattenedRow interface

    public FlattenedRow(FlattenedRowType rowType, ManagedRow parent, ManagedRow child)
    {
        this.rowType = rowType;
        this.parent.set(parent);
        this.child.set(child);
        this.nParentFields = parent.rowType().nFields();
    }

    // Object state

    private final FlattenedRowType rowType;
    private final RowHolder<ManagedRow> parent = new RowHolder<ManagedRow>();
    private final RowHolder<ManagedRow> child = new RowHolder<ManagedRow>();
    private final int nParentFields;
}
