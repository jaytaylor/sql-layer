/** *
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

import com.akiban.qp.physicaloperator.Bindings;
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
    public Object field(int i, Bindings bindings)
    {
        Object field;
        if (i < nParentFields) {
            field = parent.isNull() ? null : parent.get().field(i, bindings);
        } else {
            field = child.isNull() ? null : child.get().field(i - nParentFields, bindings);
        }
        return field;
    }

    @Override
    public HKey hKey()
    {
        return child.get().hKey();
    }

    // FlattenedRow interface

    public FlattenedRow(FlattenedRowType rowType, Row parent, Row child)
    {
        this.rowType = rowType;
        this.parent.set(parent);
        this.child.set(child);
        this.nParentFields = rowType.parentType().nFields();
    }

    // Object state

    private final FlattenedRowType rowType;
    private final RowHolder<Row> parent = new RowHolder<Row>();
    private final RowHolder<Row> child = new RowHolder<Row>();
    private final int nParentFields;
}
