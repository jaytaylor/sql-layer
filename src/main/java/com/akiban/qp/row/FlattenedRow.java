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
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.NullValueSource;

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
    public ValueSource bindSource(int i, Bindings bindings) {
        ValueSource source;
        if (i < nParentFields) {
            source = parent.isNull() ? NullValueSource.only() : parent.get().bindSource(i, bindings);
        } else {
            source = child.isNull() ? NullValueSource.only() : child.get().bindSource(i - nParentFields, bindings);
        }
        return source;
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        Row subRow;
        if (subRowType == rowType.parentType()) {
            subRow = parent.get();
        } else if (subRowType == rowType.childType()) {
            subRow = child.get();
        } else {
            // If the subRowType doesn't match leftType or rightType, then it might be buried deeper.
            subRow = parent.get().subRow(subRowType);
            if (subRow == null) {
                subRow = child.get().subRow(subRowType);
            }
        }
        return subRow;
    }

    // FlattenedRow interface

    public FlattenedRow(FlattenedRowType rowType, Row parent, Row child, HKey hKey)
    {
        this.rowType = rowType;
        this.parent.set(parent);
        this.child.set(child);
        this.nParentFields = rowType.parentType().nFields();
        this.hKey = hKey;
        if (parent != null && child != null) {
            // assert parent.runId() == child.runId();
        }
        if (parent != null && !rowType.parentType().equals(parent.rowType())) {
            throw new IllegalArgumentException("mismatched type between " +rowType+ " and parent " + parent.rowType());
        }
        super.runId(parent == null ? child.runId() : parent.runId());
    }

    // Object state

    private final FlattenedRowType rowType;
    private final RowHolder<Row> parent = new RowHolder<Row>();
    private final RowHolder<Row> child = new RowHolder<Row>();
    private final int nParentFields;
    private final HKey hKey;
}
