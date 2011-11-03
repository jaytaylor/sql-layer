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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.NullValueSource;
import com.akiban.util.ShareHolder;

public class FlattenedRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s, %s", parenth, childh);
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        ValueSource source;
        if (i < nParentFields) {
            source = parenth.isEmpty() ? NullValueSource.only() : parenth.get().eval(i);
        } else {
            source = childh.isEmpty() ? NullValueSource.only() : childh.get().eval(i - nParentFields);
        }
        return source;
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable) {
        return containRealRowOf(parenth, childh, userTable);
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
            subRow = parenth.get();
        } else if (subRowType == rowType.childType()) {
            subRow = childh.get();
        } else {
            // If the subRowType doesn't match leftType or rightType, then it might be buried deeper.
            subRow = parenth.get().subRow(subRowType);
            if (subRow == null) {
                subRow = childh.get().subRow(subRowType);
            }
        }
        return subRow;
    }

    // FlattenedRow interface

    public FlattenedRow(FlattenedRowType rowType, Row parent, Row child, HKey hKey)
    {
        this.rowType = rowType;
        this.parenth.hold(parent);
        this.childh.hold(child);
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
    private final ShareHolder<Row> parenth = new ShareHolder<Row>();
    private final ShareHolder<Row> childh = new ShareHolder<Row>();
    private final int nParentFields;
    private final HKey hKey;
}
