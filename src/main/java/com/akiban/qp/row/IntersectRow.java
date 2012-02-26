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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.IntersectRowType;
import com.akiban.qp.rowtype.ProductRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ShareHolder;

public class IntersectRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        ValueTarget buffer = AkibanAppender.of(new StringBuilder()).asValueTarget();
        buffer.putString("(");
        int nFields = rowType.leftType().nFields() + rowType.rightType().nFields();
        for (int i = 0; i < nFields; i++) {
            if (i > 0) {
                buffer.putString(", ");
            }
            Converters.convert(eval(i), buffer);
        }
        buffer.putString(")");
        return buffer.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource eval(int i)
    {
        ValueSource source;
        if (i < nLeftFields) {
            source = left.isEmpty() ? NullValueSource.only() : left.get().eval(i);
        } else {
            source = right.isEmpty() ? NullValueSource.only() : right.get().eval(i - nLeftFields);
        }
        return source;
    }

    @Override
    public HKey hKey()
    {
        return null;
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable)
    {
        return containRealRowOf(left, right, userTable);
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        Row subRow;
        if (subRowType == rowType.leftType()) {
            subRow = left.get();
        } else if (subRowType == rowType.rightType()) {
            subRow = right.get();
        } else {
            // If the subRowType doesn't match leftType or rightType, then it might be buried deeper.
            subRow = left.get().subRow(subRowType);
            if (subRow == null) {
                subRow = right.get().subRow(subRowType);
            }
        }
        return subRow;
    }

    // ProductRow interface

    public IntersectRow(IntersectRowType rowType, Row left, Row right)
    {
        this.rowType = rowType;
        this.left.hold(left);
        this.right.hold(right);
        this.nLeftFields = rowType.leftType().nFields();
    }

    // Object state

    private final IntersectRowType rowType;
    private final ShareHolder<Row> left = new ShareHolder<Row>();
    private final ShareHolder<Row> right = new ShareHolder<Row>();
    private final int nLeftFields;
}
