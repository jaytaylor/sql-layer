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
import com.akiban.qp.rowtype.ProductRowType;
import com.akiban.qp.rowtype.RowType;

public class ProductRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('(');
        int nFields = rowType.leftType().nFields() + rowType.rightType().nFields() - rowType.branchType().nFields();
        for (int i = 0; i < nFields; i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append(field(i, null));
        }
        buffer.append(')');
        return buffer.toString();
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
        if (i < nLeftFields) {
            field = left.isNull() ? null : left.get().field(i, bindings);
        } else {
            field = right.isNull() ? null : right.get().field(i - firstRightFieldOffset, bindings);
        }
        return field;
    }

    @Override
    public HKey hKey()
    {
        return null;
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

    public ProductRow(ProductRowType rowType, Row left, Row right)
    {
        this.rowType = rowType;
        this.left.set(left);
        this.right.set(right);
        this.nLeftFields = rowType.leftType().nFields();
        this.firstRightFieldOffset = nLeftFields - rowType.branchType().nFields();
        if (left != null && right != null) {
            // assert left.runId() == right.runId();
        }
        super.runId(left == null ? right.runId() : left.runId());
    }

    // Object state

    private final ProductRowType rowType;
    private final RowHolder<Row> left = new RowHolder<Row>();
    private final RowHolder<Row> right = new RowHolder<Row>();
    private final int nLeftFields;
    private final int firstRightFieldOffset;
}
