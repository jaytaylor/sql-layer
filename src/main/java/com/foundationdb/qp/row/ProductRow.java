/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.row;

import com.foundationdb.qp.rowtype.ProductRowType;
import com.foundationdb.server.types.ValueTarget;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.util.AkibanAppender;

public class ProductRow extends CompoundRow
{
    // Object interface

    @Override
    public String toString()
    {
        ValueTarget buffer = AkibanAppender.of(new StringBuilder()).asValueTarget();
        ProductRowType type = (ProductRowType)rowType();
        buffer.putString("(");
        int nFields = type.leftType().nFields() + type.rightType().nFields() - type.branchType().nFields();
        for (int i = 0; i < nFields; i++) {
            if (i > 0) {
                buffer.putString(", ");
            }
            Converters.convert(eval(i), buffer);
        }
        buffer.putString(")");
        return buffer.toString();
    }

    // ProductRow interface

    public ProductRow(ProductRowType rowType, Row left, Row right)
    {
        super (rowType, left, right);
        this.rowOffset = firstRowFields() - rowType.branchType().nFields();
        if (left != null && right != null) {
            // assert left.runId() == right.runId();
        }
    }
}
