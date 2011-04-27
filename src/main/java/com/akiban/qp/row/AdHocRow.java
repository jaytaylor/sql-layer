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

import java.util.List;

public final class AdHocRow extends RowBase {

    private final RowType rowType;
    private final Object[] fields;


    public AdHocRow(RowType rowType, List<Object> fields) {
        this(rowType, fields.toArray());
    }

    public AdHocRow(RowType rowType, Object... fields) {
        if (fields.length != rowType.nFields()) {
            throw new IllegalArgumentException( String.format(
                    "incompatible fields lengths: rowType says %d but fields array says %d",
                            rowType.nFields(), fields.length
                    )
            );
        }
        this.rowType = rowType;
        this.fields = new Object[fields.length];
        System.arraycopy(fields, 0, this.fields, 0, fields.length);
    }

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public Object field(int i) {
        return fields[i];
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }
}
