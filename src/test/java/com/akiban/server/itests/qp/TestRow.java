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

package com.akiban.server.itests.qp;

import com.akiban.qp.row.HKey;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.scan.NewRow;

public class TestRow extends RowBase
{
    // RowBase interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public Object field(int i)
    {
        return fields[i];
    }

    @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // TestRow interface

    public TestRow(RowType rowType)
    {
        this.rowType = rowType;
        this.fields = new Object[rowType.nFields()];
    }

    public TestRow(RowType rowType, Object[] fields)
    {
        this(rowType);
        assert rowType.nFields() == fields.length;
        System.arraycopy(fields, 0, this.fields, 0, fields.length);
    }

    public TestRow(RowType rowType, NewRow row)
    {
        this(rowType);
        assert rowType.nFields() == row.getFields().size();
        for (int i = 0; i < rowType.nFields(); i++) {
            fields[i] = row.get(i);
        }
    }

    // Object state

    private final RowType rowType;
    private final Object[] fields;
}
