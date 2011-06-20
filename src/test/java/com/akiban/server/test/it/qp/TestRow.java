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

package com.akiban.server.test.it.qp;

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.ArgumentValidation;

public class TestRow extends AbstractRow
{
    // RowBase interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public Object field(int i, Bindings bindings)
    {
        return fields[i];
    }

    @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // TestRow interface

    public TestRow(RowType rowType, String hKeyString)
    {
        this.rowType = rowType;
        this.fields = new Object[rowType.nFields()];
        this.hKeyString = hKeyString;
    }

    public TestRow(RowType rowType, Object[] fields, String hKeyString)
    {
        this(rowType, hKeyString);
        ArgumentValidation.isEQ("rowType.nFields()", rowType.nFields(), "fields.length", fields.length);
        System.arraycopy(fields, 0, this.fields, 0, fields.length);
    }

    public TestRow(RowType rowType, Object[] fields) {
        this(rowType, fields, null);
    }

    public String persistityString() {
        return hKeyString;
    }

    // Object state

    private final RowType rowType;
    private final Object[] fields;
    private final String hKeyString;
}
