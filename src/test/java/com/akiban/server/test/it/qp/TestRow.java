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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.RowValuesHolder;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
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
    public ValueSource eval(int i) {
        return valuesHolder.valueSourceAt(i);
    }

    @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // TestRow interface

    public TestRow(RowType rowType, Object[] fields, String hKeyString)
    {
        this.rowType = rowType;
        this.hKeyString = hKeyString;
        ArgumentValidation.isEQ("rowType.nFields()", rowType.nFields(), "fields.length", fields.length);
        this.valuesHolder = new RowValuesHolder(fields);
    }

    public TestRow(RowType rowType, Object[] fields) {
        this(rowType, fields, null);
    }

    public String persistityString() {
        return hKeyString;
    }

    // Object state

    private final RowType rowType;
    private final RowValuesHolder valuesHolder;
    private final String hKeyString;
}
