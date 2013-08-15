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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.RowValuesHolder;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.ValueSource;

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
        this(rowType, new RowValuesHolder(fields), hKeyString);
    }

    public TestRow(RowType rowType, Object[] fields) {
        this(rowType, fields, null);
    }

    public TestRow(RowType rowType, RowValuesHolder valuesHolder, String hKeyString) {
        this.rowType = rowType;
        this.valuesHolder = valuesHolder;
        this.hKeyString = hKeyString;
    }

    public String persistityString() {
        return hKeyString;
    }

    // Object state

    private final RowType rowType;
    private final RowValuesHolder valuesHolder;
    private final String hKeyString;
}
