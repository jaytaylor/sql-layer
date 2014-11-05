/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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
import com.foundationdb.qp.row.ValuesRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;

public class TestRow extends AbstractRow
{
    // Row interface

    @Override
    public RowType rowType()
    {
        return valueRow.rowType();
    }

     @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

     @Override
     public boolean isBindingsSensitive() {
         return false;
     }

     @Override
     public ValueSource uncheckedValue(int i) {
         return valueRow.value(i);
     }

    // KeyUpdateRow interface

    public TestRow(RowType rowType, Object[] fields, String hKeyString)
    {
        this(rowType, new ValuesRow(rowType, fields), hKeyString);
    }

    public TestRow(RowType rowType, Object... fields) {
        this(rowType, fields, null);
    }

    public TestRow(RowType rowType, ValuesRow valueRow, String hKeyString) {
        this.valueRow = valueRow;
        this.hKeyString = hKeyString;
    }

    public String persistityString() {
        return hKeyString;
    }


    // Object state

    private final ValuesRow valueRow;
    private final String hKeyString;
}
