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

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;

public class ValuesRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return valuesHolder.toString();
    }

    // Row interface

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
        return null;
    }

    // ValuesRow interface

    public ValuesRow(RowType rowType, Object... values)
    {
        this.rowType = rowType;
        AkType[] akTypes = new AkType[rowType.nFields()];
        for (int i = 0; i < akTypes.length; i++) {
            akTypes[i] = rowType.typeAt(i);
        }
        this.valuesHolder = new RowValuesHolder(values, akTypes);
    }

    // Object state

    private final RowType rowType;
    private final RowValuesHolder valuesHolder;
}
