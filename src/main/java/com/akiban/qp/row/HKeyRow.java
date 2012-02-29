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
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;

public class HKeyRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return hKey.toString();
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
        return hKey.eval(i);
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        throw new UnsupportedOperationException();
    }

    // ProductRow interface

    public HKeyRow(HKeyRowType rowType, HKey hKey)
    {
        this.rowType = rowType;
        this.hKey = hKey;
    }
    
    // Object state

    private final HKeyRowType rowType;
    private HKey hKey;
}
