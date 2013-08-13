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

package com.akiban.qp.rowtype;

import com.akiban.ais.model.HKey;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

public class HKeyRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return "HKey";
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return nFields;
    }

    @Override
    public TInstance typeInstanceAt(int index) {
        return hKey().column(index).tInstance();
    }

    @Override
    public AkType typeAt(int index)
    {
        return hKey().columnType(index);
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    // HKeyRowType interface
    
    public HKeyRowType(DerivedTypesSchema schema, HKey hKey)
    {
        super(schema, schema.nextTypeId());
        this.hKey = hKey;
        this.nFields = hKey.nColumns();
    }

    // Object state

    private final int nFields;
    private HKey hKey;
}
