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

package com.akiban.qp.rowtype;

import com.akiban.server.types.AkType;

import java.util.Arrays;

public class ValuesRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return "values(" + Arrays.toString(types) + ')';
    }


    // RowType interface

    @Override
    public int nFields()
    {
        return types.length;
    }

    @Override
    public AkType typeAt(int index) {
        return types[index];
    }

    // ValuesRowType interface

    public ValuesRowType(DerivedTypesSchema schema, int typeId, AkType... types)
    {
        super(schema, typeId);
        this.types = types;
    }

    // Object state

    private final AkType[] types;
}
