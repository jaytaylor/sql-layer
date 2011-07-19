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

public class ValuesRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("values(%d)", nfields);
    }


    // RowType interface

    @Override
    public int nFields()
    {
        return nfields;
    }

    // ValuesRowType interface

    public ValuesRowType(Schema schema, int typeId, int nfields)
    {
        super(schema, typeId);
        this.nfields = nfields;
    }

    // Object state

    private final int nfields;
}
