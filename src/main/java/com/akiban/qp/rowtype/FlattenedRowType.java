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

import com.akiban.ais.model.HKey;
import com.akiban.ais.model.UserTable;

import java.util.ArrayList;
import java.util.List;

public class FlattenedRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("flatten(%s, %s)", parent, child);
    }


    // RowType interface

    @Override
    public int nFields()
    {
        return parent.nFields() + child.nFields();
    }

    @Override
    public HKey hKey()
    {
        return child.hKey();
    }

    // FlattenedRowType interface

    public RowType parentType()
    {
        return parent;
    }

    public FlattenedRowType(SchemaOBSOLETE schema, int typeId, RowType parent, RowType child)
    {
        super(schema, typeId);
        assert parent.schema() == schema : parent;
        assert child.schema() == schema : child;
        this.parent = parent;
        this.child = child;
        List<UserTable> parentAndChildTables = new ArrayList<UserTable>(parent.typeComposition().tables());
        parentAndChildTables.addAll(child.typeComposition().tables());
        typeComposition(new TypeComposition(this, parentAndChildTables));
    }

    // Object state

    private final RowType parent;
    private final RowType child;
}
