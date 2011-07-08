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

import com.akiban.ais.model.UserTable;

import java.util.ArrayList;
import java.util.List;

public class ProductRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("product(%s x %s)", left, right);
    }


    // RowType interface

    @Override
    public int nFields()
    {
        return left.nFields() + right.nFields();
    }

    // ProductRowType interface

    public RowType leftType()
    {
        return left;
    }

    public RowType rightType()
    {
        return right;
    }

    public ProductRowType(SchemaOBSOLETE schema, int typeId, RowType left, RowType right)
    {
        super(schema, typeId);
        assert left.schema() == schema : left;
        assert right.schema() == schema : right;
        this.left = left;
        this.right = right;
        List<UserTable> parentAndChildTables = new ArrayList<UserTable>(left.typeComposition().tables());
        parentAndChildTables.addAll(right.typeComposition().tables());
        typeComposition(new TypeComposition(this, parentAndChildTables));
    }

    // Object state

    private final RowType left;
    private final RowType right;
}
