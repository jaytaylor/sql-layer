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

public class IntersectInBranchRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("intersect(%s, %s)", leftType, rightType);
    }


    // RowType interface

    @Override
    public int nFields()
    {
        return leftType.nFields() + rightType.nFields();
    }

    @Override
    public AkType typeAt(int index)
    {
        if (index < leftType.nFields()) {
            return leftType.typeAt(index);
        }
        return rightType.typeAt(index - leftType.nFields());
    }

    // IntersectInBranchRowType interface

    public RowType leftType()
    {
        return leftType;
    }

    public RowType rightType()
    {
        return rightType;
    }

    public IntersectInBranchRowType(DerivedTypesSchema schema, int typeId, RowType leftType, RowType rightType)
    {
        super(schema, typeId);
        assert leftType.schema() == schema : leftType;
        assert rightType.schema() == schema : rightType;
        this.leftType = leftType;
        this.rightType = rightType;
    }

    // Object state

    private final RowType leftType;
    private final RowType rightType;
}
