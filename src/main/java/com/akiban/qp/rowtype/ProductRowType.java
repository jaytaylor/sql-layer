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
        return String.format("product(%s: %s x %s)", branchType, leftType, rightType);
    }


    // RowType interface

    @Override
    public int nFields()
    {
        return leftType.nFields() + rightType.nFields() - branchType.nFields();
    }

    // ProductRowType interface

    public RowType branchType()
    {
        return branchType;
    }

    public RowType leftType()
    {
        return leftType;
    }

    public RowType rightType()
    {
        return rightType;
    }

    public ProductRowType(Schema schema, int typeId, RowType branchType, RowType leftType, RowType rightType)
    {
        super(schema, typeId);
        assert branchType.schema() == schema : branchType;
        assert leftType.schema() == schema : leftType;
        assert rightType.schema() == schema : rightType;
        this.branchType = branchType;
        this.leftType = leftType;
        this.rightType = rightType;
        List<UserTable> tables = new ArrayList<UserTable>(leftType.typeComposition().tables());
        tables.addAll(rightType.typeComposition().tables());
        typeComposition(new TypeComposition(this, tables));
    }

    // Object state

    private final RowType branchType;
    private final RowType leftType;
    private final RowType rightType;
}
