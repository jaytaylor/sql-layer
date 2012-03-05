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
import com.akiban.server.types.AkType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    @Override
    public AkType typeAt(int index) {
        if (index < leftType.nFields())
            return leftType.typeAt(index);
        return rightType.typeAt(index - leftType.nFields() + branchType.nFields());
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

    public ProductRowType(DerivedTypesSchema schema, 
                          int typeId, 
                          RowType leftType, 
                          UserTableRowType branchType, 
                          RowType rightType)
    {
        super(schema, typeId);
        assert leftType.schema() == schema : leftType;
        assert rightType.schema() == schema : rightType;
        this.branchType =
            branchType == null
            ? leafmostCommonType(leftType, rightType)
            : branchType;
        this.leftType = leftType;
        this.rightType = rightType;
        List<UserTable> tables = new ArrayList<UserTable>(leftType.typeComposition().tables());
        tables.addAll(rightType.typeComposition().tables());
        typeComposition(new TypeComposition(this, tables));
    }

    // For use by this class

    private static RowType leafmostCommonType(RowType leftType, RowType rightType)
    {
        Set<UserTable> common = new HashSet<UserTable>(leftType.typeComposition().tables());
        common.retainAll(rightType.typeComposition().tables());
        UserTable leafmostCommon = null;
        for (UserTable table : common) {
            if (leafmostCommon == null || table.getDepth() > leafmostCommon.getDepth()) {
                leafmostCommon = table;
            }
        }
        assert leafmostCommon != null : String.format("leftType: %s, rightType: %s", leftType, rightType);
        return ((Schema)leftType.schema()).userTableRowType(leafmostCommon);
    }

    // Object state

    private final RowType branchType;
    private final RowType leftType;
    private final RowType rightType;
}
