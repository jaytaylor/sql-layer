/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.qp.rowtype;

import com.foundationdb.ais.model.Table;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.types.TInstance;

import java.util.HashSet;
import java.util.Set;

public class ProductRowType extends CompoundRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("product(%s: %s x %s)", branchType, first(), second());
    }

    // RowType interface

    @Override
    public TInstance typeAt(int index) {
        if (index < first().nFields())
            return first().typeAt(index);
        return second().typeAt(index - first().nFields() + branchType.nFields());
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.LEFT_TYPE, first().getExplainer(context));
        explainer.addAttribute(Label.RIGHT_TYPE, second().getExplainer(context));
        return explainer;
    }

    // ProductRowType interface

    public RowType branchType()
    {
        return branchType;
    }

    public RowType leftType()
    {
        return first();
    }

    public RowType rightType()
    {
        return second();
    }

    public ProductRowType(Schema schema,
                          int typeId, 
                          RowType leftType, 
                          TableRowType branchType,
                          RowType rightType)
    {
        super(schema, typeId, leftType, rightType);
        this.branchType =
            branchType == null
            ? leafmostCommonType(leftType, rightType)
            : branchType;
        this.nFields = leftType.nFields() + rightType.nFields() - this.branchType.nFields();
    }

    // For use by this class

    private static RowType leafmostCommonType(RowType leftType, RowType rightType)
    {
        Set<Table> common = new HashSet<>(leftType.typeComposition().tables());
        common.retainAll(rightType.typeComposition().tables());
        Table leafmostCommon = null;
        for (Table table : common) {
            if (leafmostCommon == null || table.getDepth() > leafmostCommon.getDepth()) {
                leafmostCommon = table;
            }
        }
        assert leafmostCommon != null : String.format("leftType: %s, rightType: %s", leftType, rightType);
        return leftType.schema().tableRowType(leafmostCommon);
    }

    // Object state

    private final RowType branchType;
}
