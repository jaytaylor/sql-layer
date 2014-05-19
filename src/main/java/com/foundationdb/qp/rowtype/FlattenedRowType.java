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

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.ais.model.HKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;

public class FlattenedRowType extends CompoundRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("flatten(%s, %s)", first(), second());
    }

    // RowType interface

    @Override
    public HKey hKey()
    {
        return second().hKey();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.PARENT_TYPE, first().getExplainer(context));
        explainer.addAttribute(Label.CHILD_TYPE, second().getExplainer(context));
        return explainer;
    }

    // FlattenedRowType interface

    public RowType parentType()
    {
        return first();
    }

    public RowType childType()
    {
        return second();
    }

    public FlattenedRowType(Schema schema, int typeId, RowType parent, RowType child)
    {
        super(schema, typeId, parent, child);
        // re-replace the type composition with the single branch type
        List<Table> parentAndChildTables = new ArrayList<>(parent.typeComposition().tables());
        parentAndChildTables.addAll(child.typeComposition().tables());
        typeComposition(new SingleBranchTypeComposition(this, parentAndChildTables));
        
    }
}
