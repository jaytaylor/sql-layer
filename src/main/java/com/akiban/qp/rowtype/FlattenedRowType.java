
package com.akiban.qp.rowtype;

import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.HKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;

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

    public FlattenedRowType(DerivedTypesSchema schema, int typeId, RowType parent, RowType child)
    {
        super(schema, typeId, parent, child);
        // re-replace the type composition with the single branch type
        List<UserTable> parentAndChildTables = new ArrayList<>(parent.typeComposition().tables());
        parentAndChildTables.addAll(child.typeComposition().tables());
        typeComposition(new SingleBranchTypeComposition(this, parentAndChildTables));
        
    }
}
