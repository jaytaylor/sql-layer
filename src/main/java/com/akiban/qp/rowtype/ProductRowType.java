
package com.akiban.qp.rowtype;

import com.akiban.ais.model.UserTable;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

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
    public AkType typeAt(int index) {
        if (index < first().nFields())
            return first().typeAt(index);
        return second().typeAt(index - first().nFields() + branchType.nFields());
    }

    @Override
    public TInstance typeInstanceAt(int index) {
        if (index < first().nFields())
            return first().typeInstanceAt(index);
        return second().typeInstanceAt(index - first().nFields() + branchType.nFields());
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

    public ProductRowType(DerivedTypesSchema schema, 
                          int typeId, 
                          RowType leftType, 
                          UserTableRowType branchType, 
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
        Set<UserTable> common = new HashSet<>(leftType.typeComposition().tables());
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
}
