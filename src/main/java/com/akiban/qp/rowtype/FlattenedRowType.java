/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.rowtype;

import com.akiban.ais.model.HKey;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.exec.Plannable;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.explain.*;
import java.math.BigDecimal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlattenedRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("flatten(%s, %s)", parent, child);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        FlattenedRowType that = (FlattenedRowType) o;

        if (child != null ? !child.equals(that.child) : that.child != null) return false;
        if (parent != null ? !parent.equals(that.parent) : that.parent != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (parent != null ? parent.hashCode() : 0);
        result = 31 * result + (child != null ? child.hashCode() : 0);
        return result;
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return nFields;
    }

    @Override
    public AkType typeAt(int index) {
        if (index < parent.nFields())
            return parent.typeAt(index);
        return child.typeAt(index - parent.nFields());
    }

    @Override
    public TInstance typeInstanceAt(int index) {
        if (index < parent.nFields())
            return parent.typeInstanceAt(index);
        return child.typeInstanceAt(index - parent.nFields());
    }

    @Override
    public HKey hKey()
    {
        return child.hKey();
    }
    
    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.PARENT_TYPE, parent.getExplainer(context));
        explainer.addAttribute(Label.CHILD_TYPE, child.getExplainer(context));
        return explainer;
    }

    // FlattenedRowType interface

    public RowType parentType()
    {
        return parent;
    }

    public RowType childType()
    {
        return child;
    }

    public FlattenedRowType(DerivedTypesSchema schema, int typeId, RowType parent, RowType child)
    {
        super(schema, typeId);
        assert parent.schema() == schema : parent;
        assert child.schema() == schema : child;
        this.parent = parent;
        this.child = child;
        this.nFields = parent.nFields() + child.nFields();
        List<UserTable> parentAndChildTables = new ArrayList<UserTable>(parent.typeComposition().tables());
        parentAndChildTables.addAll(child.typeComposition().tables());
        typeComposition(new SingleBranchTypeComposition(this, parentAndChildTables));
    }

    // Object state

    private final RowType parent;
    private final RowType child;
    private final int nFields;
}
