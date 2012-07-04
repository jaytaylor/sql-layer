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

import com.akiban.ais.model.UserTable;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

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

    @Override
    public AkCollator collatorAt(int index) {
        if (index < leftType.nFields())
            return leftType.collatorAt(index);
        return rightType.collatorAt(index - leftType.nFields() + branchType.nFields());
    }

    @Override
    public TInstance typeInstanceAt(int index) {
        if (index < leftType.nFields())
            return leftType.typeInstanceAt(index);
        return rightType.typeInstanceAt(index - leftType.nFields() + branchType.nFields());
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
