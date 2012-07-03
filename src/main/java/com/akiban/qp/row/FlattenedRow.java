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

package com.akiban.qp.row;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.ShareHolder;

public class FlattenedRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s, %s", parenth, childh);
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        ValueSource source;
        if (i < nParentFields) {
            source = parenth.isEmpty() ? NullValueSource.only() : parenth.get().eval(i);
        } else {
            source = childh.isEmpty() ? NullValueSource.only() : childh.get().eval(i - nParentFields);
        }
        return source;
    }

    @Override
    public PValueSource pvalue(int i) {
        PValueSource source;
        if (i < nParentFields) {
            source = parenth.isEmpty() ? nullPValue(i) : parenth.get().pvalue(i);
        } else {
            source = childh.isEmpty() ? nullPValue(i) : childh.get().pvalue(i - nParentFields);
        }
        return source;
    }

    private PValueSource nullPValue(int i) {
        PUnderlying underlying = rowType.typeInstanceAt(i).typeClass().underlyingType();
        return PValueSources.getNullSource(underlying);
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        Row subRow;
        if (subRowType == rowType.parentType()) {
            subRow = parenth.get();
        } else if (subRowType == rowType.childType()) {
            subRow = childh.get();
        } else {
            // If the subRowType doesn't match leftType or rightType, then it might be buried deeper.
            subRow = parenth.get().subRow(subRowType);
            if (subRow == null) {
                subRow = childh.get().subRow(subRowType);
            }
        }
        return subRow;
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable)
    {
        return     (parenth.isHolding() && parenth.get().rowType().hasUserTable() && parenth.get().rowType().userTable() == userTable)
                   || (childh.isHolding() && childh.get().rowType().hasUserTable() && childh.get().rowType().userTable() == userTable)
                   || (parenth.isHolding() && parenth.get().containsRealRowOf(userTable))
                   || (childh.isHolding() && childh.get().containsRealRowOf(userTable))
            ;
    }

    // FlattenedRow interface

    public FlattenedRow(FlattenedRowType rowType, Row parent, Row child, HKey hKey)
    {
        this.rowType = rowType;
        this.parenth.hold(parent);
        this.childh.hold(child);
        this.nParentFields = rowType.parentType().nFields();
        this.hKey = hKey;
        if (parent != null && child != null) {
            // assert parent.runId() == child.runId();
        }
        if (parent != null && !rowType.parentType().equals(parent.rowType())) {
            throw new IllegalArgumentException("mismatched type between " +rowType+ " and parent " + parent.rowType());
        }
    }

    // Object state

    private final FlattenedRowType rowType;
    private final ShareHolder<Row> parenth = new ShareHolder<Row>();
    private final ShareHolder<Row> childh = new ShareHolder<Row>();
    private final int nParentFields;
    private final HKey hKey;
}
