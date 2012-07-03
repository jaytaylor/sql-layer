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

import com.akiban.qp.rowtype.ProductRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ShareHolder;

public class ProductRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        ValueTarget buffer = AkibanAppender.of(new StringBuilder()).asValueTarget();
        buffer.putString("(");
        int nFields = rowType.leftType().nFields() + rowType.rightType().nFields() - rowType.branchType().nFields();
        for (int i = 0; i < nFields; i++) {
            if (i > 0) {
                buffer.putString(", ");
            }
            Converters.convert(eval(i), buffer);
        }
        buffer.putString(")");
        return buffer.toString();
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
        if (i < nLeftFields) {
            source = left.isEmpty() ? NullValueSource.only() : left.get().eval(i);
        } else {
            source = right.isEmpty() ? NullValueSource.only() : right.get().eval(i - firstRightFieldOffset);
        }
        return source;
    }

    @Override
    public PValueSource pvalue(int i) {
        PValueSource source;
        if (i < nLeftFields) {
            source = left.isEmpty() ? nullPValue(i) : left.get().pvalue(i);
        } else {
            source = right.isEmpty() ? nullPValue(i - firstRightFieldOffset) : right.get().pvalue(i - firstRightFieldOffset);
        }
        return source;
    }

    @Override
    public HKey hKey()
    {
        return null;
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        Row subRow;
        if (subRowType == rowType.leftType()) {
            subRow = left.get();
        } else if (subRowType == rowType.rightType()) {
            subRow = right.get();
        } else {
            // If the subRowType doesn't match leftType or rightType, then it might be buried deeper.
            subRow = left.get().subRow(subRowType);
            if (subRow == null) {
                subRow = right.get().subRow(subRowType);
            }
        }
        return subRow;
    }

    // ProductRow interface

    public ProductRow(ProductRowType rowType, Row left, Row right)
    {
        this.rowType = rowType;
        this.left.hold(left);
        this.right.hold(right);
        this.nLeftFields = rowType.leftType().nFields();
        this.firstRightFieldOffset = nLeftFields - rowType.branchType().nFields();
        if (left != null && right != null) {
            // assert left.runId() == right.runId();
        }
    }

    // Object state

    private final ProductRowType rowType;
    private final ShareHolder<Row> left = new ShareHolder<Row>();
    private final ShareHolder<Row> right = new ShareHolder<Row>();
    private final int nLeftFields;
    private final int firstRightFieldOffset;

    // private methods

    private PValueSource nullPValue(int i) {
        PUnderlying underlying = rowType.typeInstanceAt(i).typeClass().underlyingType();
        return PValueSources.getNullSource(underlying);
    }
}
