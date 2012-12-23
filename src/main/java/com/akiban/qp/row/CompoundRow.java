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

import com.akiban.qp.rowtype.CompoundRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.ShareHolder;

public class CompoundRow extends AbstractRow {

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        return null;
    }

    @Override
    public ValueSource eval(int i) {
        ValueSource source;
        if (i < firstRowFields) {
            source = firstRowh.isEmpty() ? NullValueSource.only() : firstRowh.get().eval(i);
        } else {
            source = secondRowh.isEmpty() ? NullValueSource.only() : secondRowh.get().eval(i - rowOffset);
        }
        return source;
    }

    @Override
    public PValueSource pvalue(int i) {
        PValueSource source;
        if (i < firstRowFields) {
            source = firstRowh.isEmpty() ? nullPValue(i) : firstRowh.get().pvalue(i);
        } else {
            source = secondRowh.isEmpty() ? nullPValue(i) : secondRowh.get().pvalue(i - rowOffset);
        }
        return source;
    }
    
    @Override
    public Row subRow(RowType subRowType)
    {
        Row subRow;
        if (subRowType == rowType.first()) {
            subRow = firstRowh.get();
        } else if (subRowType == rowType.second()) {
            subRow = secondRowh.get();
        } else {
            // If the subRowType doesn't match leftType or rightType, then it might be buried deeper.
            subRow = firstRowh.get().subRow(subRowType);
            if (subRow == null) {
                subRow = secondRowh.get().subRow(subRowType);
            }
        }
        return subRow;
    }

    protected ShareHolder<Row> first() {
        return firstRowh;
    }
    
    protected ShareHolder<Row> second() {
        return secondRowh;
    }
    
    protected int firstRowFields() {
        return firstRowFields;
    }
    
    public CompoundRow (CompoundRowType type, Row firstRow, Row secondRow)
    {
        this.rowType = type;
        this.firstRowh.hold(firstRow);
        this.secondRowh.hold(secondRow);
        this.firstRowFields = type.first().nFields();
        this.rowOffset = type.first().nFields();
    }

    private PValueSource nullPValue(int i) {
        return PValueSources.getNullSource(rowType.typeInstanceAt(i));
    }

    // Object state

    private final CompoundRowType rowType;
    private final ShareHolder<Row> firstRowh = new ShareHolder<Row>();
    private final ShareHolder<Row> secondRowh = new ShareHolder<Row>();
    private final int firstRowFields;
    protected int rowOffset; 


}
