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

package com.akiban.server.test.it.qp;

import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.RowValuesHolder;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;

public class TestRow extends AbstractRow
{
    // RowBase interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        return valuesHolder.valueSourceAt(i);
    }

    @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // TestRow interface

    public TestRow(RowType rowType, Object[] fields, String hKeyString)
    {
        this(rowType, new RowValuesHolder(fields), hKeyString);
    }

    public TestRow(RowType rowType, Object[] fields) {
        this(rowType, fields, null);
    }

    public TestRow(RowType rowType, RowValuesHolder valuesHolder, String hKeyString) {
        this.rowType = rowType;
        this.valuesHolder = valuesHolder;
        this.hKeyString = hKeyString;
    }

    public String persistityString() {
        return hKeyString;
    }

    // Object state

    private final RowType rowType;
    private final RowValuesHolder valuesHolder;
    private final String hKeyString;
}
