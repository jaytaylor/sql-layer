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
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;

public class DelegateRow implements Row {
    private final Row delegate;

    public DelegateRow(Row delegate) {
        this.delegate = delegate;
    }

    public Row getDelegate() {
        return delegate;
    }

    //
    // Row
    //

    @Override
    public RowType rowType() {
        return delegate.rowType();
    }

    @Override
    public HKey hKey() {
        return delegate.hKey();
    }

    @Override
    public HKey ancestorHKey(UserTable table) {
        return delegate.ancestorHKey(table);
    }

    @Override
    public boolean ancestorOf(RowBase that) {
        return delegate.ancestorOf(that);
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable) {
        return delegate.containsRealRowOf(userTable);
    }

    @Override
    public Row subRow(RowType subRowType) {
        return delegate.subRow(subRowType);
    }

    @Override
    public int compareTo(RowBase row, int leftStartIndex, int rightStartIndex, int fieldCount) {
        return delegate.compareTo(row, leftStartIndex, rightStartIndex, fieldCount);
    }

    @Override
    public PValueSource pvalue(int index) {
        return delegate.pvalue(index);
    }

    @Override
    public ValueSource eval(int index) {
        return delegate.eval(index);
    }

    @Override
    public void acquire() {
        delegate.acquire();
    }

    @Override
    public boolean isShared() {
        return delegate.isShared();
    }

    @Override
    public void release() {
        delegate.release();
    }
}
