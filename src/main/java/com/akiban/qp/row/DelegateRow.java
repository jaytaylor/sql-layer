
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
