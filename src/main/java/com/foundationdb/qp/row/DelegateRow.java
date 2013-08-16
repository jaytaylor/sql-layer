/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.row;

import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types3.pvalue.PValueSource;

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
