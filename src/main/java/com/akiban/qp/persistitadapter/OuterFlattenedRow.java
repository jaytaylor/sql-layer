/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;

final class OuterFlattenedRow extends AbstractRow {

    @Override
    public RowType rowType() {
        throw new UnsupportedOperationException();  // TODO
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable) {
        return userTable.getDepth() <= maxTableDepth && delegate.containsRealRowOf(userTable);
    }

    @Override
    public ValueSource eval(int index) {
        return (index <= maxEvalIndex)
                ? delegate.eval(index)
                : NullValueSource.only();
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

    private OuterFlattenedRow(UserTable maxTable, int lastNonNullField, Row delegate) {
        this.maxTableDepth = maxTable.getDepth();
        this.maxEvalIndex = lastNonNullField;
        this.delegate = delegate;
    }

    private final int maxTableDepth;
    private final int maxEvalIndex;
    private Row delegate;

    public static class Factory {
        public Factory(UserTable maxTable, int maxEvalIndex) {
            this.maxTable = maxTable;
            this.maxEvalIndex = maxEvalIndex;
        }

        public Row createFrom(Row row) {
            return new OuterFlattenedRow(maxTable, maxEvalIndex, row);
        }

        private final UserTable maxTable;
        private final int maxEvalIndex;
    }
}
