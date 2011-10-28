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
package com.akiban.server.test.it.qp;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;

public final class NullsRow extends AbstractRow {
    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueSource eval(int index) {
        return NullValueSource.only();
    }

    public NullsRow(RowType rowType) {
        this.rowType = rowType;
    }

    private final RowType rowType;
}
