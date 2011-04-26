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

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.physicaloperator.CursorUpdateException;
import com.akiban.qp.physicaloperator.ModifiableCursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.persistit.exception.PersistitException;

final class NonPropogatingPersistitIndexCursor extends PersistitIndexCursor implements ModifiableCursor {

    NonPropogatingPersistitIndexCursor(PersistitAdapter adapter, IndexRowType indexRowType) throws PersistitException {
        super(adapter, indexRowType);
    }

    @Override
    public void removeCurrentRow() {
        try {
            exchange().remove();
        } catch (PersistitException e) {
            throw new CursorUpdateException(e);
        }
    }

    @Override
    public void updateCurrentRow(Row newRow) {
        removeCurrentRow();
        addRow(newRow);
    }

    @Override
    public void addRow(Row newRow) {
        throw new UnsupportedOperationException(); // TODO
    }
}
