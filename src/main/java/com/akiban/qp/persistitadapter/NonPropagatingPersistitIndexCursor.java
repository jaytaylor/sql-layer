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

import com.akiban.qp.physicaloperator.CursorUpdateException;
import com.akiban.qp.physicaloperator.ModifiableCursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.IndexDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.Store;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

final class NonPropagatingPersistitIndexCursor extends PersistitIndexCursor implements ModifiableCursor {

    NonPropagatingPersistitIndexCursor(PersistitAdapter adapter, IndexRowType indexRowType) throws PersistitException {
        super(adapter, indexRowType);

        Store store = ServiceManagerImpl.get().getStore();
        try {
            persistitStore = (PersistitStore) store;
        } catch (ClassCastException e) {
            throw new RuntimeException(
                    "ServiceManagerImpl's store must be PersistitStore; is "
                            + (store == null ? "null" : store.getClass())
            );
        }
        session = new SessionImpl();
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
        // TODO need some way of validating that the newRow is of the same type as the index's main group type
        
        RowDef rowDef = (RowDef) indexRowType().index().getTable().rowDef();
        RowData rowData = adapter().rowData(rowDef, newRow);
        IndexDef indexDef = (IndexDef) indexRowType().index().indexDef();
        final Key iKey;
        try {
            persistitStore.constructHKey(session(), exchange(), rowDef, rowData, false);
            Key hKey = new Key(exchange().getKey());
            iKey = exchange().getKey().clear();
            PersistitStore.constructIndexKey(iKey, rowData, indexDef, hKey);
        } catch (Exception e) {
            throw new CursorUpdateException(e);
        }
        // TODO uniqueness check needs to be merged in from trunk
    }

    private Session session() {
        return session;
    }

    private final PersistitStore persistitStore;
    private final Session session;
}
