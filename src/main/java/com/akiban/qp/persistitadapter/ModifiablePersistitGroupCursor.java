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
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.IndexDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.Store;
import com.persistit.exception.PersistitException;
import com.persistit.Key;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public final class ModifiablePersistitGroupCursor extends PersistitGroupCursor implements ModifiableCursor {

    public ModifiablePersistitGroupCursor(PersistitAdapter adapter, GroupTable groupTable) throws PersistitException {
        super(adapter, groupTable);
    }

    @Override
    public void updateCurrentRow(Row newRow) {
        RowHolder<PersistitGroupRow> currentRow = currentHeldRow();
        RowData currentRowData = currentRow.managedRow().rowData();
        RowDef rowDef = currentRow.managedRow().rowDef();
        RowData newRowData = adapter().rowData(rowDef, newRow);

        if (updateWouldChangeHKey(rowDef, currentRowData, newRowData)) {
            removeCurrentRow();
            addRow(newRow);
        } else {
            updateIndexes(newRow);
            updateGroupTable(newRow);
        }
    }

    @Override
    public void removeCurrentRow() {
        // for now, use PS
        RowHolder<PersistitGroupRow> currentRow = currentHeldRow();
        Store store = ServiceManagerImpl.get().getStore();
        try {
            store.deleteRow(session, currentRow.managedRow().rowData());
        } catch (Exception e) {
            throw new CursorUpdateException(e);
        }
    }

    private void updateGroupTable(Row newRow) {
        RowHolder<PersistitGroupRow> currentRow = currentHeldRow();
        RowDef rowDef = currentRow.managedRow().rowDef();
        RowData rowData = adapter().rowData(rowDef, newRow);
        try {
            PersistitStore.packRowData(exchange(), rowDef, rowData, ServiceManagerImpl.get().getTreeService());
            exchange().store();
        } catch (PersistitException e) {
            throw new CursorUpdateException(e);
        }
        currentRow.set(PersistitGroupRow.newPersistitGroupRow(adapter(), rowData));
    }

    private void updateIndexes(Row newRow) {
        Row current = currentRow();
        final UserTableRowType rowType = userTableRowType(current);

        RowDef rowDef = (RowDef) rowType.userTable().rowDef();
        exchange().clear();

        try {
            adapter().constructHKey(exchange(), rowDef, adapter().rowData(rowDef, current));
        } catch (Exception e) {
            throw new CursorUpdateException(e);
        }
        Key hKey = exchange().getKey();

        try {
            for(IndexUpdater indexUpdater : indexCursors(rowType)) {
                indexUpdater.update(current, hKey, newRow);
            }
        } catch (PersistitException e) {
            throw new CursorUpdateException(e);
        }
    }

    private UserTableRowType userTableRowType(Row row) {
        RowType rowType = row.rowType();
        try {
            return (UserTableRowType) rowType;
        } catch (ClassCastException e) {
            Object type = rowType == null ? "null" : rowType.getClass();
            throw new UnsupportedOperationException("require UserTableRowType; found " + type);
        }
    }

    private Collection<IndexUpdater> indexCursors(UserTableRowType utRowType) throws PersistitException {
        Collection<IndexUpdater> ret = indexCursorsMap.get(utRowType);
        if (ret != null) {
            return ret;
        }
        ret = new ArrayList<IndexUpdater>();
        for (IndexRowType indexRowType : utRowType.indexRowTypes()) {
            if (indexRowType == null) {
                continue;
            }
            if (((IndexDef)indexRowType.index().indexDef()).isHKeyEquivalent()) {
                continue;
            }
            ret.add(new IndexUpdater(adapter(), indexRowType));
        }
        indexCursorsMap.put(utRowType, ret);
        return ret;
    }

    @Override
    public void addRow(Row newRow) {
        // for now, use PS
        RowHolder<PersistitGroupRow> currentRow = currentHeldRow();
        Store store = ServiceManagerImpl.get().getStore();
        try {
            store.writeRow(session, currentRow.managedRow().rowData());
        } catch (Exception e) {
            throw new CursorUpdateException(e);
        }
    }

    private boolean updateWouldChangeHKey(RowDef rowDef, RowData oldRow, RowData newRow) {
        int[] affectedFields = getHKeyFields(rowDef);
        return ! PersistitStore.fieldsEqual(rowDef, oldRow, newRow, affectedFields);
    }

    private int[] getHKeyFields(RowDef rowDef) {
        int[] pkFields = rowDef.getPKIndexDef().getFields();
        int[] fkFields = rowDef.getParentJoinFields();
        int[] all = new int[pkFields.length + fkFields.length];
        System.arraycopy(pkFields, 0, all, 0, pkFields.length);
        System.arraycopy(fkFields, 0, all, pkFields.length, fkFields.length);
        return all;
    }

    private final Session session = new SessionImpl();
    private final Map<UserTableRowType,Collection<IndexUpdater>> indexCursorsMap = new HashMap<UserTableRowType, Collection<IndexUpdater>>();

    private static class IndexUpdater {
        private final PersistitAdapter adapter;
        private final IndexDef indexDef;

        public IndexUpdater(PersistitAdapter adapter, IndexRowType indexRowType) {
            this.adapter = adapter;
            this.indexDef = (IndexDef) indexRowType.index().indexDef();
        }

        public void update(Row oldRow, Key hKey, Row newRow) {
            try {
                adapter.updateIndex(indexDef, oldRow, newRow, hKey);
            } catch (PersistitException e) {
                throw new CursorUpdateException(e);
            }
        }
    }
}
