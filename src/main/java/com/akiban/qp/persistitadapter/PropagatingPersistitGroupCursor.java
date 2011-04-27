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
import com.akiban.ais.model.Index;
import com.akiban.qp.expression.API;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.CursorUpdateException;
import com.akiban.qp.physicaloperator.ModifiableCursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.IndexKeyType;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public final class PropagatingPersistitGroupCursor extends PersistitGroupCursor implements ModifiableCursor {

    public PropagatingPersistitGroupCursor(PersistitAdapter adapter, GroupTable groupTable) throws PersistitException {
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
        Schema schema = current.rowType().schema();

        try {
            for(NonPropagatingPersistitIndexCursor indexCursor : indexCursors(current.rowType())) {
                Index index = indexCursor.indexRowType().index();
                if (((IndexDef)index.indexDef()).isHKeyEquivalent()) {
                    continue;
                }
                IndexBound singleRowBound = singleRowBound(schema, index, current);
                IndexKeyRange range = new IndexKeyRange(singleRowBound, true, singleRowBound, true);
                indexCursor.bind(range);
                indexCursor.open();
                boolean indexNext = indexCursor.next();
                assert indexNext;
                indexCursor.updateCurrentRow(newRow);
                assert ! indexCursor.next();
                indexCursor.close();
            }
        } catch (PersistitException e) {
            throw new CursorUpdateException(e);
        }
    }

    private IndexBound singleRowBound(Schema schema, Index index, Row groupRow)
    {
        return API.indexBound(new IndexKeyType(schema, index), groupRow);
//        IndexRowType indexRowType = schema.indexRowType(index);
//        List<Object> indexFields = new ArrayList<Object>();
//        for (IndexColumn indexColumn : index.getColumns()) {
//            int groupRowField = indexColumn.getColumn().getPosition();
//            Object field = groupRow.field(groupRowField);
//            indexFields.add(field);
//        }
//        Row indexRow = new AdHocRow(indexRowType, indexFields);
//        return new IndexBound(new IndexKeyType(schema, index), indexRow);
    }

    private Collection<NonPropagatingPersistitIndexCursor> indexCursors(RowType rowType) throws PersistitException {
        Collection<NonPropagatingPersistitIndexCursor> ret = indexCursorsMap.get(rowType);
        if (ret != null) {
            return ret;
        }

        final UserTableRowType utRowType;
        try {
            utRowType = (UserTableRowType) rowType;
        } catch (ClassCastException e) {
            Object type = rowType == null ? "null" : rowType.getClass();
            throw new UnsupportedOperationException("require UserTableRowType; found " + type);
        }
        ret = new ArrayList<NonPropagatingPersistitIndexCursor>();
        for (IndexRowType indexRowType : utRowType.indexRowTypes()) {
            if (indexRowType == null) {
                continue;
            }
            ret.add(new NonPropagatingPersistitIndexCursor(adapter(), indexRowType));
        }
        indexCursorsMap.put(rowType, ret);
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
    private final Map<RowType,Collection<NonPropagatingPersistitIndexCursor>> indexCursorsMap = new HashMap<RowType, Collection<NonPropagatingPersistitIndexCursor>>();
}
