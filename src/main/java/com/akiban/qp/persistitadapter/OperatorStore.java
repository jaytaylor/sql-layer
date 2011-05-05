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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.ConstantValueBindable;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.NoLimit;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.physicaloperator.UpdateLambda;
import com.akiban.qp.physicaloperator.Update_Default;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.DelegatingStore;
import com.akiban.server.store.PersistitStore;

import static com.akiban.qp.physicaloperator.API.emptyBindings;

public final class OperatorStore extends DelegatingStore<PersistitStore> {

    // DelegatingStore interface

    public OperatorStore() {
        super(new PersistitStore());
    }

    // OperatorStore interface

    public PersistitStore getPersistitStore() {
        return super.getDelegate();
    }

    // Store interface

    @Override
    public void updateRow(Session session, RowData oldRowData, RowData newRowData, ColumnSelector columnSelector)
            throws Exception
    {
        PersistitStore persistitStore = getPersistitStore();
        AkibanInformationSchema ais = persistitStore.getRowDefCache().ais();
        Schema schema = new Schema(ais);
        PersistitAdapter adapter = new PersistitAdapter(schema, persistitStore, session);

        PersistitGroupRow oldRow = PersistitGroupRow.newPersistitGroupRow(adapter, oldRowData);
        RowDef rowDef = persistitStore.getRowDefCache().rowDef(oldRowData.getRowDefId());
        UpdateLambda updateLambda = new InternalUpdateLambda(rowDef, newRowData, columnSelector);

        UserTable userTable = ais.getUserTable(oldRowData.getRowDefId());
        GroupTable groupTable = userTable.getGroup().getGroupTable();

        IndexBound bound = new IndexBound(userTable, oldRow, new ConstantColumnSelector(true));
        IndexKeyRange range = new IndexKeyRange(bound, true, bound, true);
        PhysicalOperator scanOp = API.groupScan_Default(groupTable, false, NoLimit.instance(), ConstantValueBindable.of(range));

        Update_Default updateOp = new Update_Default(scanOp, ConstantValueBindable.of(updateLambda));

        Cursor updateCursor = emptyBindings(adapter, updateOp);
        updateCursor.open();
        try {
            if (!updateCursor.next()) {
                throw new RuntimeException("no next!");
            }
            if (updateCursor.next()) {
                throw new RuntimeException("superfluous next: " + updateCursor.currentRow());
            }
        } finally {
            updateCursor.close();
        }
    }

    private static class InternalUpdateLambda implements UpdateLambda {
        private final RowData newRowData;
        private final ColumnSelector columnSelector;
        private final RowDef rowDef;

        private InternalUpdateLambda(RowDef rowDef, RowData newRowData, ColumnSelector columnSelector) {
            this.newRowData = newRowData;
            this.columnSelector = columnSelector;
            this.rowDef = rowDef;
        }

        @Override
        public boolean rowIsApplicable(Row row) {
            return (row instanceof PersistitGroupRow)
                    && ((PersistitGroupRow)row).rowDef().getRowDefId() == rowDef.getRowDefId();
        }

        @Override
        public Row applyUpdate(Row original) {
            OverlayingRow overlay = new OverlayingRow(original);
            for (int i=0; i < rowDef.getFieldCount(); ++i) {
                if (columnSelector.includesColumn(i)) {
                    overlay.overlay(i, newRowData.toObject(rowDef, i));
                }
            }
            return overlay;
        }
    }
}
