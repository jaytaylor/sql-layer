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
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.message.ErrorCode;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.physicaloperator.CursorUpdateException;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.physicaloperator.UpdateFunction;
import com.akiban.qp.physicaloperator.Update_Default;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.api.dml.DuplicateKeyException;
import com.akiban.server.api.dml.NoSuchRowException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.DelegatingStore;
import com.akiban.server.store.PersistitStore;
import com.persistit.Transaction;

import static com.akiban.qp.physicaloperator.API.branchLookup_Default;
import static com.akiban.qp.physicaloperator.API.indexScan_Default;

public final class OperatorStore extends DelegatingStore<PersistitStore> {

    // Store interface

    @Override
    public void updateRow(Session session, RowData oldRowData, RowData newRowData, ColumnSelector columnSelector)
            throws Exception
    {
        PersistitStore persistitStore = getPersistitStore();
        AkibanInformationSchema ais = persistitStore.getRowDefCache().ais();
        PersistitAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), persistitStore, session);
        Schema schema = adapter.schema();

        PersistitGroupRow oldRow = PersistitGroupRow.newPersistitGroupRow(adapter, oldRowData);
        RowDef rowDef = persistitStore.getRowDefCache().rowDef(oldRowData.getRowDefId());
        UpdateFunction updateFunction = new InternalUpdateFunction(adapter, rowDef, newRowData, columnSelector);

        UserTable userTable = ais.getUserTable(oldRowData.getRowDefId());
        GroupTable groupTable = userTable.getGroup().getGroupTable();
        IndexBound bound = new IndexBound(userTable, oldRow, ConstantColumnSelector.ALL_ON);
        IndexKeyRange range = new IndexKeyRange(bound, true, bound, true);

        PhysicalOperator scanOp;
        Index index = userTable.getPrimaryKeyIncludingInternal().getIndex();
        assert index != null : userTable;
        UserTableRowType tableType = schema.userTableRowType(userTable);
        IndexRowType indexType = tableType.indexRowType(index);
        PhysicalOperator indexScan = indexScan_Default(indexType, false, range);
        scanOp = branchLookup_Default(indexScan, groupTable, indexType, tableType, false);

        // MVCC will render this useless, but for now, a limit of 1 ensures we won't see the row we just updated,
        // and therefore scan through two rows -- once to update old -> new, then to update new -> copy of new
        scanOp = com.akiban.qp.physicaloperator.API.limit_Default(scanOp, 1);

        Update_Default updateOp = new Update_Default(scanOp, updateFunction);

        Transaction transaction = ServiceManagerImpl.get().getTreeService().getTransaction(session);
        try {
            transaction.begin();
            runCursor(oldRowData, rowDef, updateOp, adapter);
            transaction.commit();
        } finally {
            transaction.end();
        }
    }

    private static void runCursor(RowData oldRowData, RowDef rowDef, UpdatePlannable plannable, PersistitAdapter adapter)
            throws DuplicateKeyException, NoSuchRowException
    {
        final UpdateResult result;
        try {
            result = plannable.run(UndefBindings.only(), adapter);
        } catch (CursorUpdateException e) {
            Throwable cause = e.getCause();
            if ( (cause instanceof InvalidOperationException)
                    && ErrorCode.DUPLICATE_KEY.equals(((InvalidOperationException) cause).getCode()))
            {
                throw new DuplicateKeyException((InvalidOperationException)cause);
            }
            throw e;
        }

        if (result.rowsModified() == 0 || result.rowsTouched() == 0) {
            throw new NoSuchRowException(describeRow(oldRowData, rowDef));
        }
        else if(result.rowsModified() != 1 || result.rowsTouched() != 1) {
            throw new RuntimeException(String.format(
                    "%s: %d touched, %d modified",
                    describeRow(oldRowData, rowDef),
                    result.rowsTouched(),
                    result.rowsModified()
            ));
        }
    }

    private static String describeRow(RowData oldRowData, RowDef rowDef) {
        String rowDescription;
        try {
            rowDescription = oldRowData.toString(rowDef);
        } catch (Exception e) {
            rowDescription = "error in generating RowData.toString";
        }
        return rowDescription;
    }

    // OperatorStore interface

    public OperatorStore() {
        super(new PersistitStore());
    }

    public PersistitStore getPersistitStore() {
        return super.getDelegate();
    }

    // inner classes

    private static class InternalUpdateFunction implements UpdateFunction {
        private final PersistitAdapter adapter;
        private final RowData newRowData;
        private final ColumnSelector columnSelector;
        private final RowDef rowDef;

        private InternalUpdateFunction(PersistitAdapter adapter, RowDef rowDef, RowData newRowData, ColumnSelector columnSelector) {
            this.newRowData = newRowData;
            this.columnSelector = columnSelector;
            this.rowDef = rowDef;
            this.adapter = adapter;
        }

        @Override
        public boolean rowIsSelected(Row row) {
            return row.rowType().typeId() == rowDef.getRowDefId();
        }

        @Override
        public Row evaluate(Row original, Bindings bindings) {
            // TODO
            // ideally we'd like to use an OverlayingRow, but ModifiablePersistitGroupCursor requires
            // a PersistitGroupRow if an hkey changes
//            OverlayingRow overlay = new OverlayingRow(original);
//            for (int i=0; i < rowDef.getFieldCount(); ++i) {
//                if (columnSelector == null || columnSelector.includesColumn(i)) {
//                    overlay.overlay(i, newRowData.toObject(rowDef, i));
//                }
//            }
//            return overlay;
            // null selector means all cols, so we can skip the merging and just return the new row data
            if (columnSelector == null) {
                return PersistitGroupRow.newPersistitGroupRow(adapter, newRowData);
            }
            // Note: some encodings are untested except as necessary for mtr
            NewRow newRow = new NiceRow(rowDef.getRowDefId());
            for (int i=0; i < original.rowType().nFields(); ++i) {
                if (columnSelector.includesColumn(i)) {
                    newRow.put(i, newRowData.toObject(rowDef, i));
                }
                else {
                    newRow.put(i, original.field(i, bindings));
                }
            }
            return PersistitGroupRow.newPersistitGroupRow(adapter, newRow.toRowData());
        }
    }
}
