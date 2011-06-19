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
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.message.ErrorCode;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.ArrayBindings;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.CursorUpdateException;
import com.akiban.qp.physicaloperator.NoLimit;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.physicaloperator.UpdateFunction;
import com.akiban.qp.physicaloperator.Update_Default;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
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
import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.akiban.qp.physicaloperator.API.ancestorLookup_Default;
import static com.akiban.qp.physicaloperator.API.indexScan_Default;

public class OperatorStore extends DelegatingStore<PersistitStore> {

    // Store interface

    @Override
    public void updateRow(Session session, RowData oldRowData, RowData newRowData, ColumnSelector columnSelector)
            throws Exception
    {
        if (columnSelector != null) {
            throw new RuntimeException("group index maintence, and possibly other features, won't work with partial rows");
        }
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
        scanOp = ancestorLookup_Default(indexScan, groupTable, indexType, Collections.singletonList(tableType), false);

        // MVCC will render this useless, but for now, a limit of 1 ensures we won't see the row we just updated,
        // and therefore scan through two rows -- once to update old -> new, then to update new -> copy of new
        scanOp = com.akiban.qp.physicaloperator.API.limit_Default(scanOp, 1);

        Update_Default updateOp = new Update_Default(scanOp, updateFunction);

        Transaction transaction = ServiceManagerImpl.get().getTreeService().getTransaction(session);
        try {
            transaction.begin();
            maintainGroupIndexes(session, oldRowData, groupIndexDelete);
            runCursor(oldRowData, rowDef, updateOp, adapter);
            maintainGroupIndexes(session, newRowData, groupIndexInsert);
            transaction.commit();
        } finally {
            transaction.end();
        }
    }

    @Override
    public void writeRow(Session session, RowData rowData) throws Exception {
        Transaction transaction = ServiceManagerImpl.get().getTreeService().getTransaction(session);
        try {
            transaction.begin();
            super.writeRow(session, rowData);
            maintainGroupIndexes(session, rowData, groupIndexInsert);
            transaction.commit();
        } finally {
            transaction.end();
        }
    }

    @Override
    public void deleteRow(Session session, RowData rowData) throws Exception {
        Transaction transaction = ServiceManagerImpl.get().getTreeService().getTransaction(session);
        try {
            transaction.begin();
            maintainGroupIndexes(session, rowData, groupIndexDelete);
            super.deleteRow(session, rowData);
            transaction.commit();
        } finally {
            transaction.end();
        }
    }

    // OperatorStore interface

    public OperatorStore() {
        super(new PersistitStore(false));
    }

    public PersistitStore getPersistitStore() {
        return super.getDelegate();
    }

    // for use by subclasses

    protected final void maintainGroupIndexes(Session session, RowData rowData, GroupIndexHandler handler)
            throws PersistitException
    {
        AkibanInformationSchema ais = ServiceManagerImpl.get().getDXL().ddlFunctions().getAIS(session);
        PersistitAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), getPersistitStore(), session);

        UserTable userTable = ais.getUserTable(rowData.getRowDefId());

        Exchange hEx = adapter.takeExchange(userTable.getGroup().getGroupTable());
        try {
            getPersistitStore().constructHKey(session, hEx, (RowDef) userTable.rowDef(), rowData, true);
            PersistitHKey persistitHKey = new PersistitHKey(adapter, userTable.hKey());
            persistitHKey.copyFrom(hEx.getKey());

            ArrayBindings bindings = new ArrayBindings(1);
            bindings.set(HKEY_BINDING_POSITION, persistitHKey);

            for (GroupIndex groupIndex : optionallyOrderGroupIndexes(userTable.getGroupIndexes())) {
                PhysicalOperator plan = groupIndexCreationPlan(
                        adapter.schema(),
                        groupIndex,
                        adapter.schema().userTableRowType(userTable)
                );
                Cursor cursor = API.cursor(plan, adapter);
                cursor.open(bindings);
                try {
                    while (cursor.next()) {
                        Row row = cursor.currentRow();
                        handler.handleRow(groupIndexFields(groupIndex, row), java.util.Arrays.asList("TODO")); // TODO
                    }
                } finally {
                    cursor.close();
                }
            }
        } finally {
            adapter.returnExchange(hEx);
        }
    }

    protected Collection<GroupIndex> optionallyOrderGroupIndexes(Collection<GroupIndex> groupIndexes) {
        return groupIndexes;
    }

    // private methods

    private List<?> groupIndexFields(GroupIndex groupIndex, Row row) {
        return java.util.Arrays.asList("not yet working!", groupIndex.hashCode(), row.hashCode());
    }

    // private static methods

    static PhysicalOperator groupIndexCreationPlan(Schema schema, GroupIndex groupIndex, UserTableRowType rowType) {
        List<UserTableRowType> branchTables = branchTablesRootToLeaf(schema, groupIndex);
        if (branchTables.isEmpty()) {
            throw new RuntimeException("group index has empty branch: " + groupIndex);
        }

        boolean singleTableGI = branchTables.size() == 1;
        PhysicalOperator plan = API.groupScan_Default(
                groupIndex.getGroup().getGroupTable(),
                NoLimit.instance(),
                com.akiban.qp.expression.API.variable(HKEY_BINDING_POSITION),
                ! singleTableGI
        );
        if (singleTableGI) {
            return plan;
        }

        UserTable rowTypeTable = rowType.userTable();
        if (!branchTables.contains(rowType)) {
            throw new RuntimeException(rowType + " not in branch for " + groupIndex + ": " + branchTables);
        }

        if (!rowTypeTable.equals(branchTables.get(0).userTable())) {
            plan = API.ancestorLookup_Default(
                    plan,
                    groupIndex.getGroup().getGroupTable(),
                    rowType,
                    ancestors(rowType, branchTables),
                    true
            );
        }
        
        RowType planRowType = branchTables.get( branchTables.size() - 1 );
        for (RowType branchRowType : branchTables) {
            plan = API.flatten_HKeyOrdered(plan, planRowType, branchRowType, API.JoinType.INNER_JOIN);
            planRowType = plan.rowType();
        }
        return plan;
    }

    private static List<RowType> ancestors(RowType rowType, List<? extends RowType> branchTables) {
        List<RowType> ancestors = new ArrayList<RowType>();
        for(RowType ancestor : branchTables) {
            if (ancestor.equals(rowType)) {
                return ancestors;
            }
            ancestors.add(ancestor);
        }
        throw new RuntimeException(rowType + "not found in " + branchTables);
    }

    private static List<UserTableRowType> branchTablesRootToLeaf(Schema schema, GroupIndex groupIndex) {
        List<UserTableRowType> tables = new ArrayList<UserTableRowType>();
        UserTable rootmost = groupIndex.rootMostTable();
        for (UserTable table = groupIndex.leafMostTable(); !table.equals(rootmost); table = table.parentTable()) {
            tables.add(schema.userTableRowType(table));
        }
        tables.add(schema.userTableRowType(rootmost));
        Collections.reverse(tables);
        return tables;
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

    // object state
    
    private final GroupIndexHandler groupIndexInsert = new GroupIndexHandler() {
        @Override
        public void handleRow(List<?> fields, List<?> hKey) {
            // TODO
        }
    };
    
    private final GroupIndexHandler groupIndexDelete = new GroupIndexHandler() {
        @Override
        public void handleRow(List<?> fields, List<?> hKey) {
            // TODO
        }
    };

    // consts

    private static final int HKEY_BINDING_POSITION = 0;

    // nested classes

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
    
    protected interface GroupIndexHandler {
        void handleRow(List<?> fields, List<?> hKey);
    }
}
