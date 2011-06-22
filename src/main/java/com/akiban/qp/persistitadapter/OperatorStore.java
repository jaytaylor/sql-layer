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
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexRowComposition;
import com.akiban.ais.model.TableIndex;
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
import com.akiban.server.FieldDef;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.api.dml.DuplicateKeyException;
import com.akiban.server.api.dml.NoSuchRowException;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.DelegatingStore;
import com.akiban.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.sun.corba.se.spi.ior.IdentifiableFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.akiban.qp.physicaloperator.API.ancestorLookup_Default;
import static com.akiban.qp.physicaloperator.API.indexScan_Default;

public class OperatorStore extends DelegatingStore<PersistitStore> {

    // Store interface

    @Override
    public void updateRow(Session session, RowData oldRowData, RowData newRowData, ColumnSelector columnSelector)
            throws Exception
    {
        PersistitStore persistitStore = getPersistitStore();
        AkibanInformationSchema ais = persistitStore.getRowDefCache().ais();

        RowDef rowDef = persistitStore.getRowDefCache().rowDef(oldRowData.getRowDefId());
        if ((columnSelector != null) && !rowDef.table().getGroupIndexes().isEmpty()) {
            throw new RuntimeException("group index maintence won't work with partial rows");
        }

        PersistitAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), persistitStore, session);
        Schema schema = adapter.schema();

        UpdateFunction updateFunction = new InternalUpdateFunction(adapter, rowDef, newRowData, columnSelector);

        UserTable userTable = ais.getUserTable(oldRowData.getRowDefId());
        GroupTable groupTable = userTable.getGroup().getGroupTable();

        TableIndex index = userTable.getPrimaryKeyIncludingInternal().getIndex();
        assert index != null : userTable;
        UserTableRowType tableType = schema.userTableRowType(userTable);
        IndexRowType indexType = tableType.indexRowType(index);
        IndexBound bound = new IndexBound(new NewRowBackedIndexRow(tableType, new LegacyRowWrapper(oldRowData), index),
                                          ConstantColumnSelector.ALL_ON);
        IndexKeyRange range = new IndexKeyRange(bound, true, bound, true);

        PhysicalOperator indexScan = indexScan_Default(indexType, false, range);
        PhysicalOperator scanOp;
        scanOp = ancestorLookup_Default(indexScan, groupTable, indexType, Collections.singletonList(tableType), false);

        // MVCC will render this useless, but for now, a limit of 1 ensures we won't see the row we just updated,
        // and therefore scan through two rows -- once to update old -> new, then to update new -> copy of new
        scanOp = com.akiban.qp.physicaloperator.API.limit_Default(scanOp, 1);

        Update_Default updateOp = new Update_Default(scanOp, updateFunction);

        Transaction transaction = ServiceManagerImpl.get().getTreeService().getTransaction(session);
        for(int retryCount=0; ; ++retryCount) {
            try {
                transaction.begin();

                maintainGroupIndexes(
                        session, ais, adapter,
                        oldRowData, new PersistitKeyHandler(adapter), new RowAction(userTable, Action.DELETE)
                );

                runCursor(oldRowData, rowDef, updateOp, adapter);

                maintainGroupIndexes(
                        session, ais, adapter,
                        newRowData, new PersistitKeyHandler(adapter), new RowAction(userTable, Action.STORE)
                );

                transaction.commit();
                break;
            } catch (RollbackException e) {
                if (retryCount >= MAX_RETRIES) {
                    throw e;
                }
            } finally {
                transaction.end();
            }
        }
    }

    @Override
    public void writeRow(Session session, RowData rowData) throws Exception {
        Transaction transaction = ServiceManagerImpl.get().getTreeService().getTransaction(session);
        for(int retryCount=0; ; ++retryCount) {
            try {
                transaction.begin();
                super.writeRow(session, rowData);

                AkibanInformationSchema ais = ServiceManagerImpl.get().getDXL().ddlFunctions().getAIS(session);
                PersistitAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), getPersistitStore(), session);
                UserTable uTable = ais.getUserTable(rowData.getRowDefId());
                maintainGroupIndexes(
                        session, ais, adapter,
                        rowData, new PersistitKeyHandler(adapter), new RowAction(uTable, Action.STORE)
                );

                transaction.commit();
                break;
            } catch (RollbackException e) {
                if (retryCount >= MAX_RETRIES) {
                    throw e;
                }
            } finally {
                transaction.end();
            }
        }
    }

    @Override
    public void deleteRow(Session session, RowData rowData) throws Exception {
        Transaction transaction = ServiceManagerImpl.get().getTreeService().getTransaction(session);
        for(int retryCount=0; ; ++retryCount) {
            try {
                transaction.begin();
                AkibanInformationSchema ais = ServiceManagerImpl.get().getDXL().ddlFunctions().getAIS(session);
                PersistitAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), getPersistitStore(), session);
                UserTable uTable = ais.getUserTable(rowData.getRowDefId());
                maintainGroupIndexes(
                        session, ais, adapter,
                        rowData, new PersistitKeyHandler(adapter), new RowAction(uTable, Action.DELETE)
                );
                super.deleteRow(session, rowData);
                transaction.commit();
                break;
            } catch (RollbackException e) {
                if (retryCount >= MAX_RETRIES) {
                    throw e;
                }
            } finally {
                transaction.end();
            }
        }
    }

    @Override
    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean defer) throws Exception {
        List<TableIndex> tableIndexes = new ArrayList<TableIndex>();
        List<GroupIndex> groupIndexes = new ArrayList<GroupIndex>();
        for(Index index : indexes) {
            if(index.isTableIndex()) {
                tableIndexes.add((TableIndex)index);
            }
            else if(index.isGroupIndex()) {
                groupIndexes.add((GroupIndex)index);
            }
            else {
                throw new IllegalArgumentException("Unknown index type: " + index);
            }
        }

        if(!tableIndexes.isEmpty()) {
            super.buildIndexes(session, tableIndexes, defer);
        }

        AkibanInformationSchema ais = ServiceManagerImpl.get().getDXL().ddlFunctions().getAIS(session);
        PersistitAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), getPersistitStore(), session);
        for(GroupIndex groupIndex : groupIndexes) {
            PhysicalOperator plan = groupIndexCreationPlan(adapter.schema(), groupIndex);
            runMaintenancePlan(adapter, groupIndex, plan, UndefBindings.only(),
                    new PersistitKeyHandler(adapter), new RowAction(null, Action.STORE)); // TODO
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

    protected final <A,T extends Throwable> void maintainGroupIndexes(
            Session session,
            RowData rowData,
            GroupIndexHandler<A,T> handler, A action
    )
    throws PersistitException, T
    {
        AkibanInformationSchema ais = ServiceManagerImpl.get().getDXL().ddlFunctions().getAIS(session);
        PersistitAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), getPersistitStore(), session);
        maintainGroupIndexes(session, ais, adapter, rowData, handler, action);
    }

    protected Collection<GroupIndex> optionallyOrderGroupIndexes(Collection<GroupIndex> groupIndexes) {
        return groupIndexes;
    }

    // private methods

    private <A,T extends Throwable> void maintainGroupIndexes(
            Session session,
            AkibanInformationSchema ais, PersistitAdapter adapter,
            RowData rowData,
            GroupIndexHandler<A,T> handler, A action
    )
    throws PersistitException, T
    {
        UserTable userTable = ais.getUserTable(rowData.getRowDefId());

        Exchange hEx = adapter.takeExchange(userTable.getGroup().getGroupTable());
        try {
            // the "false" at the end of constructHKey toggles whether the RowData should be modified to increment
            // the hidden PK field, if there is one. For PK-less rows, this field have already been incremented by now,
            // so we don't want to increment it again
            getPersistitStore().constructHKey(session, hEx, (RowDef) userTable.rowDef(), rowData, false);
            PersistitHKey persistitHKey = new PersistitHKey(adapter, userTable.hKey());
            persistitHKey.copyFrom(hEx.getKey());

            ArrayBindings bindings = new ArrayBindings(1);
            bindings.set(HKEY_BINDING_POSITION, persistitHKey);

            Collection<GroupIndex> branchIndexes = new ArrayList<GroupIndex>();
            for (GroupIndex groupIndex : userTable.getGroup().getIndexes()) {
                if (groupIndex.leafMostTable().isDescendantOf(userTable)) {
                    branchIndexes.add(groupIndex);
                }
            }

            for (GroupIndex groupIndex : optionallyOrderGroupIndexes(branchIndexes)) {
                if (groupIndex.isUnique()) {
                    throw new UnsupportedOperationException("unique indexes not supported");
                }
                PhysicalOperator plan = groupIndexCreationPlan(
                        adapter.schema(),
                        groupIndex,
                        adapter.schema().userTableRowType(userTable)
                );
                runMaintenancePlan(adapter, groupIndex, plan, bindings, handler, action);
            }
        } finally {
            adapter.returnExchange(hEx);
        }
    }

    private <A,T extends Throwable> void runMaintenancePlan(
            PersistitAdapter adapter,
            GroupIndex groupIndex,
            PhysicalOperator plan,
            Bindings bindings,
            GroupIndexHandler<A, T> handler, A action
    )
    throws T
    {
        Cursor cursor = API.cursor(plan, adapter);
        cursor.open(bindings);
        try {
            while (cursor.next()) {
                Row row = cursor.currentRow();
                if (row.rowType().equals(plan.rowType())) {
                    handler.handleRow(action, groupIndex, row);
                }
            }
        } finally {
            cursor.close();
        }
    }


    // private static methods

    static PhysicalOperator groupIndexCreationPlan(Schema schema, GroupIndex groupIndex, UserTableRowType rowType) {
        BranchTables branchTables = branchTablesRootToLeaf(schema, groupIndex);
        if (branchTables.isEmpty()) {
            throw new RuntimeException("group index has empty branch: " + groupIndex);
        }
        if (!branchTables.fromRoot().contains(rowType)) {
            throw new RuntimeException(rowType + " not in branch for " + groupIndex + ": " + branchTables);
        }

        boolean deep = !branchTables.leafMost().equals(rowType);
        PhysicalOperator plan = API.groupScan_Default(
                groupIndex.getGroup().getGroupTable(),
                NoLimit.instance(),
                com.akiban.qp.expression.API.variable(HKEY_BINDING_POSITION),
                deep
        );
        if (branchTables.fromRoot().size() == 1) {
            return plan;
        }
        if (!branchTables.fromRoot().get(0).equals(rowType)) {
            plan = API.ancestorLookup_Default(
                    plan,
                    groupIndex.getGroup().getGroupTable(),
                    rowType,
                    ancestors(rowType, branchTables.fromRoot()),
                    true
            );
        }
        
        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        for (RowType branchRowType : branchTables.fromRoot()) {
            if (parentRowType == null) {
                parentRowType = branchRowType;
            }
            else {
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType);
                parentRowType = plan.rowType();
            }
            if (branchRowType.equals(branchTables.rootMost())) {
                joinType = API.JoinType.INNER_JOIN;
            }
        }
        return plan;
    }

    /**
     * Create plan for the complete selection of all rows of the given GroupIndex (e.g. creating an
     * index on existing date).
     * @param schema Schema
     * @param groupIndex GroupIndex
     * @return PhysicalOperator
     */
    static PhysicalOperator groupIndexCreationPlan(Schema schema, GroupIndex groupIndex) {
        BranchTables branchTables = branchTablesRootToLeaf(schema, groupIndex);

        PhysicalOperator plan = API.groupScan_Default(groupIndex.getGroup().getGroupTable(), NoLimit.instance());

        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        for (RowType branchRowType : branchTables.fromRoot()) {
            if (parentRowType == null) {
                parentRowType = branchRowType;
            }
            else {
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType);
                parentRowType = plan.rowType();
            }
            if (branchRowType.equals(branchTables.rootMost())) {
                joinType = API.JoinType.INNER_JOIN;
            }
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

    private static BranchTables branchTablesRootToLeaf(Schema schema, GroupIndex groupIndex) {
        return new BranchTables(schema, groupIndex);
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

    // consts

    private static final int HKEY_BINDING_POSITION = 0;
    private static final int MAX_RETRIES = 10;

    public static AtomicBoolean DEBUG_POINT = new AtomicBoolean(false); // TODO remove this! If you see this in a merge proposal, tell Yuval to remove it!

    // nested classes

    private static class PersistitKeyHandler implements GroupIndexHandler<RowAction,PersistitException> {

        // GroupIndexHandler interface

        @Override
        public void handleRow(RowAction action, GroupIndex groupIndex, Row row)
        throws PersistitException
        {
            Exchange exchange = adapter.takeExchange(groupIndex);
            Key key = exchange.getKey();
            key.clear();
            IndexRowComposition irc = groupIndex.indexRowComposition();

            UserTable sourceTable = action.sourceTable();
            final boolean sourceRowAboveIndex;
            final UserTable leafmost = groupIndex.leafMostTable();
            if (sourceTable == null) {
                sourceRowAboveIndex = true;
            }
            else if (sourceTable.equals(leafmost)) {
                sourceRowAboveIndex = false;
            }
            else if (sourceTable.isDescendantOf(leafmost)) {
                return; // nothing to do
            }
            else if (groupIndex.rootMostTable().equals(sourceTable)) {
                sourceRowAboveIndex = false;
            }
            else if (groupIndex.rootMostTable().isDescendantOf(sourceTable)) {
                sourceRowAboveIndex = true;
            }
            else {
                sourceRowAboveIndex = false; // source table is within this branch
            }


            // nullPoint is the point at which we should stop nulling hkey values; needs a better name.
            // This is the last index of the hkey component that should be nulled.
            int nullPoint = -1;
            for(int i=0, LEN = irc.getLength(); i < LEN; ++i) {
                assert irc.isInRowData(i);
                assert ! irc.isInHKey(i);

                final int flattenedIndex = irc.getFieldPosition(i);
                Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);
                Object value = row.field(flattenedIndex, UndefBindings.only());
                RowDef rowDef = (RowDef) column.getTable().rowDef();
                FieldDef fieldDef = rowDef.getFieldDef(column.getPosition());
                fieldDef.getEncoding().toKey(fieldDef, value, key);
                boolean isHKeyComponent = i+1 > groupIndex.getColumns().size();
                if (sourceRowAboveIndex && isHKeyComponent && column.getTable().equals(sourceTable)) {
                    nullPoint = i;
                }
            }

            switch (action.action()) {
            case STORE:
                exchange.store();
                if (nullOutHKey(nullPoint, groupIndex, row, key)) {
                    exchange.remove();
                }
                break;
            case DELETE:
                exchange.remove();
                if (nullOutHKey(nullPoint, groupIndex, row, key)) {
                    exchange.store();
                }
                break;
            default:
                throw new UnsupportedOperationException(action.action().name());
            }
        }

        private boolean nullOutHKey(int nullPoint, GroupIndex groupIndex, Row row, Key key) {
            if (nullPoint < 0) {
                return false;
            }
            key.setDepth(nullPoint);
            IndexRowComposition irc = groupIndex.indexRowComposition();
            for (int i = groupIndex.getColumns().size(), LEN=irc.getLength(); i < LEN; ++i) {
                if (i <= nullPoint) {
                    key.append(null);
                }
                else {
                    final int flattenedIndex = irc.getFieldPosition(i);
                    Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);
                    Object value = row.field(flattenedIndex, UndefBindings.only());
                    RowDef rowDef = (RowDef) column.getTable().rowDef();
                    FieldDef fieldDef = rowDef.getFieldDef(column.getPosition());
                    fieldDef.getEncoding().toKey(fieldDef, value, key);
                }
            }
            return true;
        }

        public PersistitKeyHandler(PersistitAdapter adapter) {
            this.adapter = adapter;
        }

        // object state

        private final PersistitAdapter adapter;

        // nested classes

    }

    private static class RowAction {

        public UserTable sourceTable() {
            return sourceTable;
        }

        public Action action() {
            return action;
        }

        public RowAction(UserTable sourceTable, Action action) {
//            assert sourceTable != null : "source table is null"; // TODO this shouldn't be null, I think
            assert action != null : "action is null";
            this.sourceTable = sourceTable;
            this.action = action;
        }

        @Override
        public String toString() {
            return action().name() + ' ' + sourceTable();
        }

        private final UserTable sourceTable;
        private final Action action;
    }

    private static class BranchTables {

        // BranchTables interface

        public List<UserTableRowType> fromRoot() {
            return allTablesForBranch;
        }

        public List<UserTableRowType> fromRootMost() {
            return onlyBranch;
        }

        public boolean isEmpty() {
            return fromRootMost().isEmpty();
        }

        public UserTableRowType leafMost() {
            return onlyBranch.get(onlyBranch.size()-1);
        }

        public UserTableRowType rootMost() {
            return onlyBranch.get(0);
        }

        public BranchTables(Schema schema, GroupIndex groupIndex) {
            List<UserTableRowType> localTables = new ArrayList<UserTableRowType>();
            UserTable rootmost = groupIndex.rootMostTable();
            int branchRootmostIndex = -1;
            for (UserTable table = groupIndex.leafMostTable(); table != null; table = table.parentTable()) {
                if (table.equals(rootmost)) {
                    assert branchRootmostIndex == -1 : branchRootmostIndex;
                    branchRootmostIndex = table.getDepth();
                }
                localTables.add(schema.userTableRowType(table));
            }
            if (branchRootmostIndex < 0) {
                throw new RuntimeException("branch root not found! " + rootmost + " within " + localTables);
            }
            Collections.reverse(localTables);
            this.allTablesForBranch = Collections.unmodifiableList(localTables);
            this.onlyBranch = branchRootmostIndex == 0
                    ? allTablesForBranch
                    : allTablesForBranch.subList(branchRootmostIndex, allTablesForBranch.size());
        }

        // object state
        private final List<UserTableRowType> allTablesForBranch;
        private final List<UserTableRowType> onlyBranch;
    }

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
    
    protected interface GroupIndexHandler<A, T extends Throwable> {
        void handleRow(A action, GroupIndex groupIndex, Row row) throws T;
    }

    public enum Action {STORE, DELETE }
}
