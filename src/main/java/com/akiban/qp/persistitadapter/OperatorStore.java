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
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.*;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataExtractor;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.NoRowsUpdatedException;
import com.akiban.server.error.TooManyRowsUpdatedException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.AisHolder;
import com.akiban.server.store.DelegatingStore;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.util.Tap;
import com.google.inject.Inject;
import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.akiban.qp.operator.API.ancestorLookup_Default;
import static com.akiban.qp.operator.API.indexScan_Default;

public class OperatorStore extends DelegatingStore<PersistitStore> {

    // Store interface

    @Override
    public void updateRow(Session session, RowData oldRowData, RowData newRowData, ColumnSelector columnSelector) throws PersistitException
    {
        UPDATE_TOTAL.in();
        PersistitStore persistitStore = getPersistitStore();
        AkibanInformationSchema ais = persistitStore.getRowDefCache().ais();

        RowDef rowDef = persistitStore.getRowDefCache().rowDef(oldRowData.getRowDefId());
        if ((columnSelector != null) && !rowDef.table().getGroupIndexes().isEmpty()) {
            throw new RuntimeException("group index maintence won't work with partial rows");
        }

        PersistitAdapter adapter =
            new PersistitAdapter(SchemaCache.globalSchema(ais), persistitStore, treeService, session, config);
        Schema schema = adapter.schema();

        UpdateFunction updateFunction = new InternalUpdateFunction(adapter, rowDef, newRowData, columnSelector);

        UserTable userTable = ais.getUserTable(oldRowData.getRowDefId());
        GroupTable groupTable = userTable.getGroup().getGroupTable();

        TableIndex index = userTable.getPrimaryKeyIncludingInternal().getIndex();
        assert index != null : userTable;
        UserTableRowType tableType = schema.userTableRowType(userTable);
        IndexRowType indexType = tableType.indexRowType(index);
        IndexBound bound = new IndexBound(new NewRowBackedIndexRow(tableType, new LegacyRowWrapper(oldRowData, this), index),
                                          ConstantColumnSelector.ALL_ON);
        IndexKeyRange range = IndexKeyRange.bounded(indexType, bound, true, bound, true);

        Operator indexScan = indexScan_Default(indexType, false, range);
        Operator scanOp;
        scanOp = ancestorLookup_Default(indexScan, groupTable, indexType, Collections.singletonList(tableType), API.LookupOption.DISCARD_INPUT);

        // MVCC will render this useless, but for now, a limit of 1 ensures we won't see the row we just updated,
        // and therefore scan through two rows -- once to update old -> new, then to update new -> copy of new
        scanOp = com.akiban.qp.operator.API.limit_Default(scanOp, 1);

        UpdatePlannable updateOp = com.akiban.qp.operator.API.update_Default(scanOp, updateFunction);

        Transaction transaction = treeService.getTransaction(session);
        for(int retryCount=0; ; ++retryCount) {
            try {
                UPDATE_MAINTENANCE.in();
                transaction.begin();

                maintainGroupIndexes(
                        session, ais, adapter,
                        oldRowData, OperatorStoreGIHandler.forTable(adapter, userTable),
                        OperatorStoreGIHandler.Action.DELETE
                );

                runCursor(oldRowData, rowDef, updateOp, adapter);

                maintainGroupIndexes(
                        session, ais, adapter,
                        newRowData, OperatorStoreGIHandler.forTable(adapter, userTable),
                        OperatorStoreGIHandler.Action.STORE
                );

                transaction.commit();
                break;
            } catch (RollbackException e) {
                if (retryCount >= MAX_RETRIES) {
                    throw e;
                }
            } finally {
                transaction.end();
                UPDATE_MAINTENANCE.out();
            }
        }
        UPDATE_TOTAL.out();
    }

    @Override
    public void writeRow(Session session, RowData rowData) throws PersistitException {
        INSERT_TOTAL.in();
        Transaction transaction = treeService.getTransaction(session);
        for(int retryCount=0; ; ++retryCount) {
            try {
                INSERT_MAINTENANCE.in();
                transaction.begin();

                AkibanInformationSchema ais = aisHolder.getAis();
                PersistitAdapter adapter =
                    new PersistitAdapter(SchemaCache.globalSchema(ais),
                                         getPersistitStore(),
                                         treeService,
                                         session,
                                         config);
                UserTable uTable = ais.getUserTable(rowData.getRowDefId());
                super.writeRow(session, rowData);
                maintainGroupIndexes(
                        session, ais, adapter,
                        rowData, OperatorStoreGIHandler.forTable(adapter, uTable),
                        OperatorStoreGIHandler.Action.STORE
                );

                transaction.commit();
                break;
            } catch (RollbackException e) {
                if (retryCount >= MAX_RETRIES) {
                    throw e;
                }
            } finally {
                transaction.end();
                INSERT_MAINTENANCE.out();
            }
        }
        INSERT_TOTAL.out();
    }

    @Override
    public void deleteRow(Session session, RowData rowData) throws PersistitException {
        DELETE_TOTAL.in();
        Transaction transaction = treeService.getTransaction(session);
        for(int retryCount=0; ; ++retryCount) {
            try {
                DELETE_MAINTENANCE.in();
                transaction.begin();
                AkibanInformationSchema ais = aisHolder.getAis();
                PersistitAdapter adapter =
                    new PersistitAdapter(SchemaCache.globalSchema(ais),
                                         getPersistitStore(),
                                         treeService,
                                         session,
                                         config);
                UserTable uTable = ais.getUserTable(rowData.getRowDefId());

                maintainGroupIndexes(
                        session, ais, adapter,
                        rowData, OperatorStoreGIHandler.forTable(adapter, uTable),
                        OperatorStoreGIHandler.Action.DELETE
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
                DELETE_MAINTENANCE.out();
            }
        }
        DELETE_TOTAL.out();
    }

    @Override
    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean defer) {
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

        AkibanInformationSchema ais = aisHolder.getAis();
        PersistitAdapter adapter =
            new PersistitAdapter(SchemaCache.globalSchema(ais),
                                 getPersistitStore(),
                                 treeService,
                                 session,
                                 config);
        for(GroupIndex groupIndex : groupIndexes) {
            Operator plan = OperatorStoreMaintenancePlans.groupIndexCreationPlan(adapter.schema(), groupIndex);
            runMaintenancePlan(
                    adapter,
                    groupIndex,
                    plan,
                    UndefBindings.only(),
                    OperatorStoreGIHandler.forBuilding(adapter),
                    OperatorStoreGIHandler.Action.STORE
            );
        }
    }

    // OperatorStore interface

    @Inject
    public OperatorStore(AisHolder aisHolder, TreeService treeService, ConfigurationService config) {
        super(new PersistitStore(false, treeService, config));
        this.aisHolder = aisHolder;
        this.treeService = treeService;
        this.config = config;
    }

    public PersistitStore getPersistitStore() {
        return super.getDelegate();
    }

    // for use by subclasses

    protected Collection<GroupIndex> optionallyOrderGroupIndexes(Collection<GroupIndex> groupIndexes) {
        return groupIndexes;
    }

    // private methods

    private void maintainGroupIndexes(
            Session session,
            AkibanInformationSchema ais, PersistitAdapter adapter,
            RowData rowData,
            OperatorStoreGIHandler handler,
            OperatorStoreGIHandler.Action action
    )
    throws PersistitException
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

            Collection<GroupIndex> branchIndexes = new ArrayList<GroupIndex>();
            for (GroupIndex groupIndex : userTable.getGroup().getIndexes()) {
                if (groupIndex.leafMostTable().isDescendantOf(userTable)) {
                    branchIndexes.add(groupIndex);
                }
            }

            for (GroupIndex groupIndex : optionallyOrderGroupIndexes(branchIndexes)) {
                assert !groupIndex.isUnique() : "unique GI: " + groupIndex;
                OperatorStoreMaintenance plan = groupIndexCreationPlan(
                        ais,
                        groupIndex,
                        adapter.schema().userTableRowType(userTable)
                );
                plan.run(action, persistitHKey, rowData, adapter, handler);
            }
        } finally {
            adapter.returnExchange(hEx);
        }
    }

    private void runMaintenancePlan(
            PersistitAdapter adapter,
            GroupIndex groupIndex,
            Operator rootOperator,
            Bindings bindings,
            OperatorStoreGIHandler handler,
            OperatorStoreGIHandler.Action action
    )
    {
        Cursor cursor = API.cursor(rootOperator, adapter);
        cursor.open(bindings);
        try {
            Row row;
            while ((row = cursor.next()) != null) {
                if (row.rowType().equals(rootOperator.rowType())) {
                    handler.handleRow(groupIndex, row, action);
                }
            }
        } finally {
            cursor.close();
        }
    }

    private OperatorStoreMaintenance groupIndexCreationPlan(
            AkibanInformationSchema ais, GroupIndex groupIndex, UserTableRowType rowType
    ) {
        return OperatorStoreMaintenancePlans.forAis(ais).forRowType(groupIndex, rowType);
    }

    // private static methods

    private static void runCursor(RowData oldRowData, RowDef rowDef, UpdatePlannable plannable, PersistitAdapter adapter)
    {
        final UpdateResult result  = plannable.run(UndefBindings.only(), adapter);
        if (result.rowsModified() == 0 || result.rowsTouched() == 0) {
            throw new NoRowsUpdatedException (oldRowData, rowDef);
        }
        else if(result.rowsModified() != 1 || result.rowsTouched() != 1) {
            throw new TooManyRowsUpdatedException (oldRowData, rowDef, result);
        }
    }

    // object state
    private final ConfigurationService config;
    private final TreeService treeService;
    private final AisHolder aisHolder;

    // consts

    private static final int MAX_RETRIES = 10;
    private static final Tap.InOutTap INSERT_TOTAL = Tap.createTimer("write: write_total");
    private static final Tap.InOutTap UPDATE_TOTAL = Tap.createTimer("write: update_total");
    private static final Tap.InOutTap DELETE_TOTAL = Tap.createTimer("write: delete_total");
    private static final Tap.InOutTap INSERT_MAINTENANCE = Tap.createTimer("write: write_maintenance");
    private static final Tap.InOutTap UPDATE_MAINTENANCE = Tap.createTimer("write: update_maintenance");
    private static final Tap.InOutTap DELETE_MAINTENANCE = Tap.createTimer("write: delete_maintenance");


    // nested classes

    private static class InternalUpdateFunction implements UpdateFunction {
        private final PersistitAdapter adapter;
        private final RowData newRowData;
        private final ColumnSelector columnSelector;
        private final RowDef rowDef;
        private final RowDataExtractor extractor;

        private InternalUpdateFunction(PersistitAdapter adapter, RowDef rowDef, RowData newRowData, ColumnSelector columnSelector) {
            this.newRowData = newRowData;
            this.columnSelector = columnSelector;
            this.rowDef = rowDef;
            this.adapter = adapter;
            this.extractor = new RowDataExtractor(newRowData, rowDef);
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
            NewRow newRow = adapter.newRow(rowDef);
            ToObjectValueTarget target = new ToObjectValueTarget();
            for (int i=0; i < original.rowType().nFields(); ++i) {
                if (columnSelector.includesColumn(i)) {
                    Object value = extractor.get(rowDef.getFieldDef(i));
                    newRow.put(i, value);
                }
                else {
                    ValueSource source = original.eval(i);
                    newRow.put(i, target.convertFromSource(source));
                }
            }
            return PersistitGroupRow.newPersistitGroupRow(adapter, newRow.toRowData());
        }
    }
}
