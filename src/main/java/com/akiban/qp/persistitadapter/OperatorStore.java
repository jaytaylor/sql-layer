/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.*;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.*;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.error.NoRowsUpdatedException;
import com.akiban.server.error.TooManyRowsUpdatedException;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataExtractor;
import com.akiban.server.rowdata.RowDataPValueSource;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.DelegatingStore;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.optimizer.rule.PlanGenerator;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
import com.google.inject.Inject;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

import java.util.*;

import static com.akiban.qp.operator.API.*;

public class OperatorStore extends DelegatingStore<PersistitStore> {
    /*
     * We instantiate another PersistitAdapter for doing scans/changes with the raw
     * PersistitStore explicitly passed. We don't want step management in this sub-adapter
     * or we'll get a double increment for each row (if we are already being called with it).
     */
    private static final boolean WITH_STEPS = false;


    private PersistitAdapter createAdapterNoSteps(AkibanInformationSchema ais, Session session) {
        PersistitAdapter adapter =
            new PersistitAdapter(SchemaCache.globalSchema(ais),
                                 getPersistitStore(),
                                 treeService,
                                 session,
                                 config,
                                 WITH_STEPS);
        session.put(StoreAdapter.STORE_ADAPTER_KEY, adapter);
        return adapter;
    }

    // Store interface

    private static RowData mergeRows(RowDef rowDef, RowData currentRow, RowData newRowData, ColumnSelector selector) {
        if(selector == null) {
            return newRowData;
        }
        NewRow mergedRow = NiceRow.fromRowData(currentRow, rowDef);
        NewRow newRow = new LegacyRowWrapper(rowDef, newRowData);
        int fields = rowDef.getFieldCount();
        for (int i = 0; i < fields; i++) {
            if (selector.includesColumn(i)) {
                mergedRow.put(i, newRow.get(i));
            }
        }
        return mergedRow.toRowData();
    }

    @Override
    public void updateRow(Session session, RowData oldRowData, RowData newRowData, ColumnSelector columnSelector, Index[] indexes)
    {
        if(indexes != null) {
            throw new IllegalStateException("Unexpected indexes: " + Arrays.toString(indexes));
        }

        AkibanInformationSchema ais = schemaManager.getAis(session);
        UserTable userTable = ais.getUserTable(oldRowData.getRowDefId());
        if(canSkipMaintenance(userTable)) {
            super.updateRow(session, oldRowData, newRowData, columnSelector, indexes);
            return;
        }

        UPDATE_MAINTENANCE.in();
        try {
            if(columnSelector == ConstantColumnSelector.ALL_ON) {
                columnSelector = null;
            }
            RowData mergedRow = mergeRows(userTable.rowDef(), oldRowData, newRowData, columnSelector);

            BitSet changedColumnPositions = changedColumnPositions(userTable.rowDef(), oldRowData, mergedRow);
            PersistitAdapter adapter = createAdapterNoSteps(ais, session);
            maintainGroupIndexes(session,
                                 ais,
                                 adapter,
                                 oldRowData,
                                 changedColumnPositions,
                                 OperatorStoreGIHandler.forTable(adapter, userTable),
                                 OperatorStoreGIHandler.Action.DELETE);

            super.updateRow(session, oldRowData, mergedRow, columnSelector, indexes);

            maintainGroupIndexes(session,
                                 ais,
                                 adapter,
                                 mergedRow,
                                 changedColumnPositions,
                                 OperatorStoreGIHandler.forTable(adapter, userTable),
                                 OperatorStoreGIHandler.Action.STORE);
        } finally {
            UPDATE_MAINTENANCE.out();
        }
    }

    @Override
    public void writeRow(Session session, RowData rowData) {
        AkibanInformationSchema ais = schemaManager.getAis(session);
        PersistitAdapter adapter = createAdapterNoSteps(ais, session);

        // Requires adapter created
        super.writeRow(session, rowData);

        INSERT_MAINTENANCE.in();
        try {
            UserTable uTable = ais.getUserTable(rowData.getRowDefId());
            maintainGroupIndexes(session,
                                 ais,
                                 adapter,
                                 rowData, null,
                                 OperatorStoreGIHandler.forTable(adapter, uTable),
                                 OperatorStoreGIHandler.Action.STORE);
        } finally {
            INSERT_MAINTENANCE.out();
        }
    }

    @Override
    public void deleteRow(Session session, RowData rowData, boolean deleteIndexes, boolean cascadeDelete) {
        DELETE_MAINTENANCE.in();
        try {
            AkibanInformationSchema ais = schemaManager.getAis(session);
            PersistitAdapter adapter = createAdapterNoSteps(ais, session);
            UserTable uTable = ais.getUserTable(rowData.getRowDefId());
            if(cascadeDelete) {
                cascadeDeleteMaintainGroupIndex(session, ais, adapter, rowData);
            } else { // one row, one update to group indexes
                maintainGroupIndexes(session,
                                     ais,
                                     adapter,
                                     rowData,
                                     null,
                                     OperatorStoreGIHandler.forTable(adapter, uTable),
                                     OperatorStoreGIHandler.Action.DELETE);
            }
        } finally {
            DELETE_MAINTENANCE.out();
        }

        super.deleteRow(session, rowData, deleteIndexes, cascadeDelete);
    }

    @Override
    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean defer) {
        List<TableIndex> tableIndexes = new ArrayList<>();
        List<GroupIndex> groupIndexes = new ArrayList<>();
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

        AkibanInformationSchema ais = schemaManager.getAis(session);
        PersistitAdapter adapter = createAdapterNoSteps(ais, session);

        if(!tableIndexes.isEmpty()) {
            super.buildIndexes(session, tableIndexes, defer);
        }

        QueryContext context = new SimpleQueryContext(adapter);
        for(GroupIndex groupIndex : groupIndexes) {
            Operator plan = OperatorStoreMaintenancePlans.groupIndexCreationPlan(adapter.schema(), groupIndex);
            runMaintenancePlan(
                    context,
                    groupIndex,
                    plan,
                    OperatorStoreGIHandler.forBuilding(adapter),
                    OperatorStoreGIHandler.Action.STORE
            );
        }
    }

    @Override
    public StoreAdapter createAdapter(Session session, com.akiban.qp.rowtype.Schema schema) {
        return new PersistitAdapter(schema, this, treeService, session, config);
    }

    // OperatorStore interface

    @Inject
    public OperatorStore(TreeService treeService, ConfigurationService config, SchemaManager schemaManager,
                         LockService lockService) {
        super(new PersistitStore(treeService, config, schemaManager, lockService));
        this.treeService = treeService;
        this.config = config;
        this.schemaManager = schemaManager;
    }

    @Override
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
            AkibanInformationSchema ais,
            PersistitAdapter adapter,
            RowData rowData,
            BitSet columnDifferences,
            OperatorStoreGIHandler handler,
            OperatorStoreGIHandler.Action action)
    {
        UserTable userTable = ais.getUserTable(rowData.getRowDefId());
        if(canSkipMaintenance(userTable)) {
            return;
        }
        Exchange hEx = null;
        try {
            hEx = adapter.takeExchange(userTable.getGroup());
            // the "false" at the end of constructHKey toggles whether the RowData should be modified to increment
            // the hidden PK field, if there is one. For PK-less rows, this field have already been incremented by now,
            // so we don't want to increment it again
            getPersistitStore().constructHKey(session, userTable.rowDef(), rowData, false, hEx.getKey());
            PersistitHKey persistitHKey = new PersistitHKey(adapter, userTable.hKey());
            persistitHKey.copyFrom(hEx.getKey());

            Collection<GroupIndex> branchIndexes = userTable.getGroupIndexes();
            for (GroupIndex groupIndex : optionallyOrderGroupIndexes(branchIndexes)) {
                assert !groupIndex.isUnique() : "unique GI: " + groupIndex;
                if (columnDifferences == null || groupIndex.columnsOverlap(userTable, columnDifferences)) {
                    OperatorStoreMaintenance plan = groupIndexCreationPlan(
                            ais,
                            groupIndex,
                            adapter.schema().userTableRowType(userTable));
                    plan.run(action, persistitHKey, rowData, adapter, handler);
                } else {
                    SKIP_MAINTENANCE.hit();
                }
            }
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            if(hEx != null) {
                adapter.returnExchange(hEx);
            }
        }
    }

    private void runMaintenancePlan(
            QueryContext context,
            GroupIndex groupIndex,
            Operator rootOperator,
            OperatorStoreGIHandler handler,
            OperatorStoreGIHandler.Action action)
    {
        Cursor cursor = API.cursor(rootOperator, context);
        cursor.open();
        try {
            Row row;
            while ((row = cursor.next()) != null) {
                if (row.rowType().equals(rootOperator.rowType())) {
                    handler.handleRow(groupIndex, row, action);
                }
            }
        } finally {
            cursor.destroy();
        }
    }
    
    private OperatorStoreMaintenance groupIndexCreationPlan(
            AkibanInformationSchema ais, GroupIndex groupIndex, UserTableRowType rowType
    ) {
        return OperatorStoreMaintenancePlans.forAis(ais).forRowType(groupIndex, rowType);
    }
    
    /*
     * This does the full cascading delete, updating both the group indexes for
     * each table affected and removing the rows.  
     * It does this root to leaf order. 
     */
    private void cascadeDeleteMaintainGroupIndex (Session session,
            AkibanInformationSchema ais, 
            PersistitAdapter adapter, 
            RowData rowData)
    {
        UserTable uTable = ais.getUserTable(rowData.getRowDefId());

        Operator plan = PlanGenerator.generateBranchPlan(ais, uTable);
        
        QueryContext queryContext = new SimpleQueryContext(adapter);
        Cursor cursor = API.cursor(plan, queryContext);

        List<Column> lookupCols = uTable.getPrimaryKeyIncludingInternal().getColumns();
        RowDataPValueSource pSource = new RowDataPValueSource();
        for (int i=0; i < lookupCols.size(); ++i) {
            Column col = lookupCols.get(i);
            pSource.bind(col.getFieldDef(), rowData);
            queryContext.setPValue(i, pSource);
        }
        try {
            Row row;
            cursor.open();
            while ((row = cursor.next()) != null) {
                UserTable table = row.rowType().userTable();
                RowData data = rowData(adapter, table.rowDef(), row, new PValueRowDataCreator());
                maintainGroupIndexes(session,
                        ais,
                        adapter,
                        data,
                        null,
                        OperatorStoreGIHandler.forTable(adapter, uTable),
                        OperatorStoreGIHandler.Action.CASCADE);
            }
            cursor.close();
        } finally {
            cursor.destroy();
        }
    }

    private <S> RowData rowData (PersistitAdapter adapter, RowDef rowDef, RowBase row, RowDataCreator<S> creator) {
        if (row instanceof PersistitGroupRow) {
            return ((PersistitGroupRow) row).rowData();
        }
        NewRow niceRow = adapter.newRow(rowDef);
        for(int i = 0; i < row.rowType().nFields(); ++i) {
            S source = creator.eval(row, i);
            creator.put(source, niceRow, rowDef.getFieldDef(i), i);
        }
        return niceRow.toRowData();
    }


    // private static methods

    private static BitSet changedColumnPositions(RowDef rowDef, RowData a, RowData b)
    {
        int fields = rowDef.getFieldCount();
        BitSet differences = new BitSet(fields);
        for (int f = 0; f < fields; f++) {
            long aloc = rowDef.fieldLocation(a, f);
            long bloc = rowDef.fieldLocation(b, f);
            differences.set(f,
                            !bytesEqual(a.getBytes(),
                                        (int) aloc,
                                        (int) (aloc >>> 32),
                                        b.getBytes(),
                                        (int) bloc,
                                        (int) (bloc >>> 32)));
        }
        return differences;
    }

    static boolean bytesEqual(byte[] a, int aoffset, int asize, byte[] b, int boffset, int bsize)
    {
        if (asize != bsize) {
            return false;
        }
        for (int i = 0; i < asize; i++) {
            if (a[i + aoffset] != b[i + boffset]) {
                return false;
            }
        }
        return true;
    }

    private static boolean canSkipMaintenance(UserTable userTable) {
       return userTable.getGroupIndexes().isEmpty();
    }

    
    // object state
    private final ConfigurationService config;
    private final TreeService treeService;
    private final SchemaManager schemaManager;


    // consts

    private static final InOutTap INSERT_MAINTENANCE = Tap.createTimer("write: write_gi_maintenance");
    private static final InOutTap UPDATE_MAINTENANCE = Tap.createTimer("write: update_gi_maintenance");
    private static final InOutTap DELETE_MAINTENANCE = Tap.createTimer("write: delete_gi_maintenance");
    private static final PointTap SKIP_MAINTENANCE = Tap.createCount("write: skip_maintenance");
}
