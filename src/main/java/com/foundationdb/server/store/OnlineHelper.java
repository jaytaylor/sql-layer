/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Index.IndexType;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.util.TableChange.ChangeType;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ChainedCursor;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.Rebindable;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.ProjectedRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.ConstraintChecker;
import com.foundationdb.qp.rowtype.ProjectedTableRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowChecker;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.expressions.TCastResolver;
import com.foundationdb.server.expressions.TypesRegistryService;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.dxl.DelegatingContext;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.TableChanges.Change;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.TableChanges.IndexChange;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.TCastExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OnlineHelper
{
    private final static Logger LOG = LoggerFactory.getLogger(OnlineHelper.class);

    public static void buildIndexes(Session session,
                                    QueryContext context,
                                    TransactionService txnService,
                                    SchemaManager schemaManager,
                                    Store store) {
        LOG.debug("Building constraints");
        txnService.beginTransaction(session);
        try {
            buildIndexesInternal(session, store, schemaManager, txnService, context);
            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    public static void checkTableConstraints(Session session,
                                             QueryContext context,
                                             TransactionService txnService,
                                             SchemaManager schemaManager,
                                             Store store,
                                             int tableID) {
        LOG.debug("Checking constraints: {}", tableID);
        txnService.beginTransaction(session);
        try {
            AkibanInformationSchema ais = schemaManager.getOnlineAIS(session);
            Table table = ais.getTable(tableID);
            Schema schema = SchemaCache.globalSchema(ais);
            StoreAdapter adapter = store.createAdapter(session, schema);
            RowType sourceType = schema.tableRowType(table);
            Operator plan = API.filter_Default(API.groupScan_Default(table.getGroup()),
                                               Collections.singleton(sourceType));
            final ConstraintChecker checker = new TableRowChecker(table);
            runPlan(session, contextIfNull(context, adapter), txnService, plan, new RowHandler() {
                @Override
                public void handleRow(Row row) {
                    checker.checkConstraints(row);
                }
            }
            );
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    public static void alterTable(Session session,
                                  QueryContext context,
                                  TransactionService txnService,
                                  SchemaManager schemaManager,
                                  Store store,
                                  TypesRegistryService typesRegistry,
                                  Table origTable,
                                  boolean isGroupChange) {
        LOG.debug("Altering table {}, group change: {}", origTable, isGroupChange);
        txnService.beginTransaction(session);
        try {
            alterInternal(session, context, txnService, schemaManager, store, typesRegistry, origTable, isGroupChange);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }


    //
    // Internal
    //

    private static void buildIndexesInternal(Session session,
                                             Store store,
                                             SchemaManager schemaManager,
                                             TransactionService txnService,
                                             QueryContext context) {
        AkibanInformationSchema ais = schemaManager.getOnlineAIS(session);
        Collection<ChangeSet> changeSets = schemaManager.getOnlineChangeSets(session);
        Collection<Index> allIndexes = findIndexesToBuild(changeSets, ais);
        List<TableIndex> tableIndexes = new ArrayList<>();
        List<GroupIndex> groupIndexes = new ArrayList<>();
        for(Index index : allIndexes) {
            switch(index.getIndexType()) {
                case TABLE:
                    tableIndexes.add((TableIndex)index);
                    break;
                case GROUP:
                    groupIndexes.add((GroupIndex)index);
                    break;
                // Others not managed by Store
            }
        }
        StoreAdapter adapter = store.createAdapter(session, SchemaCache.globalSchema(ais));
        if(!tableIndexes.isEmpty()) {
            buildTableIndexes(session, txnService, store, context, adapter, tableIndexes);
        }
        if(!groupIndexes.isEmpty()) {
            buildGroupIndexes(session, txnService, store, context, adapter, groupIndexes);
        }
    }

    private static void alterInternal(Session session,
                                      QueryContext context,
                                      TransactionService txnService,
                                      SchemaManager schemaManager,
                                      Store store,
                                      TypesRegistryService typesRegistry,
                                      Table origTable,
                                      boolean isGroupChange) {
        final Collection<ChangeSet> changeSets = schemaManager.getOnlineChangeSets(session);

        final AkibanInformationSchema origAIS = origTable.getAIS();
        final Schema origSchema = SchemaCache.globalSchema(origAIS);
        final StoreAdapter origAdapter = store.createAdapter(session, origSchema);
        final QueryContext origContext = new DelegatingContext(origAdapter, context);
        final QueryBindings origBindings = origContext.createBindings();

        final AkibanInformationSchema newAIS = schemaManager.getOnlineAIS(session);
        final Schema newSchema = SchemaCache.globalSchema(newAIS);
        final Table newTable = newAIS.getTable(origTable.getTableId());

        final List<TPreparedExpression> pProjections = buildProjections(changeSets,
                                                                        origTable,
                                                                        newTable,
                                                                        typesRegistry.getCastsResolver(),
                                                                        origContext);
        // PTRT for constraint checking
        final ProjectedTableRowType newTableType = new ProjectedTableRowType(newSchema, newTable, pProjections, !isGroupChange);

        // For any table, group affecting or not, the original rows are never modified.
        // The affected group is scanned fully and every row is inserted into its new
        // location (which may be multiple groups).

        // TODO: propagateDownGroup could be skipped if we were careful about insertion order (esp. for new groups)
        List<Table> origRoots = new ArrayList<>();
        if(isGroupChange && origTable.isRoot() && !newTable.isRoot()) {
            Table newRoot = newTable.getGroup().getRoot();
            Table origNewRoot = origAIS.getTable(newRoot.getName());
            origRoots.add(origNewRoot);
        }
        origRoots.add(origTable.getGroup().getRoot());

        final RowType origTableType = origSchema.tableRowType(origTable);
        final Map<RowType, RowTypeAndIndexes> typeMap = buildRowTypesAndIndexes(changeSets, origRoots, origTable, newAIS);
        for(Table root : origRoots) {
            Operator plan = API.groupScan_Default(root.getGroup());
            runPlan(session, contextIfNull(context, origAdapter), txnService, plan, new RowHandler() {
                @Override
                public void handleRow(Row oldRow) {
                    RowType oldType = oldRow.rowType();
                    RowTypeAndIndexes rti = typeMap.get(oldType);
                    final Row newRow;
                    if(oldType == origTableType) {
                        newRow = new ProjectedRow(newTableType,
                                                  oldRow,
                                                  origContext,
                                                  origBindings,
                                                  ProjectedRow.createTEvaluatableExpressions(pProjections),
                                                  TInstance.createTInstances(pProjections));
                        origContext.checkConstraints(newRow);
                    } else {
                        newRow = new OverlayingRow(oldRow, rti.rowType);
                    }
                    origAdapter.writeRow(newRow, rti.tableIndexes, rti.groupIndexes);
                }
            });
        }
    }

    private static void buildTableIndexes(final Session session,
                                          TransactionService txnService,
                                          final Store store,
                                          QueryContext context,
                                          StoreAdapter adapter,
                                          List<TableIndex> tableIndexes) {
        if(tableIndexes.isEmpty()) {
            return;
        }
        Set<Group> groups = new HashSet<>();
        final Multimap<Integer, Index> tableIDs = ArrayListMultimap.create();
        for(Index index : tableIndexes) {
            Table table = index.leafMostTable();
            tableIDs.put(table.getTableId(), index);
            groups.add(table.getGroup());
        }
        final PersistitIndexRowBuffer buffer = new PersistitIndexRowBuffer(adapter);
        for(Group group : groups) {
            Operator plan = API.groupScan_Default(group);
            runPlan(session, contextIfNull(context, adapter), txnService, plan, new RowHandler() {
                @Override
                public void handleRow(Row row) {
                    RowData rowData = ((AbstractRow)row).rowData();
                    int tableId = rowData.getRowDefId();
                    for(Index index : tableIDs.get(tableId)) {
                        store.writeIndexRow(session, index, rowData, ((PersistitHKey)row.hKey()).key(), buffer);
                    }
                }
            });
        }
    }

    private static void buildGroupIndexes(Session session,
                                          TransactionService txnService,
                                          Store store,
                                          QueryContext context,
                                          StoreAdapter adapter,
                                          Collection<GroupIndex> groupIndexes) {
        if(groupIndexes.isEmpty()) {
            return;
        }
        for(final GroupIndex groupIndex : groupIndexes) {
            final Operator plan = StoreGIMaintenancePlans.groupIndexCreationPlan(adapter.schema(), groupIndex);
            final StoreGIHandler giHandler = StoreGIHandler.forBuilding((AbstractStore)store, session);
            runPlan(session, contextIfNull(context, adapter), txnService, plan, new RowHandler() {
                @Override
                public void handleRow(Row row) {
                    if(row.rowType().equals(plan.rowType())) {
                        giHandler.handleRow(groupIndex, row, StoreGIHandler.Action.STORE);
                    }
                }
            });
        }
    }

    private static void runPlan(Session session,
                                QueryContext context,
                                TransactionService txnService,
                                Operator plan,
                                RowHandler handler) {
        LOG.debug("Running online plan: {}", plan);
        QueryBindings bindings = context.createBindings();
        Cursor cursor = API.cursor(plan, context, bindings);
        Rebindable rebindable = getRebindable(cursor);
        cursor.openTopLevel();
        try {
            boolean done = false;
            Row lastCommitted = null;
            while(!done) {
                Row row = cursor.next();
                boolean didCommit = false;
                boolean didRollback = false;
                if(row != null) {
                    handler.handleRow(row);
                    try {
                        didCommit = txnService.periodicallyCommit(session);
                    } catch(InvalidOperationException e) {
                        if(!e.getCode().isRollbackClass()) {
                            throw e;
                        }
                        didRollback = true;
                    }
                } else {
                    // Cursor exhausted, completely finished
                    didRollback = txnService.commitOrRetryTransaction(session);
                    done = didCommit = !didRollback;
                    if(didCommit) {
                        txnService.beginTransaction(session);
                    }
                }
                if(didCommit) {
                    LOG.debug("Committed up to row: {}", row);
                    lastCommitted = row;
                } else if(didRollback) {
                    LOG.debug("Rolling back to row: {}", lastCommitted);
                    cursor.closeTopLevel();
                    rebindable.rebind((lastCommitted == null) ? null : lastCommitted.hKey(), true);
                    cursor.openTopLevel();
                }
            }
        } finally {
            cursor.closeTopLevel();
        }
    }

    private static ChangeSet findByID(Collection<ChangeSet> changeSets, int tableID) {
        for(ChangeSet cs : changeSets) {
            if(cs.getTableId() == tableID) {
                return cs;
            }
        }
        return null;
    }

    /** Find all {@code ADD} or {@code MODIFY} group indexes referenced by {@code changeSets}. */
    public static Collection<Index> findIndexesToBuild(Collection<ChangeSet> changeSets, AkibanInformationSchema ais) {
        // There may be duplicates (e.g. every table has a GI it participates in)
        Collection<Index> newIndexes = new HashSet<>();
        for(ChangeSet cs : changeSets) {
            Table table = ais.getTable(cs.getTableId());
            for(IndexChange ic : cs.getIndexChangeList()) {
                ChangeType changeType = ChangeType.valueOf(ic.getChange().getChangeType());
                if(changeType == ChangeType.ADD || changeType == ChangeType.MODIFY) {
                    String name = ic.getChange().getNewName();
                    final Index index;
                    switch(IndexType.valueOf(ic.getIndexType())) {
                        case TABLE:
                            index = table.getIndexIncludingInternal(name);
                            break;
                        case FULL_TEXT:
                            index = table.getFullTextIndex(name);
                            break;
                        case GROUP:
                            index = table.getGroup().getIndex(name);
                            break;
                        default:
                            throw new IllegalStateException(ic.getIndexType());
                    }
                    assert index != null : ic;
                    newIndexes.add(index);
                }
            }
        }
        return newIndexes;
    }

    /** Find all {@code ADD} or {@code MODIFY} table indexes from {@code changeSet}. */
    private static Collection<TableIndex> findTableIndexesToBuild(ChangeSet changeSet, Table newTable) {
        if(changeSet == null) {
            return Collections.emptyList();
        }
        List<TableIndex> tableIndexes = new ArrayList<>();
        for(IndexChange ic : changeSet.getIndexChangeList()) {
            if(IndexType.TABLE.name().equals(ic.getIndexType())) {
                switch(ChangeType.valueOf(ic.getChange().getChangeType())) {
                    case ADD:
                    case MODIFY:
                        TableIndex index = newTable.getIndexIncludingInternal(ic.getChange().getNewName());
                        assert (index != null) : newTable.toString() + "," + ic;
                        tableIndexes.add(index);
                        break;
                }
            }
        }
        return tableIndexes;
    }

    /** Find all {@code ADD} or {@code MODIFY} group indexes from {@code changeSet}. */
    private static Collection<GroupIndex> findGroupIndexesToBuild(ChangeSet changeSet, Table newTable) {
        if(changeSet == null) {
            return Collections.emptyList();
        }
        List<GroupIndex> groupIndexes = new ArrayList<>();
        Group group = newTable.getGroup();
        for(IndexChange ic : changeSet.getIndexChangeList()) {
            if(IndexType.GROUP.name().equals(ic.getIndexType())) {
                switch(ChangeType.valueOf(ic.getChange().getChangeType())) {
                    case ADD:
                    case MODIFY:
                        GroupIndex index = group.getIndex(ic.getChange().getNewName());
                        assert index != null : ic;
                        groupIndexes.add(index);
                        break;
                }
            }
        }
        return groupIndexes;
    }

    /** Find {@code newColumn}'s position in {@code oldTable} or {@code null} if it wasn't present */
    private static Integer findOldPosition(List<Change> columnChanges, Table oldTable, Column newColumn) {
        String newName = newColumn.getName();
        for(Change change : columnChanges) {
            if(newName.equals(change.getNewName())) {
                switch(ChangeType.valueOf(change.getChangeType())) {
                    case ADD:
                        return null;
                    case MODIFY:
                        Column oldColumn = oldTable.getColumn(change.getOldName());
                        assert oldColumn != null : newColumn;
                        return oldColumn.getPosition();
                    case DROP:
                        throw new IllegalStateException("Dropped new column: " + newName);
                }
            }
        }
        Column oldColumn = oldTable.getColumn(newName);
        if((oldColumn == null) && newColumn.isAkibanPKColumn()) {
            return null;
        }
        // Not in change list, must be an original column
        assert oldColumn != null : newColumn;
        return oldColumn.getPosition();
    }

    private static List<TPreparedExpression> buildProjections(Collection<ChangeSet> changeSets,
                                                              Table origTable,
                                                              Table newTable,
                                                              TCastResolver castResolver,
                                                              QueryContext origContext) {
        ChangeSet changeSet = findByID(changeSets, origTable.getTableId());
        if(changeSet == null) {
            throw new IllegalStateException("No ChangeSet for table: " + origTable);
        }

        final List<Column> newColumns = newTable.getColumnsIncludingInternal();
        final List<TPreparedExpression> projections = new ArrayList<>(newColumns.size());

        for(Column newCol : newColumns) {
            Integer oldPosition = findOldPosition(changeSet.getColumnChangeList(), origTable, newCol);
            TInstance newInst = newCol.tInstance();
            if(oldPosition == null) {
                final String defaultString = newCol.getDefaultValue();
                final ValueSource defaultValueSource;
                if(defaultString == null) {
                    defaultValueSource = ValueSources.getNullSource(newInst);
                } else {
                    Value defaultValue = new Value(newInst);
                    TInstance defInstance = MString.VARCHAR.instance(defaultString.length(), false);
                    TExecutionContext executionContext = new TExecutionContext(
                        Collections.singletonList(defInstance),
                        newInst,
                        origContext
                    );
                    Value defaultSource = new Value(MString.varcharFor(defaultString), defaultString);
                    newInst.typeClass().fromObject(executionContext, defaultSource, defaultValue);
                    defaultValueSource = defaultValue;
                }
                projections.add(new TPreparedLiteral(newInst, defaultValueSource));
            } else {
                Column oldCol = origTable.getColumnsIncludingInternal().get(oldPosition);
                TInstance oldInst = oldCol.tInstance();
                TPreparedExpression pExp = new TPreparedField(oldInst, oldPosition);
                if(!oldInst.equalsExcludingNullable(newInst)) {
                    TCast cast = castResolver.cast(oldInst.typeClass(), newInst.typeClass());
                    pExp = new TCastExpression(pExp, cast, newInst, origContext);
                }
                projections.add(pExp);
            }
        }

        return projections;
    }

    private static Map<RowType, RowTypeAndIndexes> buildRowTypesAndIndexes(final Collection<ChangeSet> changeSets,
                                                                           final List<Table> origRoots,
                                                                           final Table origTable,
                                                                           final AkibanInformationSchema newAIS) {
        final Schema origSchema = SchemaCache.globalSchema(origTable.getAIS());
        final Schema newSchema = SchemaCache.globalSchema(newAIS);
        final Map<RowType, RowTypeAndIndexes> typeMap = new HashMap<>();
        for(Table root : origRoots) {
            root.visit(new AbstractVisitor() {
                @Override
                public void visit(Table table) {
                    ChangeSet changeSet = findByID(changeSets, table.getTableId());
                    RowType oldType = origSchema.tableRowType(table);
                    RowType newType = newSchema.tableRowType(newAIS.getTable(table.getTableId()));
                    Collection<TableIndex> tableIndexes = findTableIndexesToBuild(changeSet, newType.table());
                    typeMap.put(oldType,
                                new RowTypeAndIndexes(newType,
                                                      tableIndexes.toArray(new TableIndex[tableIndexes.size()]),
                                                      findGroupIndexesToBuild(changeSet, newType.table())));
                }
            });
        }
        return typeMap;
    }

    /**
     * NB: Current only usage is *only* with plans that have GroupScan at the bottom. Use this fact to find the bottom,
     * which can rebind(), for when periodicCommit() fails.
     */
    private static Rebindable getRebindable(Cursor cursor) {
        Cursor toRebind = cursor;
        while(toRebind instanceof ChainedCursor) {
            toRebind = ((ChainedCursor)toRebind).getInput();
        }
        assert (toRebind instanceof Rebindable) : toRebind;
        return (Rebindable)toRebind;
    }

    private static QueryContext contextIfNull(QueryContext context, StoreAdapter adapter) {
        return (context != null) ? context : new SimpleQueryContext(adapter);
    }


    //
    // Classes
    //

    private interface RowHandler {
        void handleRow(Row row);
    }

    private static class RowTypeAndIndexes {
        final RowType rowType;
        final TableIndex[] tableIndexes;
        final Collection<GroupIndex> groupIndexes;

        private RowTypeAndIndexes(RowType rowType, TableIndex[] tableIndexes, Collection<GroupIndex> groupIndexes) {
            this.rowType = rowType;
            this.tableIndexes = tableIndexes;
            this.groupIndexes = groupIndexes;
        }
    }
}
