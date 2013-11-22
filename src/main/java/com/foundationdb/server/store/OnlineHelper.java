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
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Index.IndexType;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.util.TableChange.ChangeType;
import com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
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
import com.foundationdb.qp.rowtype.ProjectedTableRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowChecker;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.expressions.TCastResolver;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.dxl.DelegatingContext;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.SchemaManager.OnlineChangeState;
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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class OnlineHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(OnlineHelper.class);
    private static final Object TRANSFORM_CACHE_KEY = new Object();

    public static void buildIndexes(Session session,
                                    QueryContext context,
                                    TransactionService txnService,
                                    SchemaManager schemaManager,
                                    Store store) {
        LOG.debug("Building indexes");
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
                                             TCastResolver castResolver) {
        LOG.debug("Checking constraints");
        txnService.beginTransaction(session);
        try {
            Collection<ChangeSet> changeSets = schemaManager.getOnlineChangeSets(session);
            assert (commonChangeLevel(changeSets) == ChangeLevel.METADATA_NOT_NULL) : changeSets;
            // Gather all tables that need scanned, keyed by group
            AkibanInformationSchema oldAIS = schemaManager.getAis(session);
            Schema oldSchema = SchemaCache.globalSchema(oldAIS);
            Multimap<Group,RowType> groupMap = HashMultimap.create();
            for(ChangeSet cs : changeSets) {
                RowType rowType = oldSchema.tableRowType(cs.getTableId());
                groupMap.put(rowType.table().getGroup(), rowType);
            }
            // Scan all affected groups
            StoreAdapter adapter = store.createAdapter(session, oldSchema);
            final TransformCache transformCache = getTransformCache(session, schemaManager, castResolver);
            for(Entry<Group, Collection<RowType>> entry : groupMap.asMap().entrySet()) {
                Operator plan = API.filter_Default(API.groupScan_Default(entry.getKey()), entry.getValue());
                runPlan(session, contextIfNull(context, adapter), txnService, plan, new RowHandler() {
                    @Override
                    public void handleRow(Row row) {
                        TableTransform transform = transformCache.get(row.rowType().typeId());
                        transform.rowChecker.checkConstraints(row);
                   }
                });
            }
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    public static void alterTable(Session session,
                                  QueryContext context,
                                  TransactionService txnService,
                                  SchemaManager schemaManager,
                                  Store store,
                                  TCastResolver castResolver) {
        LOG.debug("Altering table");
        txnService.beginTransaction(session);
        try {
            alterInternal(session, context, txnService, schemaManager, store, castResolver);
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
                                      TCastResolver castResolver) {
        final Collection<ChangeSet> changeSets = schemaManager.getOnlineChangeSets(session);
        ChangeLevel changeLevel = commonChangeLevel(changeSets);
        assert (changeLevel == ChangeLevel.TABLE || changeLevel == ChangeLevel.GROUP) : changeSets;

        final AkibanInformationSchema oldAIS = schemaManager.getAis(session);
        final AkibanInformationSchema newAIS = schemaManager.getOnlineAIS(session);

        final Schema origSchema = SchemaCache.globalSchema(oldAIS);
        final StoreAdapter origAdapter = store.createAdapter(session, origSchema);
        final QueryContext origContext = new DelegatingContext(origAdapter, context);
        final QueryBindings origBindings = origContext.createBindings();

        final TransformCache transformCache = getTransformCache(session, schemaManager, castResolver);
        Set<Table> oldRoots = findOldRoots(changeSets, oldAIS, newAIS);

        for(Table root : oldRoots) {
            Operator plan = API.groupScan_Default(root.getGroup());
            runPlan(session, contextIfNull(context, origAdapter), txnService, plan, new RowHandler() {
                @Override
                public void handleRow(Row oldRow) {
                    TableTransform transform = transformCache.get(oldRow.rowType().typeId());
                    final Row newRow;
                    if(transform.projectedRowType != null) {
                        List<? extends TPreparedExpression> pProjections = transform.projectedRowType.getProjections();
                        newRow = new ProjectedRow(transform.projectedRowType,
                                                  oldRow,
                                                  origContext,
                                                  origBindings,
                                                  // TODO: Are these reusable? Thread safe?
                                                  ProjectedRow.createTEvaluatableExpressions(pProjections),
                                                  TInstance.createTInstances(pProjections));
                        origContext.checkConstraints(newRow);
                    } else {
                        newRow = new OverlayingRow(oldRow, transform.rowType);
                    }
                    origAdapter.writeRow(newRow, transform.tableIndexes, transform.groupIndexes);
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

    private static Set<Table> findOldRoots(Collection<ChangeSet> changeSets,
                                           AkibanInformationSchema oldAIS,
                                           AkibanInformationSchema newAIS) {
        Set<Table> oldRoots = new HashSet<>();
        for(ChangeSet cs : changeSets) {
            Table oldTable = oldAIS.getTable(cs.getTableId());
            Table newTable = newAIS.getTable(cs.getTableId());
            Table oldNewTable = oldAIS.getTable(newTable.getTableId());
            oldRoots.add(oldTable.getGroup().getRoot());
            oldRoots.add(oldNewTable.getGroup().getRoot());
        }
        return oldRoots;
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

    private static ProjectedTableRowType buildProjectedRowType(ChangeSet changeSet,
                                                               Table origTable,
                                                               RowType newRowType,
                                                               boolean isGroupChange,
                                                               TCastResolver castResolver,
                                                               QueryContext origContext) {
        Table newTable = newRowType.table();
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
        return new ProjectedTableRowType(newRowType.schema(), newTable, projections, !isGroupChange);
    }

    private static TableTransform buildTableTransform(ChangeSet changeSet,
                                                      ChangeLevel changeLevel,
                                                      AkibanInformationSchema oldAIS,
                                                      RowType newRowType,
                                                      TCastResolver castResolver) {
        Table newTable = newRowType.table();
        Collection<TableIndex> tableIndexes = findTableIndexesToBuild(changeSet, newTable);
        Collection<GroupIndex> groupIndexes = findGroupIndexesToBuild(changeSet, newTable);
        TableRowChecker rowChecker = null;
        RowType finalRowType = newRowType;
        switch(changeLevel) {
            case METADATA_NOT_NULL:
                rowChecker = new TableRowChecker(newRowType);
            break;
            case TABLE:
            case GROUP:
                Table oldTable = oldAIS.getTable(newTable.getTableId());
                if((changeSet.getColumnChangeCount() > 0) ||
                   // TODO: Hidden PK changes are not in ChangeSet. They really should be.
                   (newRowType.nFields() != oldTable.getColumnsIncludingInternal().size())) {
                    finalRowType = buildProjectedRowType(changeSet,
                                                         oldTable,
                                                         newRowType,
                                                         changeLevel == ChangeLevel.GROUP,
                                                         castResolver,
                                                         new SimpleQueryContext());
                }
            break;
        }
        return new TableTransform(changeLevel,
                                  finalRowType,
                                  rowChecker,
                                  tableIndexes.toArray(new TableIndex[tableIndexes.size()]),
                                  groupIndexes);
    }

    private static void buildTransformCache(final TransformCache cache,
                                            final Collection<ChangeSet> changeSets,
                                            final AkibanInformationSchema oldAIS,
                                            final AkibanInformationSchema newAIS,
                                            final TCastResolver castResolver) {
        final ChangeLevel changeLevel = commonChangeLevel(changeSets);
        Set<Table> oldRoots = findOldRoots(changeSets, oldAIS, newAIS);
        final Schema newSchema = SchemaCache.globalSchema(newAIS);
        for(Table root : oldRoots) {
            root.visit(new AbstractVisitor() {
                @Override
                public void visit(Table table) {
                    int tableID = table.getTableId();
                    ChangeSet changeSet = findByID(changeSets, tableID);
                    RowType newType = newSchema.tableRowType(tableID);
                    TableTransform transform = buildTableTransform(changeSet, changeLevel, oldAIS, newType, castResolver);
                    TableTransform prev = cache.put(tableID, transform);
                    assert (prev == null) : tableID;
                }
            });
        }
    }

    private static TransformCache getTransformCache(final Session session,
                                                    final SchemaManager schemaManager,
                                                    final TCastResolver castResolver) {
        AkibanInformationSchema ais = schemaManager.getAis(session);
        TransformCache cache = ais.getCachedValue(TRANSFORM_CACHE_KEY, null);
        if(cache == null) {
            cache = ais.getCachedValue(TRANSFORM_CACHE_KEY, new CacheValueGenerator<TransformCache>() {
                @Override
                public TransformCache valueFor(AkibanInformationSchema ais) {
                    TransformCache cache = new TransformCache();
                    Collection<OnlineChangeState> states = schemaManager.getOnlineChangeStates(session);
                    for(OnlineChangeState s : states) {
                        buildTransformCache(cache, s.getChangeSets(), ais, s.getAIS(), castResolver);
                    }
                    return cache;
                }
            });
        }
        return cache;
    }

    /**
     * NB: Current usage is *only* with plans that have GroupScan at the bottom. Use this fact to find the bottom,
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

    public static ChangeLevel commonChangeLevel(Collection<ChangeSet> changeSets) {
        ChangeLevel level = null;
        for(ChangeSet cs : changeSets) {
            if(level == null) {
                level = ChangeLevel.valueOf(cs.getChangeLevel());
            } else if(!level.name().equals(cs.getChangeLevel())) {
                throw new IllegalStateException("Mixed ChangeLevels: " + changeSets);
            }
        }
        assert (level != null);
        return level;
    }


    //
    // Classes
    //

    private interface RowHandler {
        void handleRow(Row row);
    }

    /** Holds information about how to maintain/populate the new/modified instance of a table. */
    private static class TableTransform {
        public final ChangeLevel changeLevel;
        /** New row type for the table. */
        public final RowType rowType;
        /** Not {@code null} *iff* new rows need projected. */
        public final ProjectedTableRowType projectedRowType;
        /** Not {@code null} *iff* new rows need only be verified. */
        TableRowChecker rowChecker;
        /** Contains table indexes to build (can be empty) */
        public final TableIndex[] tableIndexes;
        /** Populated with group indexes to build (can be empty) */
        public final Collection<GroupIndex> groupIndexes;

        public TableTransform(ChangeLevel changeLevel,
                              RowType rowType,
                              TableRowChecker rowChecker,
                              TableIndex[] tableIndexes,
                              Collection<GroupIndex> groupIndexes) {
            this.changeLevel = changeLevel;
            this.rowType = rowType;
            this.projectedRowType = (rowType instanceof ProjectedTableRowType) ? (ProjectedTableRowType)rowType : null;
            this.rowChecker = rowChecker;
            this.tableIndexes = tableIndexes;
            this.groupIndexes = groupIndexes;
        }
    }

    /** Table ID -> TableTransform */
    private static class TransformCache extends HashMap<Integer,TableTransform>
    {
    }
}
