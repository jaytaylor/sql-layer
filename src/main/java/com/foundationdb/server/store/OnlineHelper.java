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
import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ChainedCursor;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.Rebindable;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.Delete_Returning;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.ProjectedRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.ProjectedTableRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.storeadapter.RowDataRow;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.ConcurrentViolationException;
import com.foundationdb.server.error.ConstraintViolationException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchRowException;
import com.foundationdb.server.error.NotAllowedByConfigException;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.dxl.DelegatingContext;
import com.foundationdb.server.service.listener.RowListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.SchemaManager.OnlineChangeState;
import com.foundationdb.server.store.TableChanges.Change;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.TableChanges.IndexChange;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TCastExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.optimizer.CreateAsCompiler;
import com.foundationdb.sql.optimizer.plan.BasePlannable;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.PlanGenerator;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerSession;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.persistit.Key;
import com.persistit.KeyState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class OnlineHelper implements RowListener
{
    private static final Logger LOG = LoggerFactory.getLogger(OnlineHelper.class);
    private static final Object TRANSFORM_CACHE_KEY = new Object();

    private final TransactionService txnService;
    private final SchemaManager schemaManager;
    private final Store store;
    private final TypesRegistryService typesRegistry;
    private final ConstraintHandler constraintHandler;
    private final boolean withConcurrentDML;

    public OnlineHelper(TransactionService txnService,
                        SchemaManager schemaManager,
                        Store store,
                        TypesRegistryService typesRegistry,
                        ConstraintHandler constraintHandler,
                        boolean withConcurrentDML) {
        this.txnService = txnService;
        this.schemaManager = schemaManager;
        this.store = store;
        this.typesRegistry = typesRegistry;
        this.constraintHandler = constraintHandler;
        this.withConcurrentDML = withConcurrentDML;
    }

    public void buildIndexes(Session session, QueryContext context) {
        LOG.debug("Building indexes");
        txnService.beginTransaction(session);
        try {
            buildIndexesInternal(session, context);
            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    public void checkTableConstraints(final Session session, QueryContext context) {
        LOG.debug("Checking constraints");
        txnService.beginTransaction(session);
        try {
            Collection<ChangeSet> changeSets = schemaManager.getOnlineChangeSets(session);
            assert (commonChangeLevel(changeSets) == ChangeLevel.METADATA_CONSTRAINT) : changeSets;
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
            final TransformCache transformCache = getTransformCache(session);
            for(Entry<Group, Collection<RowType>> entry : groupMap.asMap().entrySet()) {
                Operator plan = API.filter_Default(API.groupScan_Default(entry.getKey()), entry.getValue());
                runPlan(session, contextIfNull(context, adapter), schemaManager,  txnService, plan, new RowHandler() {
                    @Override
                    public void handleRow(Row row) {
                        simpleCheckConstraints(session, transformCache, row);
                   }
                });
            }
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    public void alterTable(Session session, QueryContext context) {
        LOG.debug("Altering table");
        txnService.beginTransaction(session);
        try {
            alterInternal(session, context);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }


    //
    // RowListener
    //

    @Override
    public void onInsertPost(Session session, Table table, Key hKey, RowData rowData) {
        TableTransform transform = getConcurrentDMLTransform(session, table);
        if(transform == null) {
            return;
        }
        try {
            concurrentDML(session, transform, hKey, null, rowData);
        } catch(ConstraintViolationException e) {
            setOnlineError(session, table, e);
        }
    }

    @Override
    public void onUpdatePre(Session session, Table table, Key hKey, RowData oldRowData, RowData newRowData) {
        TableTransform transform = getConcurrentDMLTransform(session, table);
        if(transform == null) {
            return;
        }
        try {
            concurrentDML(session, transform, hKey, oldRowData, null);
        } catch(ConstraintViolationException e) {
            setOnlineError(session, table, e);
        }
    }

    @Override
    public void onUpdatePost(Session session, Table table, Key hKey, RowData oldRowData, RowData newRowData) {
        TableTransform transform = getConcurrentDMLTransform(session, table);
        if(transform == null) {
            return;
        }
        try {
            concurrentDML(session, transform, hKey, null, newRowData);
        } catch(ConstraintViolationException e) {
            setOnlineError(session, table, e);
        }
    }

    @Override
    public void onDeletePre(Session session, Table table, Key hKey, RowData rowData) {
        TableTransform transform = getConcurrentDMLTransform(session, table);
        if(transform == null) {
            return;
        }
        try {
            concurrentDML(session, transform, hKey, rowData, null);
        } catch(ConstraintViolationException e) {
            setOnlineError(session, table, e);
        }
    }


    //
    // ConstraintHandler.Handler-ish
    //

    public void handleInsert(Session session, Table table, RowData row) {
        TableTransform transform = getConcurrentDMLTransform(session, table);
        if(transform == null) {
            return;
        }
        if(transform.checkConstraints) {
            boolean orig = txnService.setForceImmediateForeignKeyCheck(session, true);
            try {
                constraintHandler.handleInsert(session, transform.rowType.table(), row);
            } catch(ConstraintViolationException e) {
                setOnlineError(session, table, e);
            } finally {
                txnService.setForceImmediateForeignKeyCheck(session, orig);
            }
        }
    }

    public void handleUpdatePre(Session session, Table table, RowData oldRow, RowData newRow) {
        TableTransform transform = getConcurrentDMLTransform(session, table);
        if(transform == null) {
            return;
        }
        if(transform.checkConstraints) {
            boolean orig = txnService.setForceImmediateForeignKeyCheck(session, true);
            try {
                constraintHandler.handleUpdatePre(session, transform.rowType.table(), oldRow, newRow);
            } catch(ConstraintViolationException e) {
                setOnlineError(session, table, e);
            } finally {
                txnService.setForceImmediateForeignKeyCheck(session, orig);
            }
        }
    }

    public void handleUpdatePost(Session session, Table table, RowData oldRow, RowData newRow) {
        TableTransform transform = getConcurrentDMLTransform(session, table);
        if(transform == null) {
            return;
        }
        if(transform.checkConstraints) {
            boolean orig = txnService.setForceImmediateForeignKeyCheck(session, true);
            try {
                constraintHandler.handleUpdatePost(session, transform.rowType.table(), oldRow, newRow);
            } catch(ConstraintViolationException e) {
                setOnlineError(session, table, e);
            } finally {
                txnService.setForceImmediateForeignKeyCheck(session, orig);
            }
        }
    }

    public void handleDelete(Session session, Table table, RowData row) {
        TableTransform transform = getConcurrentDMLTransform(session, table);
        if(transform == null) {
            return;
        }
        if(transform.checkConstraints) {
            boolean orig = txnService.setForceImmediateForeignKeyCheck(session, true);
            try {
                constraintHandler.handleDelete(session, transform.rowType.table(), row);
            } catch(ConstraintViolationException e) {
                setOnlineError(session, table, e);
            } finally {
                txnService.setForceImmediateForeignKeyCheck(session, orig);
            }
        }
    }

    public void handleTruncate(Session session, Table table) {
        TableTransform transform = getConcurrentDMLTransform(session, table);
        if(transform == null) {
            return;
        }
        if(transform.checkConstraints) {
            boolean orig = txnService.setForceImmediateForeignKeyCheck(session, true);
            try {
                constraintHandler.handleTruncate(session, transform.rowType.table());
            } catch(ConstraintViolationException e) {
                setOnlineError(session, table, e);
            } finally {
                txnService.setForceImmediateForeignKeyCheck(session, orig);
            }
        }
    }


    //
    // Internal
    //

    private void setOnlineError(Session session, Table t, ConstraintViolationException e) {
        // Note: Written in the same transaction executing DML, checked in session executing DDL
        schemaManager.setOnlineDMLError(session, t.getTableId(), e.getMessage());
    }

    private void buildIndexesInternal(Session session, QueryContext context) {
        Collection<ChangeSet> changeSets = schemaManager.getOnlineChangeSets(session);
        ChangeLevel changeLevel = commonChangeLevel(changeSets);
        assert (changeLevel == ChangeLevel.INDEX || changeLevel == ChangeLevel.INDEX_CONSTRAINT) : changeSets;
        TransformCache transformCache = getTransformCache(session);
        Multimap<Group,RowType> tableIndexes = HashMultimap.create();
        Set<GroupIndex> groupIndexes = new HashSet<>();
        for(ChangeSet cs : changeSets) {
            TableTransform transform = transformCache.get(cs.getTableId());
            tableIndexes.put(transform.rowType.table().getGroup(), transform.rowType);
            groupIndexes.addAll(transform.groupIndexes);
        }

        AkibanInformationSchema onlineAIS = schemaManager.getOnlineAIS(session);
        StoreAdapter adapter = store.createAdapter(session, SchemaCache.globalSchema(onlineAIS));
        if(!tableIndexes.isEmpty()) {
            buildTableIndexes(session, context, adapter, transformCache, tableIndexes);
        }
        if(!groupIndexes.isEmpty()) {
            if(changeLevel == ChangeLevel.INDEX_CONSTRAINT) {
                throw new IllegalStateException("Constraint and group indexes");
            }
            buildGroupIndexes(session, context, adapter, groupIndexes);
        }
    }

    public void CreateAsSelect(final Session session, QueryContext context, final ServerSession server, String queryExpression, TableName tableName){
        LOG.debug("Creating Table As Select Online");

        txnService.beginTransaction(session);
        try {
            SQLParser parser = new SQLParser();
            StatementNode stmt;
            String statement = "insert into " + tableName.toStringEscaped() + " " + queryExpression;
            try {
                stmt = parser.parseStatement(statement);
            } catch (StandardException e) {
                e.printStackTrace();
                throw new RuntimeException(e);//make specific runtime error unexpectedException
            }
            AkibanInformationSchema onlineAIS = schemaManager.getOnlineAIS(session);
            StoreAdapter adapter = store.createAdapter(session, SchemaCache.globalSchema(onlineAIS));
            CreateAsCompiler compiler = new CreateAsCompiler(server, adapter, false, onlineAIS);
            DMLStatementNode dmlStmt = (DMLStatementNode) stmt;
            PlanContext planContext = new PlanContext(compiler);

            BasePlannable result = compiler.compile(dmlStmt, null, planContext);

            Plannable plannable = result.getPlannable();
            QueryContext newContext = contextIfNull(context, adapter);
            getTransformCache(session, server);
            runPlan(session, newContext, schemaManager, txnService, (Operator) plannable, null);
        }catch(Throwable t){
            t.printStackTrace();
            throw t;
        }
        finally{
            txnService.commitTransaction(session);
        }
    }

    private void alterInternal(Session session, QueryContext context) {
        final Collection<ChangeSet> changeSets = schemaManager.getOnlineChangeSets(session);
        final ChangeLevel changeLevel = commonChangeLevel(changeSets);
        assert (changeLevel == ChangeLevel.TABLE || changeLevel == ChangeLevel.GROUP) : changeSets;

        final AkibanInformationSchema origAIS = schemaManager.getAis(session);
        final AkibanInformationSchema newAIS = schemaManager.getOnlineAIS(session);

        final Schema origSchema = SchemaCache.globalSchema(origAIS);
        final StoreAdapter origAdapter = store.createAdapter(session, origSchema);
        final QueryContext origContext = new DelegatingContext(origAdapter, context);
        final QueryBindings origBindings = origContext.createBindings();

        final TransformCache transformCache = getTransformCache(session);
        Set<Table> origRoots = findOldRoots(changeSets, origAIS, newAIS);

        for(Table root : origRoots) {
            Operator plan = API.groupScan_Default(root.getGroup());
            runPlan(session, contextIfNull(context, origAdapter), schemaManager, txnService, plan, new RowHandler() {
                @Override
                public void handleRow(Row oldRow) {
                    TableTransform transform = transformCache.get(oldRow.rowType().typeId());
                    Row newRow = transformRow(origContext, origBindings, transform, oldRow);
                    origAdapter.writeRow(newRow, transform.tableIndexes, transform.groupIndexes,
                            // if adding a PK, fillHiddenPK has no effect
                            // if dropping the PK we want to fill the hidden PK
                            // otherwise we don't want to fill the hidden PK, we want to copy the values
                            changeLevel == ChangeLevel.GROUP);
                }
            });
        }
    }

    private void buildTableIndexes(final Session session,
                                   QueryContext context,
                                   StoreAdapter adapter,
                                   final TransformCache transformCache,
                                   Multimap<Group,RowType> tableIndexes) {
        final PersistitIndexRowBuffer buffer = new PersistitIndexRowBuffer(adapter);
        for(Entry<Group, Collection<RowType>> entry : tableIndexes.asMap().entrySet()) {
            if(entry.getValue().isEmpty()) {
                continue;
            }
            Operator plan = API.filter_Default(
                    API.groupScan_Default(entry.getKey()),
                    entry.getValue()
            );
            runPlan(session, contextIfNull(context, adapter), schemaManager, txnService, plan, new RowHandler() {
                @Override
                public void handleRow(Row row) {
                    RowData rowData = ((AbstractRow)row).rowData();
                    TableTransform transform = transformCache.get(rowData.getRowDefId());
                    TableIndex[] indexes = transform.tableIndexes;
                    simpleCheckConstraints(session, transform, rowData);
                    for(TableIndex index : indexes) {
                        store.writeIndexRow(session, index, rowData, ((PersistitHKey)row.hKey()).key(), buffer, true);
                    }
                }
            });
        }
    }

    private void buildGroupIndexes(final Session session,
                                   QueryContext context,
                                   StoreAdapter adapter,
                                   Collection<GroupIndex> groupIndexes) {
        if(groupIndexes.isEmpty()) {
            return;
        }
        for(final GroupIndex groupIndex : groupIndexes) {
            Schema schema = adapter.schema();
            final Operator plan = StoreGIMaintenancePlans.groupIndexCreationPlan(schema, groupIndex);
            final StoreGIHandler giHandler = StoreGIHandler.forBuilding((AbstractStore)store, session, schema, groupIndex);
            runPlan(session, contextIfNull(context, adapter), schemaManager, txnService, plan, new RowHandler() {
                @Override
                public void handleRow(Row row) {
                    giHandler.handleRow(groupIndex, row, StoreGIHandler.Action.STORE);
                }
            });
        }
    }

    private void simpleCheckConstraints(Session session, TransformCache transformCache, Row row) {
        TableTransform transform = transformCache.get(row.rowType().typeId());
        simpleCheckConstraints(session, transform, ((AbstractRow) row).rowData());
    }

    private void simpleCheckConstraints(Session session, TableTransform transform, RowData rowData) {
        if(transform == null || !transform.checkConstraints) {
            return;
        }
        constraintHandler.handleInsert(session, transform.rowType.table(), rowData);
    }

    private void concurrentDML(final Session session, TableTransform transform, Key hKey, RowData oldRowData, RowData newRowData) {
        final boolean doDelete = (oldRowData != null);
        final boolean doWrite = (newRowData != null);
        QueryContext context = null;
        switch(transform.changeLevel) {
            case INDEX:
                if(transform.tableIndexes.length > 0) {
                    PersistitIndexRowBuffer buffer = new PersistitIndexRowBuffer(store);
                    for (TableIndex index : transform.tableIndexes) {
                        if (doDelete) {
                            store.deleteIndexRow(session, index, oldRowData, hKey, buffer, false);
                        }
                        if (doWrite) {
                            store.writeIndexRow(session, index, newRowData, hKey, buffer, false);
                        }
                    }
                }
                if (!transform.groupIndexes.isEmpty()) {
                    if (doDelete) {
                        store.deleteIndexRows(session, transform.rowType.table(), oldRowData, transform.groupIndexes);
                    }
                    if (doWrite) {
                        store.writeIndexRows(session, transform.rowType.table(), newRowData, transform.groupIndexes);
                    }
                }
                break;
            case TABLE:
                if(transform.deleteOperator != null && transform.insertOperator != null) {
                    Schema schema = transform.rowType.schema();
                    StoreAdapter adapter = store.createAdapter(session, schema);
                    context = new SimpleQueryContext(adapter);
                    QueryBindings bindings = context.createBindings();
                    if (doDelete) {
                        Row origOldRow = new RowDataRow(transform.rowType, oldRowData);
                        bindings.setRow(2, origOldRow);
                        try {
                            runPlan(context, transform.deleteOperator, bindings);
                        } catch (NoSuchRowException e) {
                            LOG.debug("row not present: {}", origOldRow);
                        }
                    }
                    if (doWrite) {
                        Row origOldRow = new RowDataRow(transform.rowType, newRowData);
                        bindings.setRow(2, origOldRow);
                        try {
                            runPlan(context, transform.insertOperator, bindings);
                        } catch (NoSuchRowException e) {
                            LOG.debug("row not present: {}", origOldRow);
                        }
                    }
                    break;
                }
            case GROUP:
                Schema schema = transform.rowType.schema();
                StoreAdapter adapter = store.createAdapter(session, schema);
                context = new SimpleQueryContext(adapter);
                QueryBindings bindings = context.createBindings();
                if (doDelete) {
                    Row origOldRow = new RowDataRow(transform.rowType, oldRowData);
                    Row newOldRow = transformRow(context, bindings, transform, origOldRow);
                    try {
                        adapter.deleteRow(newOldRow, false);
                    } catch (NoSuchRowException e) {
                        LOG.debug("row not present: {}", newOldRow);
                    }
                }
                if (doWrite) {
                    Row origNewRow = new RowDataRow(transform.rowType, newRowData);
                    Row newNewRow = transformRow(context, bindings, transform, origNewRow);
                    adapter.writeRow(newNewRow, transform.tableIndexes, transform.groupIndexes, true);
                }
                break;
        }
        transform.hKeySaver.save(schemaManager, session, hKey);
    }

    private TransformCache getTransformCache(final Session session) {
        AkibanInformationSchema ais = schemaManager.getAis(session);
        TransformCache cache = ais.getCachedValue(TRANSFORM_CACHE_KEY, null);
        if(cache == null) {
            cache = ais.getCachedValue(TRANSFORM_CACHE_KEY, new CacheValueGenerator<TransformCache>() {
                @Override
                public TransformCache valueFor(AkibanInformationSchema ais) {
                    TransformCache cache = new TransformCache();
                    TypesTranslator typesTranslator = schemaManager.getTypesTranslator();
                    Collection<OnlineChangeState> states = schemaManager.getOnlineChangeStates(session);
                    for(OnlineChangeState s : states) {
                        buildTransformCache(cache, s.getChangeSets(), ais, s.getAIS(), typesRegistry, typesTranslator, null, null);
                    }
                    return cache;
                }
            });
        }
        return cache;
    }

    private TransformCache getTransformCache(final Session session, final ServerSession server) {
        AkibanInformationSchema ais = schemaManager.getAis(session);
        TransformCache cache = ais.getCachedValue(TRANSFORM_CACHE_KEY, null);
        if(cache == null) {
            cache = ais.getCachedValue(TRANSFORM_CACHE_KEY, new CacheValueGenerator<TransformCache>() {
                @Override
                public TransformCache valueFor(AkibanInformationSchema ais) {
                    TransformCache cache = new TransformCache();
                    TypesTranslator typesTranslator = schemaManager.getTypesTranslator();
                    Collection<OnlineChangeState> states = schemaManager.getOnlineChangeStates(session);
                    for(OnlineChangeState s : states) {
                        buildTransformCache(cache, s.getChangeSets(), ais, s.getAIS(), typesRegistry, typesTranslator, session, server);
                    }
                    return cache;
                }
            });
        }
        return cache;
    }

    private TableTransform getConcurrentDMLTransform(Session session, Table table) {
        if(!schemaManager.isOnlineActive(session, table.getTableId())) {
            return null;
        }
        if(!withConcurrentDML) {
            throw new NotAllowedByConfigException("DML during online DDL");
        }
        TableTransform transform = getTransformCache(session).get(table.getTableId());
        if(isTransformedTable(transform, table)) {
            return null;
        }
        return transform;
    }


    //
    // Static
    //

    private void buildTransformCache(TransformCache cache,
                                     Collection<ChangeSet> changeSets,
                                     AkibanInformationSchema oldAIS,
                                     AkibanInformationSchema newAIS,
                                     TypesRegistryService typesRegistry,
                                     TypesTranslator typesTranslator,
                                     Session session,
                                     ServerSession server) {

        final ChangeLevel changeLevel = commonChangeLevel(changeSets);
        final Schema newSchema = SchemaCache.globalSchema(newAIS);
        Plannable deletePlan = null;
        Plannable insertPlan = null;
        for(ChangeSet cs : changeSets) {
            if(cs.hasCreateAsStatement()) {
                SQLParser parser = new SQLParser();
                StatementNode insertStmt;
                try {
                    insertStmt = parser.parseStatement("insert into " + newAIS.getTable(cs.getToTableId()).getName().toStringEscaped() + " " + cs.getCreateAsStatement());
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
                StoreAdapter adapter = store.createAdapter(session, SchemaCache.globalSchema(newAIS));
                CreateAsCompiler compiler = new CreateAsCompiler(server, adapter, true, newAIS);
                PlanContext planContext = new PlanContext(compiler);
                BasePlannable insertResult = compiler.compile((DMLStatementNode) insertStmt, null, planContext);
                insertPlan = insertResult.getPlannable();
                deletePlan = new Delete_Returning(insertPlan.getInputOperators().iterator().next(), false);
            }
            int tableID = cs.getTableId();
            TableRowType newType = newSchema.tableRowType(tableID);
            TableTransform transform = buildTableTransform(cs, changeLevel, oldAIS, newType, typesRegistry,
                                                typesTranslator, (Operator)deletePlan, (Operator)insertPlan);
            TableTransform prev = cache.put(tableID, transform);
            assert (prev == null) : tableID;
        }
    }

    private static void runPlan(Session session,
                                QueryContext context,
                                SchemaManager schemaManager,
                                TransactionService txnService,
                                Operator plan,
                                RowHandler handler) {
        LOG.debug("Running online plan: {}", plan);
        Map<RowType,HKeyChecker> checkers = new HashMap<>();
        QueryBindings bindings = context.createBindings();
        Cursor cursor = API.cursor(plan, context, bindings);
        Rebindable rebindable = getRebindable(cursor);
        cursor.openTopLevel();
        try {
            boolean done = false;
            Row lastCommitted = null;
            boolean checkOnlineError = true;
            while(!done) {
                Row row = cursor.next();
                boolean didCommit = false;
                boolean didRollback = false;
                if(checkOnlineError) {
                    // Checked once per transaction here and in final phase in DDLFunctions
                    checkOnlineError(session, schemaManager);
                    checkOnlineError = false;
                }
                if(row != null) {
                    RowType rowType = row.rowType();
                    // No way to pre-populate this map as Operator#rowType() is optional and insufficient.
                    HKeyChecker checker = checkers.get(rowType);
                    if(checker == null) {
                        if(rowType.hasTable()) {
                            checker = new SchemaManagerChecker(rowType.table().getTableId());
                        } else {
                            checker = new FalseChecker();
                        }
                        checkers.put(row.rowType(), checker);
                    }
                    try {
                        if(handler != null) {
                            Key hKey = ((PersistitHKey) row.hKey()).key();
                            if (!checker.contains(schemaManager, session, hKey)) {
                                handler.handleRow(row);
                            } else {
                                LOG.trace("skipped row: {}", row);
                            }
                        }
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
                    checkOnlineError = true;
                    lastCommitted = row;
                    checkers.clear();
                } else if(didRollback) {
                    LOG.debug("Rolling back to row: {}", lastCommitted);
                    checkOnlineError = true;
                    checkers.clear();
                    txnService.rollbackTransactionIfOpen(session);
                    txnService.beginTransaction(session);
                    cursor.closeTopLevel();
                    rebindable.rebind((lastCommitted == null) ? null : lastCommitted.hKey(), true);
                    cursor.openTopLevel();
                }
            }
        } finally {
            cursor.closeTopLevel();
        }
    }

    private static void runPlan(QueryContext context,
                                Operator plan,
                                QueryBindings bindings) {
        LOG.debug("Running online DML plan: {}", plan);
        Map<RowType,HKeyChecker> checkers = new HashMap<>();
        Cursor cursor = API.cursor(plan, context, bindings);
        cursor.openTopLevel();//open up top cursor
        try {
            boolean done = false;
            while(!done) {
                Row row = cursor.next();
                if(row != null) {
                    RowType rowType = row.rowType();
                    HKeyChecker checker = checkers.get(rowType);
                    if (checker == null) {
                        if (rowType.hasTable()) {
                            checker = new SchemaManagerChecker(rowType.table().getTableId());
                        } else {
                            checker = new FalseChecker();
                        }
                        checkers.put(row.rowType(), checker);
                    }
                }
                done = true;
            }
        } finally {
            cursor.closeTopLevel();
        }
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

    private static void checkOnlineError(Session session, SchemaManager sm) {
        String msg = sm.getOnlineDMLError(session);
        if(msg != null) {
            throw new ConcurrentViolationException(msg);
        }
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
                                                               TypesRegistryService typesRegistry,
                                                               TypesTranslator typesTranslator,
                                                               QueryContext origContext) {
        Table newTable = newRowType.table();
        final List<Column> newColumns = newTable.getColumnsIncludingInternal();
        final List<TPreparedExpression> projections = new ArrayList<>(newColumns.size());
        for(Column newCol : newColumns) {
            Integer oldPosition = findOldPosition(changeSet.getColumnChangeList(), origTable, newCol);
            TInstance newInst = newCol.getType();
            if(oldPosition == null) {
                projections.add(buildColumnDefault(newCol, typesRegistry, typesTranslator, origContext));
            } else {
                Column oldCol = origTable.getColumnsIncludingInternal().get(oldPosition);
                TInstance oldInst = oldCol.getType();
                TPreparedExpression pExp = new TPreparedField(oldInst, oldPosition);
                if(!oldInst.equalsExcludingNullable(newInst)) {
                    TCast cast = typesRegistry.getCastsResolver().cast(oldInst.typeClass(), newInst.typeClass());
                    pExp = new TCastExpression(pExp, cast, newInst);
                }
                projections.add(pExp);
            }
        }
        return new ProjectedTableRowType(newRowType.schema(), newTable, projections, !isGroupChange);
    }

    // This should be quite similar to ExpressionAssembler#assembleColumnDefault()
    private static TPreparedExpression buildColumnDefault(Column newCol,
                                                          TypesRegistryService typesRegistry,
                                                          TypesTranslator typesTranslator,
                                                          QueryContext origContext) {
        return PlanGenerator.generateDefaultExpression(newCol,
                                                       null,
                                                       typesRegistry,
                                                       typesTranslator,
                                                       origContext);
    }

    private static TableTransform buildTableTransform(ChangeSet changeSet,
                                                      ChangeLevel changeLevel,
                                                      AkibanInformationSchema oldAIS,
                                                      TableRowType newRowType,
                                                      TypesRegistryService typesRegistry,
                                                      TypesTranslator typesTranslator,
                                                      Operator deleteOperator,
                                                      Operator insertOperator) {
        Table newTable = newRowType.table();
        Collection<TableIndex> tableIndexes = findTableIndexesToBuild(changeSet, newTable);
        Collection<GroupIndex> groupIndexes = findGroupIndexesToBuild(changeSet, newTable);
        ProjectedTableRowType projectedRowType = null;
        boolean checkConstraints = false;
        switch(changeLevel) {
            case METADATA_CONSTRAINT:
            case INDEX_CONSTRAINT:
                checkConstraints = true;
                assert groupIndexes.isEmpty() : groupIndexes;
            break;
            case TABLE:
                if(deleteOperator != null) break;
            case GROUP:
                Table oldTable = oldAIS.getTable(newTable.getTableId());
                if((changeSet.getColumnChangeCount() > 0) ||
                   // TODO: Hidden PK changes are not in ChangeSet. They really should be.
                   (newRowType.nFields() != oldTable.getColumnsIncludingInternal().size())) {
                    projectedRowType = buildProjectedRowType(changeSet,
                                                             oldTable,
                                                             newRowType,
                                                             changeLevel == ChangeLevel.GROUP,
                                                             typesRegistry,
                                                             typesTranslator,
                                                             new SimpleQueryContext());
                }
            break;
        }
        return new TableTransform(changeLevel,
                                  new SchemaManagerSaver(changeSet.getTableId()),
                                  newRowType,
                                  projectedRowType,
                                  checkConstraints,
                                  tableIndexes.toArray(new TableIndex[tableIndexes.size()]),
                                  groupIndexes,
                                  deleteOperator,
                                  insertOperator);
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
        if(context == null) {
            return new SimpleQueryContext(adapter);
        }
        assert(context.getSession() != null);
        return new DelegatingContext(adapter, context);
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

    /** Check if {@code table} is the post-transform/online DDL state. Use to avoid skip double-handling a row. */
    private static boolean isTransformedTable(TableTransform transform, Table table) {
        return (transform.rowType.table() == table);
    }

    private static Row transformRow(QueryContext context,
                                    QueryBindings bindings,
                                    TableTransform transform,
                                    Row origRow) {
        final Row newRow;
        if(transform.projectedRowType != null) {
            List<? extends TPreparedExpression> pProjections = transform.projectedRowType.getProjections();
            newRow = new ProjectedRow(transform.projectedRowType,
                                      origRow,
                                      context,
                                      bindings,
                                      ProjectedRow.createTEvaluatableExpressions(pProjections),
                                      TInstance.createTInstances(pProjections));
        } else {
            newRow = new OverlayingRow(origRow, transform.rowType);
        }
        return newRow;
    }

    //
    // Classes
    //

    private interface RowHandler {
        void handleRow(Row row);
    }

    /**
     * Helper for saving concurrently handled rows.
     * Concrete implementations *must* be thread safe.
     */
    private interface HKeySaver
    {
        void save(SchemaManager sm, Session session, Key hKey);
    }

    /**
     * Helper for checking for concurrently handled rows.
     * Must *only* be called with increasing hKeys and thrown away when the transaction closes.
     */
    private interface HKeyChecker
    {
        boolean contains(SchemaManager sm, Session session, Key hKey);
    }

    private static class SchemaManagerSaver implements HKeySaver
    {
        private final int tableID;

        private SchemaManagerSaver(int tableID) {
            this.tableID = tableID;
        }

        @Override
        public void save(SchemaManager sm, Session session, Key hKey) {
            sm.addOnlineHandledHKey(session, tableID, hKey);
        }
    }

    private static class SchemaManagerChecker implements HKeyChecker
    {
        private final int tableID;
        private Iterator<byte[]> iter;
        private KeyState last;

        private SchemaManagerChecker(int tableID) {
            this.tableID = tableID;
        }

        private void advance() {
            byte[] bytes = iter.next();
            last = (bytes != null) ? new KeyState(bytes) : null;
        }

        @Override
        public boolean contains(SchemaManager sm, Session session, Key hKey) {
            if(iter == null) {
                iter = sm.getOnlineHandledHKeyIterator(session, tableID, hKey);
                advance();
            }
            // Can scan until we reach, or go past, hKey. If past, can't skip.
            while(last != null) {
                int ret = last.compareTo(hKey);
                if(ret == 0) {
                    return true;    // Match
                }
                if(ret > 0) {
                    return false;   // last from iterator is ahead of hKey
                }
                advance();
            }
            // Iterator exhausted: no more to skip
            return false;
        }
    }

    private static class FalseChecker implements HKeyChecker
    {
        @Override
        public boolean contains(SchemaManager sm, Session session, Key hKey) {
            return false;
        }
    }

    /** Holds information about how to maintain/populate the new/modified instance of a table. */
    private static class TableTransform {
        public final ChangeLevel changeLevel;
        /** Target for concurrently handled DML. */
        public final HKeySaver hKeySaver;
        /** New row type for the table. */
        public final TableRowType rowType;
        /** Not {@code null} *iff* new rows need projected. */
        public final ProjectedTableRowType projectedRowType;
        /** Not {@code null} *iff* new rows need only be verified. */
        public final boolean checkConstraints;
        /** Contains table indexes to build (can be empty) */
        public final TableIndex[] tableIndexes;
        /** Populated with group indexes to build (can be empty) */
        public final Collection<GroupIndex> groupIndexes;
        /** Used for CreateTableAs */
        public Operator deleteOperator;
        public Operator insertOperator;


        public TableTransform(ChangeLevel changeLevel,
                              HKeySaver hKeySaver,
                              TableRowType rowType,
                              ProjectedTableRowType projectedRowType,
                              boolean checkConstraints,
                              TableIndex[] tableIndexes,
                              Collection<GroupIndex> groupIndexes,
                              Operator deleteOperator ,
                              Operator insertOperator) {
            this.changeLevel = changeLevel;
            this.hKeySaver = hKeySaver;
            this.rowType = rowType;
            this.projectedRowType = projectedRowType;
            this.checkConstraints = checkConstraints;
            this.tableIndexes = tableIndexes;
            this.groupIndexes = groupIndexes;
            this.deleteOperator = deleteOperator;
            this.insertOperator = insertOperator;
        }
    }

    /** Table ID -> TableTransform */
    private static class TransformCache extends HashMap<Integer,TableTransform>
    {
    }
}
