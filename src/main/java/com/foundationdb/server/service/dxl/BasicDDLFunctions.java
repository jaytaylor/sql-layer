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

package com.foundationdb.server.service.dxl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.LinkedList;
import java.util.Deque;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.ColumnName;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Index.IndexType;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.protobuf.ProtobufWriter.TableSelector;
import com.foundationdb.ais.util.ChangedTableDescription;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.ais.util.TableChangeValidatorState;
import com.foundationdb.ais.util.TableChangeValidator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.api.dml.scan.Cursor;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.ScanRequest;
import com.foundationdb.server.error.AlterMadeNoChangeException;
import com.foundationdb.server.error.ConcurrentViolationException;
import com.foundationdb.server.error.DropSequenceNotAllowedException;
import com.foundationdb.server.error.ForeignConstraintDDLException;
import com.foundationdb.server.error.ForeignKeyPreventsDropTableException;
import com.foundationdb.server.error.NoSuchGroupException;
import com.foundationdb.server.error.NoSuchIndexException;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.NoSuchTableIdException;
import com.foundationdb.server.error.NotAllowedByConfigException;
import com.foundationdb.server.error.ProtectedIndexException;
import com.foundationdb.server.error.RowDefNotFoundException;
import com.foundationdb.server.error.UnsupportedDropException;
import com.foundationdb.server.error.ViewReferencesExist;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.error.UnsupportedCreateSelectException;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.ChangeSetHelper;
import com.foundationdb.server.store.OnlineHelper;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.TableChanges;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.service.TypesRegistry;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.compiler.BooleanNormalizer;
import com.foundationdb.sql.optimizer.AISBinder;
import com.foundationdb.sql.optimizer.CreateAsCompiler;
import com.foundationdb.sql.optimizer.SubqueryFlattener;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerSession;
import com.google.common.collect.HashMultimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;

public class BasicDDLFunctions extends ClientAPIBase implements DDLFunctions {
    private final static Logger logger = LoggerFactory.getLogger(BasicDDLFunctions.class);

    private final static String FEATURE_SPATIAL_INDEX_PROP = "fdbsql.feature.spatial_index_on";

    private final IndexStatisticsService indexStatisticsService;
    private final TransactionService txnService;
    private final ListenerService listenerService;
    private final boolean withSpatialIndexes;
    private OnlineDDLMonitor onlineDDLMonitor;

    @Override
    public void createTable(Session session, Table table)
    {
        TableName tableName = schemaManager().createTableDefinition(session, table);
        Table newTable = getAIS(session).getTable(tableName);
        checkCursorsForDDLModification(session, newTable);
        for(TableListener listener : listenerService.getTableListeners()) {
            listener.onCreate(session, newTable);
        }
    }

    private List<TableName> getTableNames(Session session, ServerSession server, String queryExpression, Table table ){

        AkibanInformationSchema ais = schemaManager().getAis(session);
        SQLParser parser = server.getParser();
        StatementNode stmt;
        try {
            stmt = parser.parseStatement(queryExpression);
        } catch (StandardException e) {
            throw new SQLParserInternalException(e);
        }
        StoreAdapter adapter = store().createAdapter(session, SchemaCache.globalSchema(ais));
        CreateAsCompiler compiler = new CreateAsCompiler(server, adapter, false, ais);
        PlanContext plan = new PlanContext(compiler);
        ASTStatementLoader astStatementLoader = new ASTStatementLoader();
        AISBinder binder = new AISBinder(ais, table.getName().getSchemaName());
        try {
            binder.bind(stmt);
            BooleanNormalizer booleanNormalizer = new BooleanNormalizer(parser);
            stmt = booleanNormalizer.normalize(stmt);
            SubqueryFlattener subqueryFlattener = new SubqueryFlattener(parser);
            stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        } catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
        plan.setPlan(new AST((DMLStatementNode)stmt, null));
        astStatementLoader.apply(plan);

        List<TableName> tableNames = new ArrayList<>();
        Deque<PlanNode> nodeQueue = new LinkedList<>();
        nodeQueue.add(plan.getPlan());
        while(!nodeQueue.isEmpty()){
            PlanNode node = nodeQueue.poll();
            if(node instanceof BasePlanWithInput){
                nodeQueue.add(((BasePlanWithInput)node).getInput());
            }
            if(node instanceof TableSource) {
                tableNames.add(((TableSource) node).getTable().getTable().getName());
            }else if(node instanceof Select && !((Select)node).getConditions().isEmpty()) {
                ConditionList conditionList = ((Select) node).getConditions();
                for (ConditionExpression conditionExpression : conditionList.subList(0, conditionList.size())) {
                    nodeQueue.add(((AnyCondition) conditionExpression).getSubquery());
                }
            }else if( node instanceof JoinNode) {
                nodeQueue.add(((JoinNode)node).getLeft());
                nodeQueue.add(((JoinNode)node).getRight());
            }
        }
        return tableNames;
    }

    @Override
    public void createTable(final Session session, final Table table,
                            final String queryExpression, QueryContext context,
                            final ServerSession server){
        if(queryExpression == null || queryExpression.isEmpty()){
            createTable(session, table);
            return;
        }
        logger.debug("creating table {}", table);
        txnService.commitTransaction(session);
        try {
            onlineAt(OnlineDDLMonitor.Stage.PRE_METADATA);
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    schemaManager().startOnline(session);
                    TableName tableName = schemaManager().createTableDefinition(session, table);
                    AkibanInformationSchema onlineAIS = schemaManager().getOnlineAIS(session);
                    int onlineTableID = onlineAIS.getTable(table.getName()).getTableId();
                    List<TableName> tableNames = getTableNames(session, server, queryExpression, table);
                    if(tableNames.size() > 1)
                        throw new UnsupportedCreateSelectException();
                    for( TableName name : tableNames){
                        ChangeSet fromChangeSet = buildChangeSet(onlineAIS.getTable(name), queryExpression,  onlineTableID);
                        schemaManager().addOnlineChangeSet(session, fromChangeSet);
                    }
                    ChangeSet toChangeSet = buildChangeSet(onlineAIS.getTable(tableName), queryExpression, onlineTableID);
                    schemaManager().addOnlineChangeSet(session, toChangeSet);

                }
            });
            onlineAt(OnlineDDLMonitor.Stage.POST_METADATA);

            final boolean[] success = {false};
            try {
                onlineAt(OnlineDDLMonitor.Stage.PRE_TRANSFORM);
                store().getOnlineHelper().createAsSelect(session, context, server, queryExpression, table.getName());
                onlineAt(OnlineDDLMonitor.Stage.POST_TRANSFORM);

                txnService.run(session, new Runnable() {
                    @Override
                    public void run() {
                        AkibanInformationSchema onlineAIS = schemaManager().getOnlineAIS(session);
                        final Table onlineTable = onlineAIS.getTable(table.getName());
                        for (TableListener listener : listenerService.getTableListeners()) {
                            listener.onCreate(session, onlineTable);
                        }
                    }
                });
                success[0] = true;
            } finally {
                onlineAt(OnlineDDLMonitor.Stage.PRE_FINAL);
                txnService.run(session, new Runnable() {
                    @Override
                    public void run() {
                        if (success[0]) {
                            finishOnlineChange(session);
                        } else {
                            discardOnlineChange(session);
                        }
                    }
                });
                onlineAt(OnlineDDLMonitor.Stage.POST_FINAL);
            }
        }finally {
            txnService.beginTransaction(session);
        }
    }

    @Override
    public void renameTable(Session session, TableName currentName, TableName newName)
    {
        schemaManager().renameTable(session, currentName, newName);
        checkCursorsForDDLModification(session, getAIS(session).getTable(newName));
    }

    @Override
    public void dropTable(Session session, TableName tableName)
    {
        txnService.beginTransaction(session);
        try {
            dropTableInternal(session, tableName);
            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    private void dropTableInternal(Session session, TableName tableName) {
        logger.trace("dropping table {}", tableName);

        Table table = getAIS(session).getTable(tableName);
        if(table == null) {
            return;
        }

        // May only drop leaf tables through DDL interface
        if(!table.getChildJoins().isEmpty()) {
            throw new UnsupportedDropException(table.getName());
        }

        DMLFunctions dml = new BasicDMLFunctions(middleman(), schemaManager(), store(), this,
                                                 indexStatisticsService, listenerService);
        if(table.isRoot()) {
            // Root table and no child tables, can delete all associated trees
            store().removeTrees(session, table);
        } else {
            dml.truncateTable(session, table.getTableId(), false);
            store().deleteIndexes(session, table.getIndexesIncludingInternal());
            store().deleteIndexes(session, table.getGroupIndexes());

            if (table.getIdentityColumn() != null) {
                Collection<Sequence> sequences = Collections.singleton(table.getIdentityColumn().getIdentityGenerator());
                store().deleteSequences(session, sequences);
            }
        }
        for(TableListener listener : listenerService.getTableListeners()) {
            listener.onDrop(session, table);
        }
        schemaManager().dropTableDefinition(session, tableName.getSchemaName(), tableName.getTableName(),
                                            SchemaManager.DropBehavior.RESTRICT);
        checkCursorsForDDLModification(session, table);
    }

    @Override
    public ChangeLevel alterTable(final Session session,
                                  final TableName tableName,
                                  final Table newDefinition,
                                  final List<TableChange> columnChanges,
                                  final List<TableChange> tableIndexChanges,
                                  final QueryContext context)
    {
        onlineAt(OnlineDDLMonitor.Stage.PRE_METADATA);
        final AISValidatorPair pair = txnService.run(session, new Callable<AISValidatorPair>() {
            @Override
            public AISValidatorPair call() {
                AkibanInformationSchema origAIS = getAIS(session);
                Table origTable = origAIS.getTable(tableName);
                schemaManager().startOnline(session);
                TableChangeValidator validator = alterTableDefinitions(
                    session, origTable, newDefinition, columnChanges, tableIndexChanges
                );
                List<ChangeSet> changeSets = buildChangeSets(
                    origAIS,
                    schemaManager().getOnlineAIS(session),
                    origTable.getTableId(),
                    validator
                );
                for(ChangeSet cs : changeSets) {
                    schemaManager().addOnlineChangeSet(session, cs);
                }
                return new AISValidatorPair(origAIS, validator);
            }
        });
        onlineAt(OnlineDDLMonitor.Stage.POST_METADATA);

        final String errorMsg;
        final boolean[] success = { false };
        try {
            onlineAt(OnlineDDLMonitor.Stage.PRE_TRANSFORM);
            alterTablePerform(session, tableName, pair.validator.getFinalChangeLevel(), context);
            onlineAt(OnlineDDLMonitor.Stage.POST_TRANSFORM);
            success[0] = true;
        } finally {
            onlineAt(OnlineDDLMonitor.Stage.PRE_FINAL);
            errorMsg = txnService.run(session, new Callable<String>() {
                @Override
                public String call() {
                    String error = schemaManager().getOnlineDMLError(session);
                    if(success[0] && (error == null)) {
                        finishOnlineChange(session);
                    } else {
                        discardOnlineChange(session);
                    }
                    return error;
                }
            });
            onlineAt(OnlineDDLMonitor.Stage.POST_FINAL);
        }
        if(errorMsg != null) {
            throw new ConcurrentViolationException(errorMsg);
        }

        // Clear old storage after it is completely unused
        txnService.run(session, new Runnable() {
            @Override
            public void run() {
                Table origTable = pair.ais.getTable(tableName);
                Table newTable = getTable(session, origTable.getTableId());
                alterTableRemoveOldStorage(session, origTable, newTable, pair.validator);
            }
        });

        return pair.validator.getFinalChangeLevel();
    }

    @Override
    public void alterSequence(Session session, TableName sequenceName, Sequence newDefinition)
    {
        logger.trace("altering sequence {}", sequenceName);
        AkibanInformationSchema ais = getAIS(session);
        Sequence oldSeq = ais.getSequence(sequenceName);
        if(oldSeq == null) {
            throw new NoSuchSequenceException(sequenceName);
        }
        schemaManager().alterSequence(session, sequenceName, newDefinition);
        // Remove old storage
        store().deleteSequences(session, Collections.singleton(oldSeq));
    }

    @Override
    public void dropSchema(Session session, String schemaName)
    {
        logger.trace("dropping schema {}", schemaName);
        txnService.beginTransaction(session);
        try {
            dropSchemaInternal(session, schemaName);
            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    private void dropSchemaInternal(Session session, String schemaName) {
        final com.foundationdb.ais.model.Schema schema = getAIS(session).getSchema(schemaName);
        if (schema == null)
            return; // NOT throw new NoSuchSchemaException(schemaName); adapter does it.

        List<View> viewsToDrop = new ArrayList<>();
        Set<View> seen = new HashSet<>();
        for (View view : schema.getViews().values()) {
            addView(view, viewsToDrop, seen, schema, schemaName);
        }

        // Find all groups and tables in the schema
        Set<Group> groupsToDrop = new HashSet<>();
        List<Table> tablesToDrop = new ArrayList<>();

        for(Table table : schema.getTables().values()) {
            groupsToDrop.add(table.getGroup());
            // Cannot drop entire group if parent is not in the same schema
            final Join parentJoin = table.getParentJoin();
            if(parentJoin != null) {
                final Table parentTable = parentJoin.getParent();
                if(!parentTable.getName().getSchemaName().equals(schemaName)) {
                    tablesToDrop.add(table);
                }
            }
            // All children must be in the same schema
            for(Join childJoin : table.getChildJoins()) {
                final TableName childName = childJoin.getChild().getName();
                if(!childName.getSchemaName().equals(schemaName)) {
                    throw new ForeignConstraintDDLException(table.getName(), childName);
                }
            }
            // All referencing foreign keys must be in the same schema
            for(ForeignKey foreignKey : table.getReferencedForeignKeys()) {
                final TableName referencingName = foreignKey.getReferencingTable().getName();
                if(!referencingName.getSchemaName().equals(schemaName)) {
                    throw new ForeignKeyPreventsDropTableException(table.getName(), foreignKey.getConstraintName().getTableName(), referencingName);
                }
            }
        }
        Set<TableName> sequencesToDrop = new TreeSet<>();
        for (Sequence sequence : schema.getSequences().values()) {
            // Drop the sequences in this schema, but not the 
            // generator sequences, which will be dropped with the table.
            if(!isIdentitySequence(schema.getTables().values(), sequence)) {
                sequencesToDrop.add(sequence.getSequenceName());
            }
        }
        // Remove groups that contain tables in multiple schemas
        for(Table table : tablesToDrop) {
            groupsToDrop.remove(table.getGroup());
        }
        // Sort table IDs so higher (i.e. children) are first
        Collections.sort(tablesToDrop, new Comparator<Table>() {
            @Override
            public int compare(Table o1, Table o2) {

                return o2.getTableId().compareTo(o1.getTableId());
            }
        });
        List<Routine> routinesToDrop = new ArrayList<>(schema.getRoutines().values());
        List<SQLJJar> jarsToDrop = new ArrayList<>();
        for (SQLJJar jar : schema.getSQLJJars().values()) {
            boolean anyOutside = false;
            for (Routine routine : jar.getRoutines()) {
                if (!routine.getName().getSchemaName().equals(schemaName)) {
                    anyOutside = true;
                    break;
                }
            }
            if (!anyOutside)
                jarsToDrop.add(jar);
        }
        // TODO not CASCADE
        schemaManager().dropSchema(session, schemaName, SchemaManager.DropBehavior.CASCADE, sequencesToDrop);
        store().dropSchema(session, schema);
//        // Do the actual dropping
//        for(View view : viewsToDrop) {
//            dropView(session, view.getName());
//        }
//        for(Table table : tablesToDrop) {
//            dropTableInternal(session, table.getName());
//        }
//        for(Group group : groupsToDrop) {
//            dropGroupInternal(session, group.getName());
//        }
//        for (Sequence sequence : sequencesToDrop) {
//            dropSequence(session, sequence.getSequenceName());
//        }
//        for (Routine routine : routinesToDrop) {
//            dropRoutine(session, routine.getName());
//        }
//        for (SQLJJar jar : jarsToDrop) {
//            dropSQLJJar(session, jar.getName());
//        }
    }

    private void addView(View view, Collection<View> into, Collection<View> seen, 
                         com.foundationdb.ais.model.Schema schema, String schemaName) {
        if (seen.add(view)) {
            for (TableName reference : view.getTableReferences()) {
                if (!reference.getSchemaName().equals(schemaName)) {
                    throw new ViewReferencesExist(schemaName, 
                                                  view.getName().getTableName(),
                                                  reference.getSchemaName(),
                                                  reference.getTableName());
                }
                // If reference is to another view, it must come first.
                View refView = schema.getView(reference.getTableName());
                if (refView != null) {
                    addView(view, into, seen, schema, schemaName);
                }
            }
            into.add(view);
        }
    }

    @Override
    public void dropGroup(Session session, TableName groupName)
    {
        logger.trace("dropping group {}", groupName);
        txnService.beginTransaction(session);
        try {
            dropGroupInternal(session, groupName);
            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    private void dropGroupInternal(final Session session, TableName groupName) {
        final Group group = getAIS(session).getGroup(groupName);
        if(group == null) {
            return;
        }
        store().dropGroup(session, group);
        group.visit(new AbstractVisitor() {
            @Override
            public void visit(Table table) {
                for(TableListener listener : listenerService.getTableListeners()) {
                    listener.onDrop(session, table);
                }
            }
        });
        Table root = group.getRoot();
        schemaManager().dropTableDefinition(session, root.getName().getSchemaName(), root.getName().getTableName(),
                                            SchemaManager.DropBehavior.CASCADE);
        checkCursorsForDDLModification(session, root);
    }

    @Override
    public AkibanInformationSchema getAIS(final Session session) {
        logger.trace("getting AIS");
        return schemaManager().getAis(session);
    }

    @Override
    public TypesRegistry getTypesRegistry() {
        return schemaManager().getTypesRegistry();
    }

    @Override
    public TypesTranslator getTypesTranslator() {
        return schemaManager().getTypesTranslator();
    }

    @Override
    public StorageFormatRegistry getStorageFormatRegistry() {
        return schemaManager().getStorageFormatRegistry();
    }

    @Override
    public AISCloner getAISCloner() {
        return schemaManager().getAISCloner();
    }

    @Override
    public int getTableId(Session session, TableName tableName) throws NoSuchTableException {
        logger.trace("getting table ID for {}", tableName);
        Table table = getAIS(session).getTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table.getTableId();
    }

    @Override
    public Table getTable(Session session, int tableId) throws NoSuchTableIdException {
        logger.trace("getting AIS Table for {}", tableId);
        Table table = getAIS(session).getTable(tableId);
        if(table == null) {
            throw new NoSuchTableIdException(tableId);
        }
        return table;
    }

    @Override
    public Table getTable(Session session, TableName tableName) throws NoSuchTableException {
        logger.trace("getting AIS Table for {}", tableName);
        AkibanInformationSchema ais = getAIS(session);
        Table table = ais.getTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table;
    }

    @Override
    public TableName getTableName(Session session, int tableId) throws NoSuchTableException {
        logger.trace("getting table name for {}", tableId);
        return getTable(session, tableId).getName();
    }

    @Override
    public RowDef getRowDef(Session session, int tableId) throws RowDefNotFoundException {
        logger.trace("getting RowDef for {}", tableId);
        return getAIS(session).getTable(tableId).rowDef();
    }

    @Override
    public int getGenerationAsInt(Session session) {
        long full = getGeneration(session);
        return (int)full ^ (int)(full >>> 32);
    }

    @Override
    public long getGeneration(Session session) {
        return getAIS(session).getGeneration();
    }

    @Override
    public long getOldestActiveGeneration() {
        return schemaManager().getOldestActiveAISGeneration();
    }

    @Override
    public Set<Long> getActiveGenerations() {
        return schemaManager().getActiveAISGenerations();
    }

    @Override
    public void createIndexes(final Session session, final Collection<? extends Index> indexesToAdd) {
        logger.debug("creating indexes {}", indexesToAdd);
        if (indexesToAdd.isEmpty()) {
            return;
        }

        if(!withSpatialIndexes) {
            for(Index index : indexesToAdd) {
                if(index.isSpatial()) {
                    throw new NotAllowedByConfigException("spatial index");
                }
            }
        }

        onlineAt(OnlineDDLMonitor.Stage.PRE_METADATA);
        txnService.run(session, new Runnable() {
            @Override
            public void run() {
                schemaManager().startOnline(session);
                schemaManager().createIndexes(session, indexesToAdd, false);
                AkibanInformationSchema onlineAIS = schemaManager().getOnlineAIS(session);
                List<ChangeSet> changeSets = buildChangeSets(onlineAIS, indexesToAdd);
                for(ChangeSet cs : changeSets) {
                    schemaManager().addOnlineChangeSet(session, cs);
                }
            }
        });
        onlineAt(OnlineDDLMonitor.Stage.POST_METADATA);

        final String errorMsg;
        final boolean[] success = { false };
        try {
            onlineAt(OnlineDDLMonitor.Stage.PRE_TRANSFORM);
            store().getOnlineHelper().buildIndexes(session, null);
            onlineAt(OnlineDDLMonitor.Stage.POST_TRANSFORM);

            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    Collection<ChangeSet> changeSets = schemaManager().getOnlineChangeSets(session);
                    AkibanInformationSchema onlineAIS = schemaManager().getOnlineAIS(session);
                    Collection<Index> newIndexes = OnlineHelper.findIndexesToBuild(changeSets, onlineAIS);
                    for(Index index : newIndexes) {
                        checkCursorsForDDLModification(session, index.leafMostTable());
                    }
                    for(TableListener listener : listenerService.getTableListeners()) {
                        listener.onCreateIndex(session, newIndexes);
                    }
                }
            });
            success[0] = true;
        } finally {
            onlineAt(OnlineDDLMonitor.Stage.PRE_FINAL);
            errorMsg = txnService.run(session, new Callable<String>() {
                @Override
                public String call() {
                    String error = schemaManager().getOnlineDMLError(session);
                    if(success[0] && (error == null)) {
                        finishOnlineChange(session);
                    } else {
                        discardOnlineChange(session);
                    }
                    return error;
                }
            });
            onlineAt(OnlineDDLMonitor.Stage.POST_FINAL);
        }
        if(errorMsg != null) {
            throw new ConcurrentViolationException(errorMsg);
        }
    }

    @Override
    public void dropTableIndexes(Session session, TableName tableName, Collection<String> indexNamesToDrop)
    {
        logger.trace("dropping table indexes {} {}", tableName, indexNamesToDrop);
        if(indexNamesToDrop.isEmpty()) {
            return;
        }

        txnService.beginTransaction(session);
        try {
            final Table table = getTable(session, tableName);
            Collection<Index> tableIndexes = new HashSet<>();
            Collection<Index> allIndexes = tableIndexes;
            for(String indexName : indexNamesToDrop) {
                Index index = table.getIndex(indexName);
                if(index != null) {
                    tableIndexes.add(index);
                }
                else if ((index = table.getFullTextIndex(indexName)) != null) {
                    if (allIndexes == tableIndexes) {
                        allIndexes = new HashSet<>(allIndexes);
                    }
                }
                else {
                    throw new NoSuchIndexException(indexName);
                }
                // no primary key nor connected to a FK
                if(index.isPrimaryKey() || index.isConnectedToFK()) {
                    throw new ProtectedIndexException(indexName, table.getName());
                }
                if (allIndexes != tableIndexes) {
                    allIndexes.add(index);
                }
            }
            schemaManager().dropIndexes(session, allIndexes);
            store().deleteIndexes(session, tableIndexes);
            for(TableListener listener : listenerService.getTableListeners()) {
                listener.onDropIndex(session, allIndexes);
            }
            checkCursorsForDDLModification(session, table);
            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    @Override
    public void dropGroupIndexes(Session session, TableName groupName, Collection<String> indexNamesToDrop) {
        logger.trace("dropping group indexes {} {}", groupName, indexNamesToDrop);
        if(indexNamesToDrop.isEmpty()) {
            return;
        }
        txnService.beginTransaction(session);
        try {
            final Group group = getAIS(session).getGroup(groupName);
            if (group == null) {
                throw new NoSuchGroupException(groupName);
            }
            Collection<Index> indexes = new HashSet<>();
            for(String indexName : indexNamesToDrop) {
                final Index index = group.getIndex(indexName);
                if(index == null) {
                    throw new NoSuchIndexException(indexName);
                }
                indexes.add(index);
            }
            schemaManager().dropIndexes(session, indexes);
            store().deleteIndexes(session, indexes);
            for(TableListener listener : listenerService.getTableListeners()) {
                listener.onDropIndex(session, indexes);
            }
            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    @Override
    public void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate) {
        final Table table = getTable(session, tableName);
        Collection<Index> indexes = new HashSet<>();
        if (indexesToUpdate == null) {
            indexes.addAll(table.getIndexes());
            for (Index index : table.getGroup().getIndexes()) {
                if (table == index.leafMostTable())
                    indexes.add(index);
            }
        }
        else {
            for (String indexName : indexesToUpdate) {
                Index index = table.getIndex(indexName);
                if (index == null) {
                    index = table.getGroup().getIndex(indexName);
                    if (index == null)
                        throw new NoSuchIndexException(indexName);
                }
                indexes.add(index);
            }
        }
        indexStatisticsService.updateIndexStatistics(session, indexes);
    }

    @Override
    public void createView(Session session, View view)
    {
        schemaManager().createView(session, view);
    }

    @Override
    public void dropView(Session session, TableName viewName)
    {
        schemaManager().dropView(session, viewName);
    }

    @Override
    public void createRoutine(Session session, Routine routine, boolean replaceExisting)
    {
        schemaManager().createRoutine(session, routine, replaceExisting);
    }

    @Override
    public void dropRoutine(Session session, TableName routineName)
    {
        schemaManager().dropRoutine(session, routineName);
    }

    @Override
    public void createSQLJJar(Session session, SQLJJar sqljJar) {
        schemaManager().createSQLJJar(session, sqljJar);
    }
    
    @Override
    public void replaceSQLJJar(Session session, SQLJJar sqljJar) {
        schemaManager().replaceSQLJJar(session, sqljJar);
    }
    
    @Override
    public void dropSQLJJar(Session session, TableName jarName) {
        schemaManager().dropSQLJJar(session, jarName);
    }

    @Override
    public synchronized void setOnlineDDLMonitor(OnlineDDLMonitor onlineDDLMonitor) {
        assert (this.onlineDDLMonitor == null || onlineDDLMonitor == null);
        this.onlineDDLMonitor = onlineDDLMonitor;
    }

    private void checkCursorsForDDLModification(Session session, Table table) {
        Map<CursorId,BasicDXLMiddleman.ScanData> cursorsMap = getScanDataMap(session);
        if (cursorsMap == null) {
            return;
        }

        final int tableId = table.getTableId();
        for (BasicDXLMiddleman.ScanData scanData : cursorsMap.values()) {
            Cursor cursor = scanData.getCursor();
            if (cursor.isClosed()) {
                continue;
            }
            ScanRequest request = cursor.getScanRequest();
            int scanTableId = request.getTableId();
            if (scanTableId == tableId) {
                cursor.setDDLModified();
            }
        }
    }

    public void createSequence(Session session, Sequence sequence) {
        schemaManager().createSequence(session, sequence);
    }
   
    public void dropSequence(Session session, TableName sequenceName) {
        final Sequence sequence = getAIS(session).getSequence(sequenceName);
        
        if (sequence == null) {
            throw new NoSuchSequenceException (sequenceName);
        }

        for (Table table : getAIS(session).getTables().values()) {
            if (table.getIdentityColumn() != null && table.getIdentityColumn().getIdentityGenerator().equals(sequence)) {
                throw new DropSequenceNotAllowedException(sequence.getSequenceName().getTableName(), table.getName());
            }
        }
        store().deleteSequences(session, Collections.singleton(sequence));
        schemaManager().dropSequence(session, sequence);
    }


    //
    // Internal
    //

    BasicDDLFunctions(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store,
                      IndexStatisticsService indexStatisticsService, TypesRegistryService typesRegistry,
                      TransactionService txnService, ListenerService listenerService,
                      ConfigurationService configService) {
        super(middleman, schemaManager, store);
        this.indexStatisticsService = indexStatisticsService;
        this.txnService = txnService;
        this.listenerService = listenerService;
        this.withSpatialIndexes = Boolean.parseBoolean(configService.getProperty(FEATURE_SPATIAL_INDEX_PROP));
    }

    private TableChangeValidator alterTableDefinitions(Session session,
                                                       Table origTable,
                                                       Table newDefinition,
                                                       List<TableChange> columnChanges,
                                                       List<TableChange> tableIndexChanges)
    {
        // Any GroupIndex changes are entirely derived, ignore any that happen to be passed.
        if(newDefinition.getGroup() != null) {
            newDefinition.getGroup().removeIndexes(newDefinition.getGroup().getIndexes());
        }
        TableChangeValidator v = new TableChangeValidator(origTable, newDefinition, columnChanges, tableIndexChanges);
        v.compare();
        TableChangeValidatorState changeState = v.getState();
        dropGroupIndexDefinitions(session, origTable, changeState.droppedGI);
        Table newTable = schemaManager().getOnlineAIS(session).getTable(origTable.getTableId());
        dropGroupIndexDefinitions(session, newTable, changeState.affectedGI.keySet());
        schemaManager().alterTableDefinitions(session, changeState.descriptions);
        newTable = schemaManager().getOnlineAIS(session).getTable(origTable.getTableId());
        recreateGroupIndexes(session, changeState, origTable, newTable);
        return v;
    }

    private void alterTablePerform(Session session, TableName tableName, ChangeLevel level, QueryContext context) {
        switch(level) {
            case NONE:
                AlterMadeNoChangeException e = new AlterMadeNoChangeException(tableName);
                logger.warn(e.getMessage());
                if(context != null) {
                    context.warnClient(e);
                }
            break;
            case METADATA:
                // None
            break;
            case METADATA_CONSTRAINT:
                store().getOnlineHelper().checkTableConstraints(session, context);
            break;
            case INDEX:
            case INDEX_CONSTRAINT:
                store().getOnlineHelper().buildIndexes(session, context);
            break;
            case TABLE:
            case GROUP:
                store().getOnlineHelper().alterTable(session, context);
            break;
            default:
                throw new IllegalStateException(level.toString());
        }
    }

    private void discardOnlineChange(Session session) {
        Collection<ChangeSet> changeSets = schemaManager().getOnlineChangeSets(session);
        store().discardOnlineChange(session, changeSets);
        schemaManager().discardOnline(session);
    }

    private void finishOnlineChange(Session session) {
        Collection<ChangeSet> changeSets = schemaManager().getOnlineChangeSets(session);
        schemaManager().finishOnline(session);
        store().finishOnlineChange(session, changeSets);
    }

    private void alterTableRemoveOldStorage(Session session,
                                            Table origTable,
                                            Table newTable,
                                            TableChangeValidator validator)
    {
        AkibanInformationSchema origAIS = origTable.getAIS();
        TableChangeValidatorState changeState = validator.getState();
        ChangeLevel changeLevel = validator.getFinalChangeLevel();

        List<Sequence> sequencesToDrop = new ArrayList<>();
        for(ChangedTableDescription desc : changeState.descriptions) {
            for(TableName name : desc.getDroppedSequences()) {
                sequencesToDrop.add(origAIS.getSequence(name));
            }
        }

        List<Index> indexesToDrop = new ArrayList<>();
        Group group = origTable.getGroup();
        for(String name : changeState.droppedGI) {
            indexesToDrop.add(group.getIndex(name));
        }

        List<HasStorage> storageToRemove = new ArrayList<>();
        for(TableChange ic : validator.getState().tableIndexChanges) {
            switch(ic.getChangeType()) {
                case MODIFY:
                case DROP:
                    Index index = origTable.getIndexIncludingInternal(ic.getOldName());
                    storageToRemove.add(index);
                break;
            }
        }

        // Old group storage
        if(changeLevel == ChangeLevel.TABLE || changeLevel == ChangeLevel.GROUP) {
            storageToRemove.add(origTable.getGroup());
        }

        // New parent's old group storage
        // but only if the storage location is different - metadata changes may not move the group data
        Table newParent = newTable.getParentTable();
        if(newParent != null) {
            Table newParentOldTable = origAIS.getTable(newParent.getTableId());
            if (!newParent.getGroup().getStorageDescription().getUniqueKey().equals(
                    newParentOldTable.getGroup().getStorageDescription().getUniqueKey()))
            {
                storageToRemove.add(newParentOldTable.getGroup());
            }
        }

        store().deleteIndexes(session, indexesToDrop);
        store().deleteSequences(session, sequencesToDrop);
        store().removeTrees(session, storageToRemove);
    }

    private void dropGroupIndexDefinitions(Session session, Table table, Collection<String> names) {
        if(names.isEmpty()) {
            return;
        }
        List<GroupIndex> groupIndexes = new ArrayList<>();
        for(String n : names) {
            GroupIndex index = table.getGroup().getIndex(n);
            assert (index != null) : n;
            groupIndexes.add(index);
        }
        schemaManager().dropIndexes(session, groupIndexes);
    }

    private void recreateGroupIndexes(Session session,
                                      TableChangeValidatorState changeState,
                                      Table origTable,
                                      final Table newTable) {
        if(changeState.affectedGI.isEmpty()) {
            return;
        }

        AkibanInformationSchema tempAIS = getAISCloner().clone(
            newTable.getAIS(),
            new TableSelector() {
                @Override
                public boolean isSelected(Columnar columnar) {
                    if(columnar.isTable()) {
                        if(((Table)columnar).getGroup() == newTable.getGroup()) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public boolean isSelected(ForeignKey foreignKey) {
                    return false;
                }
            }
        );

        List<Index> indexesToBuild = new ArrayList<>();
        Group origGroup = origTable.getGroup();
        Group tempGroup = tempAIS.getGroup(newTable.getGroup().getName());
        for(Entry<String, List<ColumnName>> entry : changeState.affectedGI.entrySet()) {
            GroupIndex origIndex = origGroup.getIndex(entry.getKey());
            GroupIndex tempIndex = GroupIndex.create(tempAIS, tempGroup, origIndex);
            int pos = 0;
            for(ColumnName cn : entry.getValue()) {
                Table tempTable = tempAIS.getTable(cn.getTableName());
                Column tempColumn = tempTable.getColumn(cn.getName());
                IndexColumn.create(tempIndex, tempColumn, pos++, true, null);
            }
            if(!changeState.dataAffectedGI.containsKey(entry.getKey())) {
                // TODO: Maybe need a way to say copy without the tree name part?
                tempIndex.copyStorageDescription(origIndex);
            }
            indexesToBuild.add(tempIndex);
        }

        schemaManager().createIndexes(session, indexesToBuild, true);
    }


    //
    // Internal static
    //

    private static ChangedTableDescription findByID(List<ChangedTableDescription> descriptions, int tableID) {
        for(ChangedTableDescription d : descriptions) {
            if(d.getTableID() == tableID) {
                return d;
            }
        }
        return null;
    }

    /** Find indexes from the old table that 1) were not preserved and 2) are present in the new table. */
    private static Collection<TableIndex> findTableIndexesToBuild(ChangedTableDescription desc,
                                                                  Table oldTable,
                                                                  Table newTable) {
        List<TableIndex> indexes = new ArrayList<>();
        for(TableIndex index : oldTable.getIndexesIncludingInternal()) {
            String oldName = index.getIndexName().getName();
            String preserveName = desc.getPreserveIndexes().get(oldName);
            if(preserveName == null) {
                TableIndex newIndex = newTable.getIndexIncludingInternal(oldName);
                if(newIndex != null) {
                    indexes.add(newIndex);
                }
            }
        }
        return indexes;
    }


    /** ChangeSets for create table as */
       public static ChangeSet buildChangeSet(Table newTable, String sql, int toTableID) {
        ChangeSet.Builder builder = ChangeSet.newBuilder();
        builder.setChangeLevel(ChangeLevel.TABLE.name());
        assert(sql != null);
        builder.setSelectStatement(sql);
        builder.setTableId(newTable.getTableId());
        builder.setOldSchema(newTable.getName().getSchemaName());
        builder.setOldName(newTable.getName().getTableName());
        builder.setNewSchema(newTable.getName().getSchemaName());
        builder.setNewName(newTable.getName().getTableName());
        builder.setToTableId(toTableID);
        return builder.build();
    }

    /** ChangeSets for all tables affected by {@code newIndexes}. */
    private static List<ChangeSet> buildChangeSets(AkibanInformationSchema ais, Collection<? extends Index> stubIndexes) {
        HashMultimap<Integer,Index> tableToIndexes = HashMultimap.create();
        for(Index stub : stubIndexes) {
            // Re-look index up as external API previously only relied on names
            Table table = ais.getTable(stub.getIndexName().getFullTableName());
            String stubName = stub.getIndexName().getName();
            final Index index;
            switch(stub.getIndexType()) {
                case TABLE: index = table.getIndexIncludingInternal(stubName); break;
                case FULL_TEXT: index = table.getFullTextIndex(stubName); break;
                case GROUP: index = table.getGroup().getIndex(stubName); break;
                default:
                    throw new IllegalStateException(stub.getIndexType().toString());
            }
            assert (index != null) : stub;
            for(Integer tid : index.getAllTableIDs()) {
                tableToIndexes.put(tid, index);
            }
        }
        List<ChangeSet> changeSets = new ArrayList<>();
        for(Entry<Integer, Collection<Index>> entry : tableToIndexes.asMap().entrySet()) {
            Table table = ais.getTable(entry.getKey());
            ChangeSet.Builder builder = ChangeSet.newBuilder();
            builder.setChangeLevel(ChangeLevel.INDEX.name());
            builder.setTableId(table.getTableId());
            builder.setOldSchema(table.getName().getSchemaName());
            builder.setOldName(table.getName().getTableName());
            builder.setNewSchema(table.getName().getSchemaName());
            builder.setNewName(table.getName().getTableName());
            for(Index index : entry.getValue()) {
                TableChanges.IndexChange.Builder indexChange = TableChanges.IndexChange.newBuilder();
                indexChange.setChange(ChangeSetHelper.createAddChange(index.getIndexName().getName()));
                indexChange.setIndexType(index.getIndexType().name());
                builder.addIndexChange(indexChange);
            }
            changeSets.add(builder.build());
        }
        return changeSets;
    }

    /** ChangeSets for all tables affected, directly or indirectly, by {@code validator}. */
    private static List<ChangeSet> buildChangeSets(AkibanInformationSchema oldAIS,
                                                   AkibanInformationSchema newAIS,
                                                   int changedTableID,
                                                   TableChangeValidator validator) {
        Set<Integer> tableIDs = new HashSet<>();
        for(ChangedTableDescription desc : validator.getState().descriptions) {
            tableIDs.add(desc.getTableID());
        }

        // TABLE/GROUP change rebuilds entire group(s), so all are 'affected'
        if(validator.getFinalChangeLevel() == ChangeLevel.TABLE || validator.getFinalChangeLevel() == ChangeLevel.GROUP) {
            TableIDVisitor visitor = new TableIDVisitor(tableIDs);
            oldAIS.getTable(changedTableID).getGroup().visit(visitor);
            newAIS.getTable(changedTableID).getGroup().visit(visitor);
        }

        List<ChangeSet> changeSets = new ArrayList<>();
        for(Integer tid : tableIDs) {
            ChangeSet.Builder cs = ChangeSet.newBuilder();
            cs.setChangeLevel(validator.getFinalChangeLevel().name());
            cs.setTableId(tid);
            final TableName oldName, newName;
            ChangedTableDescription desc = findByID(validator.getState().descriptions, tid);
            if(desc == null) {
                Table table = newAIS.getTable(tid);
                oldName = newName = table.getName();
            } else {
                oldName = desc.getOldName();
                newName = desc.getNewName();
            }
            cs.setOldSchema(oldName.getSchemaName());
            cs.setOldName(oldName.getTableName());
            cs.setNewSchema(newName.getSchemaName());
            cs.setNewName(newName.getTableName());

            Table oldTable = oldAIS.getTable(tid);
            Table newTable = newAIS.getTable(tid);

            Set<String> handledIndexes = new HashSet<>();
            // Only the table being directly modified has explicit change list
            if(tid == changedTableID) {
                // Full column information needed to create projects for new row
                for(TableChange cc : validator.getState().columnChanges) {
                    cs.addColumnChange(ChangeSetHelper.createFromTableChange(cc));
                }
                for(TableChange ic : validator.getState().tableIndexChanges) {
                    TableChanges.IndexChange.Builder builder = TableChanges.IndexChange.newBuilder();
                    builder.setIndexType(IndexType.TABLE.name());
                    builder.setChange(ChangeSetHelper.createFromTableChange(ic));
                    cs.addIndexChange(builder);
                    if(ic.getNewName() != null) {
                        handledIndexes.add(ic.getNewName());
                    }
                }
            }

            Collection<TableIndex> additionalIndexes = Collections.emptyList();
            if(desc != null) {
                additionalIndexes = findTableIndexesToBuild(desc, oldTable, newTable);
            }

            for(TableIndex index : additionalIndexes) {
                if(!handledIndexes.contains(index.getIndexName().getName())) {
                    TableChanges.IndexChange.Builder builder = TableChanges.IndexChange.newBuilder();
                    builder.setIndexType(index.getIndexType().name());
                    String name = index.getIndexName().getName();
                    builder.setChange(ChangeSetHelper.createModifyChange(name, name));
                    cs.addIndexChange(builder);
                }
            }

            Group newGroup = newTable.getGroup();
            for(String indexName : validator.getState().dataAffectedGI.keySet()) {
                GroupIndex index = newGroup.getIndex(indexName);
                if(newTable.getGroupIndexes().contains(index)) {
                    TableChanges.IndexChange.Builder builder = TableChanges.IndexChange.newBuilder();
                    builder.setIndexType(index.getIndexType().name());
                    builder.setChange(ChangeSetHelper.createModifyChange(indexName, indexName));
                    cs.addIndexChange(builder);
                }
            }

            if(desc != null) {
                for(TableName seqName : desc.getDroppedSequences()) {
                    cs.addIdentityChange(ChangeSetHelper.createDropChange(seqName.getTableName()));
                }
                for(String identityCol : desc.getIdentityAdded()) {
                    Column c = newTable.getColumn(identityCol);
                    assert (c != null) && (c.getIdentityGenerator() != null) : c;
                    cs.addIdentityChange(ChangeSetHelper.createAddChange(c.getIdentityGenerator().getSequenceName().getTableName()));
                }
            }

            changeSets.add(cs.build());
        }
        return changeSets;
    }

    private synchronized void onlineAt(OnlineDDLMonitor.Stage stage) {
        if(onlineDDLMonitor != null) {
            onlineDDLMonitor.at(stage);
        }
    }

    private static boolean isIdentitySequence(Collection<Table> tables, Sequence s) {
        // Must search as there is no back-reference Sequence to owning Column.
        for(Table t : tables) {
            Column identityColumn = t.getIdentityColumn();
            if((identityColumn != null) && (identityColumn.getIdentityGenerator() == s)) {
                return true;
            }
        }
        return false;
    }

    //
    // Internal Classes
    //

    private static class TableIDVisitor extends AbstractVisitor {
        private final Collection<Integer> tableIDs;

        private TableIDVisitor(Collection<Integer> tableIDs) {
            this.tableIDs = tableIDs;
        }

        @Override
        public void visit(Table table) {
            tableIDs.add(table.getTableId());
        }
    }

    private static class AISValidatorPair {
        public final AkibanInformationSchema ais;
        public final TableChangeValidator validator;

        private AISValidatorPair(AkibanInformationSchema ais, TableChangeValidator validator) {
            this.ais = ais;
            this.validator = validator;
        }
    }
}
