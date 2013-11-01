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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.util.ChangedTableDescription;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.ais.util.TableChangeValidatorState;
import com.foundationdb.ais.util.TableChangeValidator;
import com.foundationdb.ais.util.TableChangeValidatorException;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.ProjectedRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.*;
import com.foundationdb.qp.rowtype.TableRowChecker;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.AlterMadeNoChangeException;
import com.foundationdb.server.error.InvalidAlterException;
import com.foundationdb.server.error.ViewReferencesExist;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.api.dml.scan.Cursor;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.ScanRequest;
import com.foundationdb.server.error.DropSequenceNotAllowedException;
import com.foundationdb.server.error.ForeignConstraintDDLException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchGroupException;
import com.foundationdb.server.error.NoSuchIndexException;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.NoSuchTableIdException;
import com.foundationdb.server.error.ProtectedIndexException;
import com.foundationdb.server.error.RowDefNotFoundException;
import com.foundationdb.server.error.UnsupportedDropException;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.expressions.TypesRegistryService;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.texpressions.TCastExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import static com.foundationdb.ais.util.TableChangeValidatorState.TableColumnNames;
import static com.foundationdb.qp.operator.API.filter_Default;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.util.Exceptions.throwAlways;

class BasicDDLFunctions extends ClientAPIBase implements DDLFunctions {
    private final static Logger logger = LoggerFactory.getLogger(BasicDDLFunctions.class);

    private final IndexStatisticsService indexStatisticsService;
    private final TypesRegistryService t3Registry;
    private final TransactionService txnService;
    private final ListenerService listenerService;


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

    private void alterTableMetadata(Session session,
                                    QueryContext context,
                                    Table origTable,
                                    Table newDefinition,
                                    TableChangeValidatorState changeState,
                                    boolean isNullChange) {
        if(isNullChange) {
            // Check new definition
            final ConstraintChecker checker = new TableRowChecker(newDefinition);

            // But scan old
            final AkibanInformationSchema origAIS = getAIS(session);
            final Schema oldSchema = SchemaCache.globalSchema(origAIS);
            final RowType oldSourceType = oldSchema.tableRowType(origTable);
            final StoreAdapter adapter = store().createAdapter(session, oldSchema);
            final QueryContext queryContext = new DelegatingContext(adapter, context);
            final QueryBindings queryBindings = queryContext.createBindings();

            Operator plan = filter_Default(
                    groupScan_Default(origTable.getGroup()),
                    Collections.singleton(oldSourceType)
            );
            com.foundationdb.qp.operator.Cursor cursor = API.cursor(plan, queryContext, queryBindings);

            cursor.openTopLevel();
            try {
                Row oldRow;
                while((oldRow = cursor.next()) != null) {
                    checker.checkConstraints(oldRow);
                }
            } finally {
                cursor.closeTopLevel();
            }
        }
        dropAffectedGroupIndexes(session, origTable, changeState);
        schemaManager().alterTableDefinitions(session, changeState.descriptions);
        createAffectedGroupIndexes(session, origTable, getTable(session, newDefinition.getName()), changeState, false);
    }

    private void alterTableIndex(Session session,
                                 Table origTable,
                                 Table newDefinition,
                                 TableChangeValidatorState changeState) {
        dropAffectedGroupIndexes(session, origTable, changeState);
        schemaManager().alterTableDefinitions(session, changeState.descriptions);
        Table newTable = getTable(session, newDefinition.getName());
        List<TableIndex> indexes = findNewIndexesToBuild(changeState.tableIndexChanges, newTable);
        store().buildIndexes(session, indexes);
        createAffectedGroupIndexes(session, origTable, getTable(session, newDefinition.getName()), changeState, false);
    }

    private static class RowTypeAndIndexes {
        final RowType rowType;
        final TableIndex[] indexes;

        private RowTypeAndIndexes(RowType rowType, TableIndex[] indexes) {
            this.rowType = rowType;
            this.indexes = indexes;
        }
    }

    private static void collectIndexesToBuild(ChangedTableDescription desc, Table oldTable, Table newTable, Collection<TableIndex> indexes) {
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
    }

    private void alterTableTable(final Session session,
                                 QueryContext context,
                                 final Table origTable,
                                 Table newDefinition,
                                 final TableChangeValidatorState changeState,
                                 final boolean isGroupChange) {
        final AkibanInformationSchema origAIS = origTable.getAIS();

        dropAffectedGroupIndexes(session, origTable, changeState);

        // Save previous state so it can be scanned
        final Schema origSchema = SchemaCache.globalSchema(origAIS);
        final RowType origTableType = origSchema.tableRowType(origTable);

        // Alter through schemaManager to get new definitions and RowDefs
        schemaManager().alterTableDefinitions(session, changeState.descriptions);

        // Build transformation
        final StoreAdapter adapter = store().createAdapter(session, origSchema);
        final QueryContext queryContext = new DelegatingContext(adapter, context);
        final QueryBindings queryBindings = queryContext.createBindings();

        final AkibanInformationSchema newAIS = getAIS(session);
        final Table newTable = newAIS.getTable(newDefinition.getName());
        final Schema newSchema = SchemaCache.globalSchema(newAIS);

        final List<Column> newColumns = newTable.getColumnsIncludingInternal();
        final List<TPreparedExpression> pProjections;

        pProjections = new ArrayList<>(newColumns.size());
        for(Column newCol : newColumns) {
            Integer oldPosition = findOldPosition(changeState.columnChanges, origTable, newCol);
            TInstance newInst = newCol.tInstance();
            if(oldPosition == null) {
                final String defaultString = newCol.getDefaultValue();
                final ValueSource defaultValueSource;
                if(defaultString == null) {
                    defaultValueSource = ValueSources.getNullSource(newInst);
                } else {
                    Value defaultValue = new Value(newInst);
                    TInstance defInstance = MString.VARCHAR.instance(defaultString.length(), defaultString == null);
                    TExecutionContext executionContext = new TExecutionContext(
                            Collections.singletonList(defInstance),
                            newInst,
                            queryContext
                    );
                    Value defaultSource = new Value(MString.varcharFor(defaultString), defaultString);
                    newInst.typeClass().fromObject(executionContext, defaultSource, defaultValue);
                    defaultValueSource = defaultValue;
                }
                pProjections.add(new TPreparedLiteral(newInst, defaultValueSource));
            } else {
                Column oldCol = origTable.getColumnsIncludingInternal().get(oldPosition);
                TInstance oldInst = oldCol.tInstance();
                TPreparedExpression pExp = new TPreparedField(oldInst, oldPosition);
                if(!oldInst.equalsExcludingNullable(newInst)) {
                    TCast cast = t3Registry.getCastsResolver().cast(oldInst.typeClass(), newInst.typeClass());
                    pExp = new TCastExpression(pExp, cast, newInst, queryContext);
                }
                pProjections.add(pExp);
            }
        }

        // PTRT for constraint checking
        final ProjectedTableRowType newTableType = new ProjectedTableRowType(newSchema, newTable, pProjections, !isGroupChange);

        // For any table, group affecting or not, the original rows are never modified.
        // The affected group is scanned fully and every row is inserted into its new
        // location (which may be multiple groups).

        // TODO: propagateDownGroup could probably be skipped, given that we know insertion order?
        List<Table> roots = new ArrayList<>();
        if(isGroupChange && origTable.isRoot() && !newTable.isRoot()) {
            Table newRoot = newTable.getGroup().getRoot();
            Table oldNewRoot = origAIS.getTable(newRoot.getName());
            roots.add(oldNewRoot);
        }
        roots.add(origTable.getGroup().getRoot());

        final Map<RowType,RowTypeAndIndexes> typeMap = new HashMap<>();
        for(Table root : roots) {
            root.visit(new AbstractVisitor() {
                @Override
                public void visit(Table table) {
                    RowType oldType = origSchema.tableRowType(table);
                    final RowType newType;
                    final TableIndex[] indexes;
                    Collection<TableIndex> indexesToBuild = new HashSet<>();
                    if(table == origTable) {
                        newType = newTableType;
                        indexesToBuild.addAll(findNewIndexesToBuild(changeState.tableIndexChanges, newTable));
                    } else {
                        newType = newSchema.tableRowType(newAIS.getTable(table.getName()));
                    }
                    for(ChangedTableDescription desc : changeState.descriptions) {
                        if(table.getName().equals(desc.getOldName())) {
                            collectIndexesToBuild(desc, table, newType.table(), indexesToBuild);
                            break;
                        }
                    }
                    indexes = indexesToBuild.toArray(new TableIndex[indexesToBuild.size()]);
                    typeMap.put(oldType, new RowTypeAndIndexes(newType, indexes));
                }
            });
        }

        for(Table root : roots) {
            Operator plan = groupScan_Default(root.getGroup());
            com.foundationdb.qp.operator.Cursor cursor = API.cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            try {
                Row oldRow;
                while((oldRow = cursor.next()) != null) {
                    RowType oldType = oldRow.rowType();
                    final Row newRow;
                    final TableIndex[] indexes;
                    if(oldType == origTableType) {
                        newRow = new ProjectedRow(newTableType,
                                                  oldRow,
                                                  queryContext,
                                                  queryBindings,
                                                  ProjectedRow.createTEvaluatableExpressions(pProjections),
                                                  TInstance.createTInstances(pProjections));
                        queryContext.checkConstraints(newRow);
                        indexes = typeMap.get(oldType).indexes;
                    } else {
                        RowTypeAndIndexes type = typeMap.get(oldType);
                        newRow = new OverlayingRow(oldRow, type.rowType);
                        indexes = type.indexes;
                    }
                    adapter.writeRow(newRow, indexes, null);
                }
            } finally {
                cursor.closeTopLevel();
            }
        }

        // Now rebuild any group indexes, leaving out empty ones
        createAffectedGroupIndexes(session, origTable, newTable, changeState, true);
    }

    @Override
    public ChangeLevel alterTable(Session session,
                                  TableName tableName,
                                  Table newDefinition,
                                  List<TableChange> columnChanges,
                                  List<TableChange> tableIndexChanges,
                                  QueryContext context)
    {
        final ChangeLevel level;
        txnService.beginTransaction(session);
        try {
            final Set<Integer> tableIDs = new HashSet<>();
            final TableChangeValidator validator;
            Table origTable = getTable(session, tableName);
            validator = new TableChangeValidator(origTable, newDefinition, columnChanges, tableIndexChanges);

            try {
                validator.compareAndThrowIfNecessary();
            } catch(TableChangeValidatorException e) {
                throw new InvalidAlterException(tableName, e.getMessage());
            }

            TableName newParentName = null;
            for(ChangedTableDescription desc : validator.getState().descriptions) {
                Table table = getTable(session, desc.getOldName());
                tableIDs.add(table.getTableId());
                if(desc.getOldName().equals(tableName)) {
                    newParentName = desc.getParentName();
                }
            }

            // If this is a TABLE or GROUP change, we're using a new group tree. Need to lock entire group.
            if(validator.getFinalChangeLevel() == ChangeLevel.TABLE || validator.getFinalChangeLevel() == ChangeLevel.GROUP) {
                // Old branch. Defensive because there can't currently be old parents
                Table parent = origTable.getParentTable();
                collectGroupTableIDs(tableIDs, parent);
                // New branch
                if(newParentName != null) {
                    collectGroupTableIDs(tableIDs, getTable(session, newParentName));
                }
            }

            schemaManager().setAlterTableActive(session, true);
            level = alterTableInternal(session, tableIDs, tableName, newDefinition, validator, context);
            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
            schemaManager().setAlterTableActive(session, false);
        }
        return level;
    }

    private ChangeLevel alterTableInternal(Session session,
                                           Collection<Integer> affectedTableIDs,
                                           TableName tableName,
                                           Table newDefinition,
                                           TableChangeValidator validator,
                                           QueryContext context)
    {
        final AkibanInformationSchema origAIS = getAIS(session);
        final Table origTable = getTable(session, tableName);
        final TableChangeValidatorState changeState = validator.getState();

        ChangeLevel changeLevel;
        List<Index> indexesToDrop = new ArrayList<>();
        List<Sequence> sequencesToDrop = new ArrayList<>();
        try {
            changeLevel = validator.getFinalChangeLevel();

            // Any GroupIndex changes are entirely derived, ignore any that happen to be passed.
            if(newDefinition.getGroup() != null) {
                newDefinition.getGroup().removeIndexes(newDefinition.getGroup().getIndexes());
            }

            for(ChangedTableDescription desc : changeState.descriptions) {
                for(TableName name : desc.getDroppedSequences()) {
                    sequencesToDrop.add(origAIS.getSequence(name));
                }
                Table oldTable = origAIS.getTable(desc.getOldName());
                for(Index index : oldTable.getIndexesIncludingInternal()) {
                    String indexName = index.getIndexName().getName();
                    if(!desc.getPreserveIndexes().containsKey(indexName)) {
                        indexesToDrop.add(index);
                    }
                }
            }

            if(!changeLevel.isNoneOrMetaData()) {
                Group group = origTable.getGroup();
                // entry.getValue().isEmpty() => index going away, non-empty will get rebuilt with new tree
                for(IndexName name : changeState.affectedGroupIndexes.keySet()) {
                    indexesToDrop.add(group.getIndex(name.getName()));
                }
            }

            final boolean groupChange = changeLevel == ChangeLevel.GROUP;
            switch(changeLevel) {
                case NONE:
                    assert changeState.affectedGroupIndexes.isEmpty() : changeState.affectedGroupIndexes;
                    AlterMadeNoChangeException error = new AlterMadeNoChangeException(tableName);
                    if(context != null) {
                        context.warnClient(error);
                    } else {
                        logger.warn(error.getMessage());
                    }
                break;
                case METADATA:
                    alterTableMetadata(session, context, origTable, newDefinition, changeState, false);
                break;
                case METADATA_NOT_NULL:
                    alterTableMetadata(session, context, origTable, newDefinition, changeState, true);
                break;
                case INDEX:
                    alterTableIndex(session, origTable, newDefinition, changeState);
                break;
                case TABLE:
                case GROUP:
                    alterTableTable(session, context, origTable, newDefinition, changeState, groupChange);
                break;

                default:
                    throw new IllegalStateException(changeLevel.toString());
            }
        } catch(Exception e) {
            if(!(e instanceof InvalidOperationException)) {
                logger.error("Rethrowing exception from failed ALTER", e);
            }
            throw throwAlways(e);
        }

        // Complete: we can now get rid of any trees that shouldn't be here
        store().deleteIndexes(session, indexesToDrop);
        store().deleteSequences(session, sequencesToDrop);

        // Old group tree
        if(changeLevel == ChangeLevel.TABLE || changeLevel == ChangeLevel.GROUP) {
            store().removeTree(session, origTable.getGroup());
        }

        // New parent's old group tree
        List<Join> newParent = newDefinition.getCandidateParentJoins();
        if(!newParent.isEmpty()) {
            Table newParentOldTable = origAIS.getTable(newParent.get(0).getParent().getName());
            store().removeTree(session, newParentOldTable.getGroup());
        }

        Map<TableName,TableName> allTableNames = new HashMap<>();
        for(Integer tableID : affectedTableIDs) {
            Table table = origAIS.getTable(tableID);
            allTableNames.put(table.getName(), table.getName());
        }
        allTableNames.put(tableName, newDefinition.getName());
        store().finishedAlter(session, allTableNames, changeLevel);

        return changeLevel;
    }

    @Override
    public void alterSequence(Session session, TableName sequenceName, Sequence newDefinition)
    {
        logger.trace("altering sequence {}", sequenceName);
        txnService.beginTransaction(session);
        try {
            AkibanInformationSchema ais = getAIS(session);
            Sequence oldSeq = ais.getSequence(sequenceName);
            if(oldSeq == null) {
                throw new NoSuchSequenceException(sequenceName);
            }
            schemaManager().alterSequence(session, sequenceName, newDefinition);

            // Remove old tree
            store().deleteSequences(session, Collections.singleton(oldSeq));

            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
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
        }
        List<Sequence> sequencesToDrop = new ArrayList<>();
        for (Sequence sequence : schema.getSequences().values()) {
            // Drop the sequences in this schema, but not the 
            // generator sequences, which will be dropped with the table. 
            if (!(sequence.getSequenceName().getTableName().startsWith(DefaultNameGenerator.IDENTITY_SEQUENCE_PREFIX))) {
                sequencesToDrop.add(sequence);
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
        // Do the actual dropping
        for(View view : viewsToDrop) {
            dropView(session, view.getName());
        }
        for(Table table : tablesToDrop) {
            dropTableInternal(session, table.getName());
        }
        for(Group group : groupsToDrop) {
            dropGroupInternal(session, group.getName());
        }
        for (Sequence sequence : sequencesToDrop) {
            dropSequence(session, sequence.getSequenceName());
        }
        for (Routine routine : routinesToDrop) {
            dropRoutine(session, routine.getName());
        }
        for (SQLJJar jar : jarsToDrop) {
            dropSQLJJar(session, jar.getName());
        }
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
    public void createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
        logger.debug("creating indexes {}", indexesToAdd);
        if (indexesToAdd.isEmpty()) {
            return;
        }

        txnService.beginTransaction(session);
        try {
            createIndexesInternal(session, indexesToAdd);
            txnService.commitTransaction(session);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
        }
    }

    void createIndexesInternal(Session session, Collection<? extends Index> indexesToAdd) {
        Collection<Index> newIndexes = schemaManager().createIndexes(session, indexesToAdd, false);
        for(Index index : newIndexes) {
            checkCursorsForDDLModification(session, index.leafMostTable());
        }
        store().buildIndexes(session, newIndexes);
        for(TableListener listener : listenerService.getTableListeners()) {
            listener.onCreateIndex(session, newIndexes);
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
                if(index.isPrimaryKey()) {
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

    private void collectGroupTableIDs(final Collection<Integer> tableIDs, Table table) {
        if(table == null) {
            return;
        }
        table.getGroup().visit(
            new AbstractVisitor()
            {
                @Override
                public void visit(Table table) {
                    tableIDs.add(table.getTableId());
                }
            }
        );
    }

    public void createSequence(Session session, Sequence sequence) {
        schemaManager().createSequence (session, sequence);
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

    BasicDDLFunctions(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store,
                      IndexStatisticsService indexStatisticsService, TypesRegistryService t3Registry,
                      TransactionService txnService, ListenerService listenerService) {
        super(middleman, schemaManager, store);
        this.indexStatisticsService = indexStatisticsService;
        this.t3Registry = t3Registry;
        this.txnService = txnService;
        this.listenerService = listenerService;
    }


    //
    // Internal
    //

    public Integer findOldPosition(List<TableChange> columnChanges, Table oldTable, Column newColumn) {
        String newName = newColumn.getName();
        for(TableChange change : columnChanges) {
            if(newName.equals(change.getNewName())) {
                switch(change.getChangeType()) {
                    case ADD:
                        return null;
                    case MODIFY:
                        Column oldColumn = oldTable.getColumn(change.getOldName());
                        assert oldColumn != null : newColumn;
                        return oldColumn.getPosition();
                    case DROP:
                        throw new IllegalStateException("Dropped new column? " + newName);
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

    public List<TableIndex> findNewIndexesToBuild(List<TableChange> indexChanges, Table newTable) {
        List<TableIndex> indexes = new ArrayList<>();
        for(TableChange change : indexChanges) {
            switch(change.getChangeType()) {
                case ADD:
                case MODIFY:
                    indexes.add(newTable.getIndexIncludingInternal(change.getNewName()));
                    break;
            }
        }
        return indexes;
    }

    public void dropAffectedGroupIndexes(Session session,
                                         Table origTable,
                                         TableChangeValidatorState changeState) {
        if(changeState.affectedGroupIndexes.isEmpty()) {
            return;
        }
        List<GroupIndex> groupIndexes = new ArrayList<>();
        for(IndexName name : changeState.affectedGroupIndexes.keySet()) {
            groupIndexes.add(origTable.getGroup().getIndex(name.getName()));
        }
        schemaManager().dropIndexes(session, groupIndexes);
    }

    public void createAffectedGroupIndexes(Session session,
                                           Table origTable,
                                           Table newTable,
                                           TableChangeValidatorState changeState,
                                           boolean isDataChange) {
        // Ideally only would copy the Group, but that is vulnerable to changing group names. Even if we handle that
        // by looking up the new name, index creation in PSSM requires index.getName().getTableName() match the actual.
        AkibanInformationSchema tempAIS = getAISCloner().clone(newTable.getAIS());
        List<Index> indexesToBuild = new ArrayList<>();
        Group origGroup = origTable.getGroup();
        Group tempGroup = tempAIS.getGroup(newTable.getGroup().getName());
        for(Map.Entry<IndexName, List<TableColumnNames>> entry : changeState.affectedGroupIndexes.entrySet()) {
            GroupIndex origIndex = origGroup.getIndex(entry.getKey().getName());
            List<TableColumnNames> columns = entry.getValue();
            // TableChangeValidator returns the index with no remaining columns
            if(columns.isEmpty()) {
                continue;
            }
            GroupIndex tempIndex = GroupIndex.create(tempAIS, tempGroup, origIndex);
            for(int i = 0; i < columns.size(); ++i) {
                TableColumnNames tcn = columns.get(i);
                Table tempTable = tempAIS.getTable(tcn.tableName);
                Column tempColumn = tempTable.getColumn(tcn.newColumnName);
                IndexColumn.create(tempIndex, tempColumn, i, true, null);
            }
            if (!isDataChange) {
                // TODO: Maybe need a way to say copy without the tree name part?
                tempIndex.copyStorageDescription(origIndex);
            }
            indexesToBuild.add(tempIndex);
        }

        if(isDataChange) {
            createIndexesInternal(session, indexesToBuild);
        } else {
            // Restore old trees
            schemaManager().createIndexes(session, indexesToBuild, true);
        }
    }
}
