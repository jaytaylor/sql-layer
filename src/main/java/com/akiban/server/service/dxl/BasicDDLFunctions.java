/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.dxl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.util.ChangedTableDescription;
import com.akiban.ais.util.TableChange;
import com.akiban.ais.util.TableChangeValidator;
import com.akiban.ais.util.TableChangeValidatorException;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.QueryContextBase;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.ProjectedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.ConstraintChecker;
import com.akiban.qp.rowtype.ProjectedUserTableRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowChecker;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.AccumulatorAdapter.AccumInfo;
import com.akiban.server.error.AlterMadeNoChangeException;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidAlterException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.scan.Cursor;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.error.ForeignConstraintDDLException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchGroupException;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.error.NoSuchSequenceException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.error.ProtectedIndexException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.error.UnsupportedDropException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.texpressions.TCastExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import com.akiban.server.types3.texpressions.TPreparedLiteral;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.akiban.ais.util.TableChangeValidator.ChangeLevel;
import static com.akiban.qp.operator.API.filter_Default;
import static com.akiban.qp.operator.API.groupScan_Default;
import static com.akiban.util.Exceptions.throwAlways;

class BasicDDLFunctions extends ClientAPIBase implements DDLFunctions {
    private final static Logger logger = LoggerFactory.getLogger(BasicDDLFunctions.class);

    private final static boolean DEFER_INDEX_BUILDING = false;
    private static final boolean ALTER_AUTO_INDEX_CHANGES = true;

    private final IndexStatisticsService indexStatisticsService;
    private final ConfigurationService configService;
    private final T3RegistryService t3Registry;
    

    private static class ShimContext extends QueryContextBase {
        private final StoreAdapter adapter;
        private final QueryContext delegate;

        public ShimContext(StoreAdapter adapter, QueryContext delegate) {
            this.adapter = adapter;
            this.delegate = (delegate == null) ? new SimpleQueryContext(adapter) : delegate;
        }

        @Override
        public StoreAdapter getStore() {
            return adapter;
        }

        @Override
        public StoreAdapter getStore(UserTable table) {
            return adapter;
        }

        @Override
        public Session getSession() {
            return delegate.getSession();
        }

        @Override
        public String getCurrentUser() {
            return delegate.getCurrentUser();
        }

        @Override
        public String getSessionUser() {
            return delegate.getSessionUser();
        }

        @Override
        public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
            delegate.notifyClient(level, errorCode, message);
        }

        @Override
        public long sequenceNextValue(TableName sequence) {
            return delegate.sequenceNextValue(sequence);
        }

        @Override
        public long sequenceCurrentValue(TableName sequence) {
            return delegate.sequenceCurrentValue(sequence);
        }

        @Override
        public long getQueryTimeoutSec() {
            return delegate.getQueryTimeoutSec();
        }
    }


    @Override
    public void createTable(Session session, UserTable table)
    {
        TableName tableName = schemaManager().createTableDefinition(session, table);
        checkCursorsForDDLModification(session, getAIS(session).getTable(tableName));
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
        logger.trace("dropping table {}", tableName);
        final Table table = getAIS(session).getTable(tableName);
        
        if(table == null) {
            return; // dropping a non-existing table is a no-op
        }

        final UserTable userTable = table.isUserTable() ? (UserTable)table : null;

        // Halo spec: may only drop leaf tables through DDL interface
        if(userTable == null || userTable.getChildJoins().isEmpty() == false) {
            throw new UnsupportedDropException(table.getName());
        }

        DMLFunctions dml = new BasicDMLFunctions(middleman(), schemaManager(), store(), treeService(), this);
        if(userTable.getParentJoin() == null) {
            // Root table and no child tables, can delete all associated trees
            store().removeTrees(session, table);
        } else {
            dml.truncateTable(session, table.getTableId());
            store().deleteIndexes(session, userTable.getIndexesIncludingInternal());
            store().deleteIndexes(session, userTable.getGroupIndexes());
            
            
            if (userTable.getIdentityColumn() != null) {
                Collection<Sequence> sequences = Collections.singleton(userTable.getIdentityColumn().getIdentityGenerator());
                store().deleteSequences(session, sequences);
            }
        }
        schemaManager().deleteTableDefinition(session, tableName.getSchemaName(), tableName.getTableName());
        checkCursorsForDDLModification(session, table);
    }

    private void doMetadataChange(Session session, QueryContext context, UserTable newDefinition,
                                  Collection<ChangedTableDescription> changedTables, boolean nullChange) {
        if(nullChange) {
            // Check new definition
            final ConstraintChecker checker = new UserTableRowChecker(newDefinition);

            // But scan old
            final AkibanInformationSchema origAIS = getAIS(session);
            final UserTable origTable = origAIS.getUserTable(changedTables.iterator().next().getOldName());
            final Schema oldSchema = SchemaCache.globalSchema(origAIS);
            final RowType oldSourceType = oldSchema.userTableRowType(origTable);
            final PersistitAdapter adapter = new PersistitAdapter(oldSchema, store().getPersistitStore(), treeService(), session, configService);
            final QueryContext queryContext = new ShimContext(adapter, context);

            Operator plan = filter_Default(
                    groupScan_Default(origTable.getGroup()),
                    Collections.singleton(oldSourceType)
            );
            com.akiban.qp.operator.Cursor cursor = API.cursor(plan, queryContext);

            cursor.open();
            try {
                Row oldRow;
                while((oldRow = cursor.next()) != null) {
                    checker.checkConstraints(oldRow, Types3Switch.ON);
                }
            } finally {
                cursor.close();
            }
        }

        schemaManager().alterTableDefinitions(session, changedTables);
    }

    private void doIndexChange(Session session, UserTable newDefinition,
                               Collection<ChangedTableDescription> changedTables, AlterTableHelper helper) {
        schemaManager().alterTableDefinitions(session, changedTables);
        AkibanInformationSchema newAIS = getAIS(session);
        UserTable newTable = newAIS.getUserTable(newDefinition.getName());
        List<Index> indexes = helper.findNewIndexesToBuild(newTable);
        store().buildIndexes(session, indexes, DEFER_INDEX_BUILDING);
    }

    private void doTableChange(Session session, QueryContext context, TableName tableName, UserTable newDefinition,
                               Collection<ChangedTableDescription> changedTables, Map<IndexName, List<Column>> affectedGroupIndexes,
                               AlterTableHelper helper, boolean groupChange) {

        final boolean usePValues = Types3Switch.ON;

        final AkibanInformationSchema origAIS = getAIS(session);
        final UserTable origTable = origAIS.getUserTable(tableName);

        // Drop definition and rebuild later, probably better than doing each entry individually
        if(!affectedGroupIndexes.isEmpty()) {
            List<GroupIndex> groupIndexes = new ArrayList<GroupIndex>();
            for(IndexName name : affectedGroupIndexes.keySet()) {
                groupIndexes.add(origTable.getGroup().getIndex(name.getName()));
            }
            store().truncateIndexes(session, groupIndexes);
            schemaManager().dropIndexes(session, groupIndexes);
        }

        // Save previous state so it can be scanned
        final Schema origSchema = SchemaCache.globalSchema(origAIS);
        final RowType origTableType = origSchema.userTableRowType(origTable);

        // Alter through schemaManager to get new definitions and RowDefs
        schemaManager().alterTableDefinitions(session, changedTables);

        // Build transformation
        final PersistitAdapter adapter = new PersistitAdapter(origSchema, store().getPersistitStore(), treeService(), session, configService);
        final QueryContext queryContext = new ShimContext(adapter, context);

        final AkibanInformationSchema newAIS = getAIS(session);
        final UserTable newTable = newAIS.getUserTable(newDefinition.getName());
        final Schema newSchema = SchemaCache.globalSchema(newAIS);

        final List<Column> newColumns = newTable.getColumnsIncludingInternal();
        final List<Expression> projections;
        final List<TPreparedExpression> pProjections;
        if(Types3Switch.ON) {
            projections = null;
            pProjections = new ArrayList<TPreparedExpression>(newColumns.size());
            for(Column newCol : newColumns) {
                Column oldCol = origTable.getColumn(newCol.getName());
                Integer oldPosition = helper.findOldPosition(origTable, newCol);
                TInstance newInst = newCol.tInstance();
                if(oldPosition == null) {
                    pProjections.add(new TPreparedLiteral(newInst, PValueSources.getNullSource(newInst.typeClass().underlyingType())));
                } else {
                    TInstance oldInst = oldCol.tInstance();
                    TPreparedExpression pExp = new TPreparedField(oldInst, oldPosition);
                    if(oldInst.typeClass() != newInst.typeClass()) {
                        TCast cast = t3Registry.cast(oldInst.typeClass(), newInst.typeClass());
                        pExp = new TCastExpression(pExp, cast, newInst, queryContext);
                    }
                    pProjections.add(pExp);
                }
            }
        } else {
            projections = new ArrayList<Expression>(newColumns.size());
            pProjections = null;
            for(Column newCol : newColumns) {
                Integer oldPosition = helper.findOldPosition(origTable, newCol);
                if(oldPosition == null) {
                    String defaultValue = newCol.getDefaultValue();
                    projections.add(new LiteralExpression(AkType.VARCHAR, defaultValue));
                } else {
                    projections.add(new FieldExpression(origTableType, oldPosition));
                }
            }
        }

        // PUTRT for constraint checking
        final ProjectedUserTableRowType newTableType = new ProjectedUserTableRowType(newSchema, newTable, projections, pProjections, !groupChange);

        Index[] oldTypeIndexes = null;
        if(!groupChange) {
            List<Index> indexesToBuild = helper.findNewIndexesToBuild(newTable);
            oldTypeIndexes = indexesToBuild.toArray(new Index[indexesToBuild.size()]);
        }

        // - For non-group change, only need to scan the table being modified.
        // - For a group change, we need to scan entire group (catch all orphans).
        //   The process of deleting a parent will update its children, and updating
        //   orphans directly covers all rows. PersistitAdapter#alterRow() does the
        //   step handling so this scan is safe (deletes at current step, writes at +1)

        final Set<RowType> filteredTypes;
        final Map<RowType,RowType> typeMap;
        if(groupChange) {
            filteredTypes = new HashSet<RowType>();
            typeMap = new HashMap<RowType,RowType>();
            origTable.traverseTableAndDescendants(new NopVisitor() {
                @Override
                public void visitUserTable(UserTable table) {
                    RowType oldType = origSchema.userTableRowType(table);
                    RowType newType = (table == origTable)
                            ? newTableType
                            : newSchema.userTableRowType(newAIS.getUserTable(table.getName()));
                    filteredTypes.add(oldType);
                    typeMap.put(oldType, newType);
                }
            });
        } else {
            filteredTypes = Collections.singleton(origTableType);
            typeMap = Collections.<RowType,RowType>singletonMap(origTableType, newTableType);
        }

        Operator plan = filter_Default(
                groupScan_Default(origTable.getGroup()),
                filteredTypes
        );
        com.akiban.qp.operator.Cursor cursor = API.cursor(plan, queryContext);


        int step = adapter.enterUpdateStep(true);
        cursor.open();
        try {
            Row oldRow;
            while((oldRow = cursor.next()) != null) {
                RowType oldType = oldRow.rowType();
                if(oldType == origTableType) {
                    Row newRow = new ProjectedRow(newTableType, oldRow, queryContext, projections, pProjections);
                    queryContext.checkConstraints(newRow, usePValues);
                    adapter.alterRow(oldRow, newRow, oldTypeIndexes, groupChange, usePValues);
                } else {
                    RowType newType = typeMap.get(oldType);
                    Row newRow = new OverlayingRow(oldRow, newType, usePValues);
                    adapter.alterRow(oldRow, newRow, null, groupChange, usePValues);
                }
            }

            // Now rebuild any group indexes, leaving out empty ones
            List<Index> gisToBuild = new ArrayList<Index>();
            adapter.enterUpdateStep();
            helper.recreateAffectedGroupIndexes(origTable, newTable, gisToBuild, affectedGroupIndexes);
            if(!gisToBuild.isEmpty()) {
                adapter.enterUpdateStep();
                createIndexes(session, gisToBuild);
            }
        } finally {
            adapter.leaveUpdateStep(step);
            cursor.close();
        }
    }

    @Override
    public ChangeLevel alterTable(Session session, TableName tableName, UserTable newDefinition,
                                  List<TableChange> origColChanges, List<TableChange> origIndexChanges,
                                  QueryContext context)
    {
        final AkibanInformationSchema origAIS = getAIS(session);
        final UserTable origTable = getUserTable(session, tableName);
        List<TableChange> columnChanges = new ArrayList<TableChange>(origColChanges);
        List<TableChange> indexChanges = new ArrayList<TableChange>(origIndexChanges);

        TableChangeValidator validator = new TableChangeValidator(origTable, newDefinition, columnChanges, indexChanges,
                                                                  ALTER_AUTO_INDEX_CHANGES);

        try {
            validator.compareAndThrowIfNecessary();
        } catch(TableChangeValidatorException e) {
            throw new InvalidAlterException(tableName, e.getMessage());
        }

        ChangeLevel changeLevel;
        boolean rollBackNeeded = false;
        Set<String> savedSchemas = new HashSet<String>();
        Map<TableName,Integer> savedOrdinals = new HashMap<TableName,Integer>();
        List<Index> indexesToDrop = new ArrayList<Index>();
        List<Sequence> sequencesToDrop = new ArrayList<Sequence>();

        savedSchemas.add(tableName.getSchemaName());
        savedSchemas.add(newDefinition.getName().getSchemaName());
        try {
            AlterTableHelper helper = new AlterTableHelper(columnChanges, indexChanges);

            changeLevel = validator.getFinalChangeLevel();
            Map<IndexName, List<Column>> affectedGroupIndexes = validator.getAffectedGroupIndexes();
            Collection<ChangedTableDescription> changedTables = validator.getAllChangedTables();

            for(ChangedTableDescription desc : changedTables) {
                for(TableName name : desc.getDroppedSequences()) {
                    sequencesToDrop.add(origAIS.getSequence(name));
                }
                UserTable oldTable = origAIS.getUserTable(desc.getOldName());
                for(Index index : oldTable.getIndexesIncludingInternal()) {
                    if(desc.getPreserveIndexes().get(index.getIndexName().getName()) == null) {
                        indexesToDrop.add(index);
                    }
                }
            }

            switch(changeLevel) {
                case NONE:
                    AlterMadeNoChangeException error = new AlterMadeNoChangeException(tableName);
                    if(context != null) {
                        context.warnClient(error);
                    } else {
                        logger.warn(error.getMessage());
                    }
                break;

                case METADATA:
                    assert affectedGroupIndexes.isEmpty() : affectedGroupIndexes;
                    doMetadataChange(session, context, newDefinition, changedTables, false);
                break;

                case METADATA_NOT_NULL:
                    assert affectedGroupIndexes.isEmpty() : affectedGroupIndexes;
                    doMetadataChange(session, context, newDefinition, changedTables, true);
                break;

                case INDEX:
                    assert affectedGroupIndexes.isEmpty() : affectedGroupIndexes;
                    doIndexChange(session, newDefinition, changedTables, helper);
                break;

                case TABLE:
                    doTableChange(session, context, tableName, newDefinition, changedTables, affectedGroupIndexes,
                                  helper, false);
                break;

                case GROUP:
                    // PRIMARY tree *must* be preserved due to accumulators. No way to dup accum state so must do this.
                    List<Index> indexesToTruncate = new ArrayList<Index>();
                    for(ChangedTableDescription desc : validator.getAllChangedTables()) {
                        desc.getPreserveIndexes().put(Index.PRIMARY_KEY_CONSTRAINT, Index.PRIMARY_KEY_CONSTRAINT);
                        UserTable oldTable = origAIS.getUserTable(desc.getOldName());
                        Index index = oldTable.getPrimaryKeyIncludingInternal().getIndex();
                        indexesToTruncate.add(index);
                        indexesToDrop.remove(index);
                        savedSchemas.add(desc.getOldName().getSchemaName());
                        savedOrdinals.put(desc.getOldName(), oldTable.rowDef().getOrdinal());
                    }
                    store().truncateIndexes(session, indexesToTruncate);
                    doTableChange(session, context, tableName, newDefinition, changedTables, affectedGroupIndexes,
                                  helper, true);
                break;

                default:
                    throw new IllegalStateException("Unhandled ChangeLevel: " + validator.getFinalChangeLevel());
            }
        } catch(Exception e) {
            if(!(e instanceof InvalidOperationException)) {
                logger.error("Rethrowing exception from failed ALTER", e);
            }
            rollBackNeeded = true;
            throw throwAlways(e);
        } finally {
            if(rollBackNeeded) {
                // All of the data changed was transactional but PSSM changes aren't like that
                AkibanInformationSchema curAIS = getAIS(session);
                if(origAIS != curAIS) {
                    schemaManager().rollbackAIS(session, origAIS, savedOrdinals, savedSchemas);
                }
                // TODO: rollback new index trees?
            }
        }

        // Complete: we can now get rid of any trees that shouldn't be here
        store().deleteIndexes(session, indexesToDrop);
        store().deleteSequences(session, sequencesToDrop);
        return changeLevel;
    }

    @Override
    public void dropSchema(Session session, String schemaName)
    {
        logger.trace("dropping schema {}", schemaName);

        // Find all groups and tables in the schema
        Set<Group> groupsToDrop = new HashSet<Group>();
        List<UserTable> tablesToDrop = new ArrayList<UserTable>();

        final AkibanInformationSchema ais = getAIS(session);
        for(UserTable table : ais.getUserTables().values()) {
            final TableName tableName = table.getName();
            if(tableName.getSchemaName().equals(schemaName)) {
                groupsToDrop.add(table.getGroup());
                // Cannot drop entire group of parent is not in the same schema
                final Join parentJoin = table.getParentJoin();
                if(parentJoin != null) {
                    final UserTable parentTable = parentJoin.getParent();
                    if(!parentTable.getName().getSchemaName().equals(schemaName)) {
                        tablesToDrop.add(table);
                    }
                }
                // All children must be in the same schema
                for(Join childJoin : table.getChildJoins()) {
                    final TableName childName = childJoin.getChild().getName();
                    if(!childName.getSchemaName().equals(schemaName)) {
                        throw new ForeignConstraintDDLException(tableName, childName);
                    }
                }
            }
        }
        List<Sequence> sequencesToDrop = new ArrayList<Sequence>();
        for (Sequence sequence : ais.getSequences().values()) {
            // Drop the sequences in this schema, but not the 
            // generator sequences, which will be dropped with the table. 
            if (sequence.getSchemaName().equals(schemaName) &&
                 !(sequence.getSequenceName().getTableName().startsWith("_sequence-"))) {
                sequencesToDrop.add(sequence);
            }
        }
        // Remove groups that contain tables in multiple schemas
        for(UserTable table : tablesToDrop) {
            groupsToDrop.remove(table.getGroup());
        }
        // Sort table IDs so higher (i.e. children) are first
        Collections.sort(tablesToDrop, new Comparator<UserTable>() {
            @Override
            public int compare(UserTable o1, UserTable o2) {

                return o2.getTableId().compareTo(o1.getTableId());
            }
        });
        // Do the actual dropping
        for(UserTable table : tablesToDrop) {
            dropTable(session, table.getName());
        }
        for(Group group : groupsToDrop) {
            dropGroup(session, group.getName());
        }
        for (Sequence sequence : sequencesToDrop) {
            dropSequence(session, sequence.getSequenceName());
        }
    }

    @Override
    public void dropGroup(Session session, String groupName)
    {
        logger.trace("dropping group {}", groupName);
        final Group group = getAIS(session).getGroup(groupName);
        if(group == null) {
            return;
        }
        final Table table = group.getGroupTable();
        final RowDef rowDef = getRowDef(table.getTableId());
        final TableName tableName = table.getName();
        try {
            store().dropGroup(session, rowDef.getRowDefId());
        } catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
        }
        schemaManager().deleteTableDefinition(session, tableName.getSchemaName(), tableName.getTableName());
        checkCursorsForDDLModification(session, table);
    }

    @Override
    public AkibanInformationSchema getAIS(final Session session) {
        logger.trace("getting AIS");
        return schemaManager().getAis(session);
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
        for (Table userTable : getAIS(session).getUserTables().values()) {
            if (tableId == userTable.getTableId()) {
                return userTable;
            }
        }
        for (Table groupTable : getAIS(session).getGroupTables().values()) {
            if (tableId == groupTable.getTableId()) {
                return groupTable;
            }
        }
        throw new NoSuchTableIdException(tableId);
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
    public UserTable getUserTable(Session session, TableName tableName) throws NoSuchTableException {
        logger.trace("getting AIS UserTable for {}", tableName);
        AkibanInformationSchema ais = getAIS(session);
        UserTable table = ais.getUserTable(tableName);
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
    public RowDef getRowDef(int tableId) throws RowDefNotFoundException {
        logger.trace("getting RowDef for {}", tableId);
        return store().getRowDefCache().getRowDef(tableId);
    }

    @Override
    public List<String> getDDLs(final Session session) {
        logger.trace("getting DDLs");
        return schemaManager().schemaStrings(session, false, false);
    }

    @Override
    public int getGeneration() {
        return schemaManager().getSchemaGeneration();
    }

    @Override
    public long getTimestamp() {
        return schemaManager().getUpdateTimestamp();
    }

    @Override
    public void createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
        logger.trace("creating indexes {}", indexesToAdd);
        if (indexesToAdd.isEmpty() == true) {
            return;
        }

        final Collection<Index> newIndexes;
        newIndexes = schemaManager().createIndexes(session, indexesToAdd);

        for(Index index : newIndexes) {
            checkCursorsForDDLModification(session, index.leafMostTable());
        }

        try {
            store().buildIndexes(session, newIndexes, DEFER_INDEX_BUILDING);
        } catch(InvalidOperationException e) {
            // Try and roll back all changes
            try {
                /*
                 * Call to deleteIndexes removed "temporarily" to fix
                 * a problem with MVCC pruning.  Theory: records
                 * added to any indexes will be removed anyway by
                 * rollback.  Any new Tree instances created above by
                 * buildIndexes will be left behind, but empty. -- Peter
                 */
//                store().deleteIndexes(session, newIndexes);
                schemaManager().dropIndexes(session, newIndexes);
            } catch(Exception e2) {
                logger.error("Exception while rolling back failed createIndex: " + newIndexes, e2);
            }
            throw e;
        }
    }

    @Override
    public void dropTableIndexes(Session session, TableName tableName, Collection<String> indexNamesToDrop)
    {
        logger.trace("dropping table indexes {} {}", tableName, indexNamesToDrop);
        if(indexNamesToDrop.isEmpty() == true) {
            return;
        }

        final Table table = getTable(session, tableName);
        Collection<Index> indexes = new HashSet<Index>();
        for(String indexName : indexNamesToDrop) {
            Index index = table.getIndex(indexName);
            if(index == null) {
                throw new NoSuchIndexException (indexName);
            }
            if(index.isPrimaryKey()) {
                throw new ProtectedIndexException ("PRIMARY", table.getName());
            }
            indexes.add(index);
        }
        // Drop them from the Store before while IndexDefs still exist
        store().deleteIndexes(session, indexes);
            
        schemaManager().dropIndexes(session, indexes);
        checkCursorsForDDLModification(session, table);
    }

    @Override
    public void dropGroupIndexes(Session session, String groupName, Collection<String> indexNamesToDrop) {
        logger.trace("dropping group indexes {} {}", groupName, indexNamesToDrop);
        if(indexNamesToDrop.isEmpty()) {
            return;
        }

        final Group group = getAIS(session).getGroup(groupName);
        if (group == null) {
            throw new NoSuchGroupException(groupName);
        }

        Collection<Index> indexes = new HashSet<Index>();
        for(String indexName : indexNamesToDrop) {
            final Index index = group.getIndex(indexName);
            if(index == null) {
                throw new NoSuchIndexException(indexName);
            }
            indexes.add(index);
        }

        // Drop them from the Store before while IndexDefs still exist
        store().deleteIndexes(session, indexes);
        schemaManager().dropIndexes(session, indexes);
        // TODO: checkCursorsForDDLModification ?
    }

    @Override
    public void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate) {
        final Table table = getTable(session, tableName);
        Collection<Index> indexes = new HashSet<Index>();
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
    public IndexCheckSummary checkAndFixIndexes(Session session, String schemaRegex, String tableRegex) {
        long startNs = System.nanoTime();
        Pattern schemaPattern = Pattern.compile(schemaRegex);
        Pattern tablePattern = Pattern.compile(tableRegex);
        List<IndexCheckResult> results = new ArrayList<IndexCheckResult>();
        AkibanInformationSchema ais = getAIS(session);

        for (Map.Entry<TableName,UserTable> entry : ais.getUserTables().entrySet()) {
            TableName tName = entry.getKey();
            if (schemaPattern.matcher(tName.getSchemaName()).find()
                    && tablePattern.matcher(tName.getTableName()).find())
            {
                UserTable uTable = entry.getValue();
                List<Index> indexes = new ArrayList<Index>();
                indexes.add(uTable.getPrimaryKeyIncludingInternal().getIndex());
                for (Index gi : uTable.getGroup().getIndexes()) {
                    if (gi.leafMostTable().equals(uTable))
                        indexes.add(gi);
                }
                for (Index index : indexes) {
                    IndexCheckResult indexCheckResult = checkAndFixIndex(session, index);
                    results.add(indexCheckResult);
                }
            }
        }
        long endNs = System.nanoTime();
        return new IndexCheckSummary(results,  endNs - startNs);
    }

    private IndexCheckResult checkAndFixIndex(Session session, Index index) {
        try {
            long expected = indexStatisticsService.countEntries(session, index);
            long actual = indexStatisticsService.countEntriesManually(session, index);
            if (expected != actual) {
                PersistitStore pStore = this.store().getPersistitStore();
                if (index.isTableIndex()) {
                    pStore.getTableStatus(((TableIndex) index).getTable()).setRowCount(actual);
                }
                else {
                    final Exchange ex = pStore.getExchange(session, index);
                    try {
                        AccumulatorAdapter accum =
                                new AccumulatorAdapter(AccumInfo.ROW_COUNT, treeService(), ex.getTree());
                        accum.set(actual);
                    }
                    finally {
                        pStore.releaseExchange(session, ex);
                    }
                }
            }
            return new IndexCheckResult(index.getIndexName(), expected, actual, indexStatisticsService.countEntries(session, index));
        }
        catch (Exception e) {
            logger.error("while checking/fixing " + index, e);
            return new IndexCheckResult(index.getIndexName(), -1, -1, -1);
        }
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

    private void checkCursorsForDDLModification(Session session, Table table) {
        Map<CursorId,BasicDXLMiddleman.ScanData> cursorsMap = getScanDataMap(session);
        if (cursorsMap == null) {
            return;
        }

        final int tableId;
        final int gTableId;
        {
            if (table.isUserTable()) {
                tableId = table.getTableId();
                gTableId = table.getGroup().getGroupTable().getTableId();
            }
            else {
                tableId = gTableId = table.getTableId();
            }
        }

        for (BasicDXLMiddleman.ScanData scanData : cursorsMap.values()) {
            Cursor cursor = scanData.getCursor();
            if (cursor.isClosed()) {
                continue;
            }
            ScanRequest request = cursor.getScanRequest();
            int scanTableId = request.getTableId();
            if (scanTableId == tableId || scanTableId == gTableId) {
                cursor.setDDLModified();
            }
        }
    }
    
    public void createSequence(Session session, Sequence sequence) {
        schemaManager().createSequence (session, sequence);
    }
   
    public void dropSequence(Session session, TableName sequenceName) {
        final Sequence sequence = getAIS(session).getSequence(sequenceName);
        
        if (sequence == null) {
            throw new NoSuchSequenceException (sequenceName);
        }
        
        store().deleteSequences(session, Collections.singleton(sequence));
        schemaManager().dropSequence(session, sequence);
    }

    BasicDDLFunctions(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store, TreeService treeService,
                      IndexStatisticsService indexStatisticsService, ConfigurationService configService, T3RegistryService t3Registry) {
        super(middleman, schemaManager, store, treeService);
        this.indexStatisticsService = indexStatisticsService;
        this.configService = configService;
        this.t3Registry = t3Registry;
    }
}
