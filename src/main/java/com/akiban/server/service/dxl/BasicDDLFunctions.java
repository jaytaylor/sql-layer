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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.AccumulatorAdapter.AccumInfo;
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
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.error.ProtectedIndexException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.error.UnsupportedDropException;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BasicDDLFunctions extends ClientAPIBase implements DDLFunctions {

    private final static Logger logger = LoggerFactory.getLogger(BasicDDLFunctions.class);

    private final IndexStatisticsService indexStatisticsService;
    
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
        }
        schemaManager().deleteTableDefinition(session, tableName.getSchemaName(), tableName.getTableName());
        checkCursorsForDDLModification(session, table);
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
            store().buildIndexes(session, newIndexes, false);
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

    BasicDDLFunctions(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store, TreeService treeService, IndexStatisticsService indexStatisticsService) {
        super(middleman, schemaManager, store, treeService);
        this.indexStatisticsService = indexStatisticsService;
    }
}
