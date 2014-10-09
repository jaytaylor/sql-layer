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
import com.foundationdb.ais.model.HKeyColumn;
import com.foundationdb.ais.model.HKeySegment;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.IndexRowComposition;
import com.foundationdb.ais.model.IndexToHKey;
import com.foundationdb.ais.model.Schema;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.storeadapter.OperatorBasedRowCollector;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.storeadapter.RowDataCreator;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.storeadapter.indexrow.SpatialColumnHandler;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.ConstantColumnSelector;
import com.foundationdb.server.api.dml.scan.LegacyRowWrapper;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.api.dml.scan.ScanLimit;
import com.foundationdb.server.error.CursorCloseBadException;
import com.foundationdb.server.error.CursorIsUnknownException;
import com.foundationdb.server.error.NoSuchRowException;
import com.foundationdb.server.error.RowDefNotFoundException;
import com.foundationdb.server.error.TableDefinitionMismatchException;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataExtractor;
import com.foundationdb.server.rowdata.RowDataValueSource;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.rowdata.encoding.EncodingException;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.RowListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.sql.optimizer.rule.PlanGenerator;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;
import com.persistit.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractStore<SType extends AbstractStore,SDType,SSDType extends StoreStorageDescription<SType,SDType>> implements Store {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStore.class);

    private static final InOutTap WRITE_ROW_TAP = Tap.createTimer("write: write_row");
    private static final InOutTap DELETE_ROW_TAP = Tap.createTimer("write: delete_row");
    private static final InOutTap UPDATE_ROW_TAP = Tap.createTimer("write: update_row");
    private static final InOutTap WRITE_ROW_GI_TAP = Tap.createTimer("write: write_row_gi");
    private static final InOutTap DELETE_ROW_GI_TAP = Tap.createTimer("write: delete_row_gi");
    private static final InOutTap UPDATE_ROW_GI_TAP = Tap.createTimer("write: update_row_gi");
    private static final InOutTap UPDATE_INDEX_TAP = Tap.createTimer("write: update_index");
    private static final InOutTap NEW_COLLECTOR_TAP = Tap.createTimer("read: new_collector");
    private static final PointTap SKIP_GI_MAINTENANCE = Tap.createCount("write: skip_gi_maintenance");
    private static final InOutTap PROPAGATE_CHANGE_TAP = Tap.createTimer("write: propagate_hkey_change");
    private static final InOutTap PROPAGATE_REPLACE_TAP = Tap.createTimer("write: propagate_hkey_change_row_replace");

    private static final Session.MapKey<Integer,List<RowCollector>> COLLECTORS = Session.MapKey.mapNamed("collectors");
    protected static final String FEATURE_DDL_WITH_DML_PROP = "fdbsql.feature.ddl_with_dml_on";

    protected final TransactionService txnService;
    protected final SchemaManager schemaManager;
    protected final ListenerService listenerService;
    protected final TypesRegistryService typesRegistryService;
    protected final ServiceManager serviceManager;
    protected OnlineHelper onlineHelper;
    protected ConstraintHandler<SType,SDType,SSDType> constraintHandler;

    protected AbstractStore(TransactionService txnService, SchemaManager schemaManager, ListenerService listenerService, TypesRegistryService typesRegistryService, ServiceManager serviceManager) {
        this.txnService = txnService;
        this.schemaManager = schemaManager;
        this.listenerService = listenerService;
        this.typesRegistryService = typesRegistryService;
        this.serviceManager = serviceManager;
    }


    //
    // Implementation methods
    //

    /** Create store specific data for working with the given storage area. */
    abstract SDType createStoreData(Session session, SSDType storageDescription);

    /** Release (or cache) any data created through {@link #createStoreData(Session, HasStorage)}. */
    abstract void releaseStoreData(Session session, SDType storeData);

    /** Get the <code>StorageDescription</code> that this store data works on. */
    abstract SSDType getStorageDescription(SDType storeData);

    /** Get the associated key */
    abstract Key getKey(Session session, SDType storeData);

    /** Save the current key and value. */
    abstract void store(Session session, SDType storeData);

    /** Fetch the value for the current key. Return <code>true</code> if it existed. */
    abstract boolean fetch(Session session, SDType storeData);

    /** Delete the key. */
    abstract void clear(Session session, SDType storeData);

    abstract void resetForWrite(SDType storeData, Index index, PersistitIndexRowBuffer indexRowBuffer);

    /** Create an iterator to visit all descendants of the current key. */
    protected abstract Iterator<Void> createDescendantIterator(Session session, SDType storeData);

    /** Read the index row for the given RowData or null if not present. storeData has been initialized for index. */
    protected abstract PersistitIndexRowBuffer readIndexRow(Session session,
                                                            Index parentPKIndex,
                                                            SDType storeData,
                                                            RowDef childRowDef,
                                                            RowData childRowData);

    /** Called when a non-serializable store would need a row lock. */
    protected abstract void lock(Session session, SDType storeData, RowDef rowDef, RowData rowData);

    /** Hook for tracking tables Session has written to. */
    protected abstract void trackTableWrite(Session session, Table table);

    //
    // AbstractStore
    //

    @SuppressWarnings("unchecked")
    public SDType createStoreData(Session session, HasStorage object) {
        return createStoreData(session, (SSDType)object.getStorageDescription());
    }

    protected void constructHKey(Session session, RowDef rowDef, RowData rowData, Key hKeyOut) {
        // Initialize the HKey being constructed
        hKeyOut.clear();
        PersistitKeyAppender hKeyAppender = PersistitKeyAppender.create(hKeyOut, rowDef.table().getName());

        // Metadata for the row's table
        Table table = rowDef.table();

        // Only set if parent row is looked up
        int i2hPosition = 0;
        IndexToHKey indexToHKey = null;
        SDType parentStoreData = null;
        PersistitIndexRowBuffer parentPKIndexRow = null;

        // All columns of all segments of the HKey
        for(HKeySegment hKeySegment : table.hKey().segments()) {
            // Ordinal for this segment
            RowDef segmentRowDef = hKeySegment.table().rowDef();
            hKeyAppender.append(segmentRowDef.table().getOrdinal());
            // Segment's columns
            for(HKeyColumn hKeyColumn : hKeySegment.columns()) {
                Table hKeyColumnTable = hKeyColumn.column().getTable();
                if(hKeyColumnTable != table) {
                    // HKey column from row of parent table
                    if (parentStoreData == null) {
                        // Initialize parent metadata and state
                        RowDef parentRowDef = rowDef.table().getParentTable().rowDef();
                        TableIndex parentPkIndex = parentRowDef.getPKIndex();
                        indexToHKey = parentPkIndex.indexToHKey();
                        parentStoreData = createStoreData(session, parentPkIndex);
                        parentPKIndexRow = readIndexRow(session, parentPkIndex, parentStoreData, rowDef, rowData);
                        i2hPosition = hKeyColumn.positionInHKey();
                    }
                    if(indexToHKey.isOrdinal(i2hPosition)) {
                        assert indexToHKey.getOrdinal(i2hPosition) == segmentRowDef.table().getOrdinal() : hKeyColumn;
                        ++i2hPosition;
                    }
                    if(parentPKIndexRow != null) {
                        parentPKIndexRow.appendFieldTo(indexToHKey.getIndexRowPosition(i2hPosition), hKeyAppender.key());
                    } else {
                        // Orphan row
                        hKeyAppender.appendNull();
                    }
                    ++i2hPosition;
                } else {
                    // HKey column from rowData
                    Column column = hKeyColumn.column();
                    hKeyAppender.append(column.getFieldDef(), rowData);
                }
            }
        }
        if(parentStoreData != null) {
            releaseStoreData(session, parentStoreData);
        }
    }

    protected boolean hasNullIndexSegments(RowData rowData, Index index)
    {
        assert index.leafMostTable().rowDef().getRowDefId() == rowData.getRowDefId();
        int nkeys = index.getKeyColumns().size();
        IndexRowComposition indexRowComposition = index.indexRowComposition();
        for (int i = 0; i < nkeys; i++) {
            int fi = indexRowComposition.getFieldPosition(i);
            if (rowData.isNull(fi)) {
                return true;
            }
        }
        return false;
    }

    protected static BitSet analyzeFieldChanges(RowDef oldRowDef, RowData oldRow, RowDef newRowDef, RowData newRow) {
        BitSet tablesRequiringHKeyMaintenance;
        assert oldRow.getRowDefId() == newRow.getRowDefId();
        int fields = oldRowDef.getFieldCount();
        // Find the PK and FK fields
        BitSet keyField = new BitSet(fields);
        TableIndex pkIndex = oldRowDef.getPKIndex();
        int nkeys = pkIndex.getKeyColumns().size();
        IndexRowComposition indexRowComposition = pkIndex.indexRowComposition();
        for (int i = 0; i < nkeys; i++) {
            int pkFieldPosition = indexRowComposition.getFieldPosition(i);
            keyField.set(pkFieldPosition, true);
        }
        for (int fkFieldPosition : oldRowDef.getParentJoinFields()) {
            keyField.set(fkFieldPosition, true);
        }
        // Find whether and where key fields differ
        boolean allEqual = true;
        for (int keyFieldPosition = keyField.nextSetBit(0);
             allEqual && keyFieldPosition >= 0;
             keyFieldPosition = keyField.nextSetBit(keyFieldPosition + 1)) {
            boolean fieldEqual = fieldEqual(oldRowDef, oldRow, newRowDef, newRow, keyFieldPosition);
            if (!fieldEqual) {
                allEqual = false;
            }
        }
        if (allEqual) {
            tablesRequiringHKeyMaintenance = null;
        } else {
            // A PK or FK field has changed, so the update has to be done as delete/insert. To minimize hKey
            // propagation work, find which tables (descendants of the updated table) are affected by hKey
            // changes.
            tablesRequiringHKeyMaintenance = hKeyDependentTableOrdinals(oldRowDef);
        }
        return tablesRequiringHKeyMaintenance;
    }

    /** Build a user-friendly representation of the Index row for the given RowData. */
    protected String formatIndexRowString(Session session, RowData rowData, Index index) {
        RowDataExtractor extractor = new RowDataExtractor(rowData, getGlobalRowDef(session, rowData));
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        boolean first = true;
        for(IndexColumn iCol : index.getKeyColumns()) {
            Object o = extractor.get(iCol.getColumn().getFieldDef());
            if(first) {
                first = false;
            } else {
                sb.append(',');
            }
            sb.append(o);
        }
        sb.append(')');
        return sb.toString();
    }

    private static BitSet hKeyDependentTableOrdinals(RowDef rowDef) {
        Table table = rowDef.table();
        BitSet ordinals = new BitSet();
        for (Table hKeyDependentTable : table.hKeyDependentTables()) {
            int ordinal = hKeyDependentTable.getOrdinal();
            ordinals.set(ordinal, true);
        }
        return ordinals;
    }

    protected void writeRow(Session session,
                            RowDef rowDef,
                            RowData rowData,
                            TableIndex[] tableIndexes,
                            BitSet tablesRequiringHKeyMaintenance,
                            boolean propagateHKeyChanges)
    {
        SDType storeData = createStoreData(session, rowDef.getGroup());
        WRITE_ROW_TAP.in();
        try {
            lock(session, storeData, rowDef, rowData);
            if(tableIndexes == null) {
                tableIndexes = rowDef.getIndexes();
            }
            writeRowInternal(session, storeData, rowDef, rowData, tableIndexes, tablesRequiringHKeyMaintenance, propagateHKeyChanges);
        } finally {
            WRITE_ROW_TAP.out();
            releaseStoreData(session, storeData);
        }
    }

    protected void deleteRow(Session session,
                             RowDef rowDef,
                             RowData rowData,
                             boolean cascadeDelete,
                             BitSet tablesRequiringHKeyMaintenance,
                             boolean propagateHKeyChanges)
    {
        SDType storeData = createStoreData(session, rowDef.getGroup());
        DELETE_ROW_TAP.in();
        try {
            lock(session, storeData, rowDef, rowData);
            deleteRowInternal(session,
                              storeData,
                              rowDef,
                              rowData,
                              cascadeDelete,
                              tablesRequiringHKeyMaintenance,
                              propagateHKeyChanges);
        } finally {
            DELETE_ROW_TAP.out();
            releaseStoreData(session, storeData);
        }
    }

    private void updateRow(Session session,
                           RowDef oldRowDef,
                           RowData oldRow,
                           RowDef newRowDef,
                           RowData newRow,
                           ColumnSelector selector,
                           boolean propagateHKeyChanges)
    {
        int oldID = oldRow.getRowDefId();
        int newID = newRow.getRowDefId();
        if(oldID != newID) {
            String msg = String.format("RowData values have different RowDef IDs: (%d vs %d)", oldID, newID);
            throw new IllegalArgumentException(msg);
        }

        // RowDefs may be different during an ALTER. Only non-PK/FK columns change in this scenario.
        SDType storeData = createStoreData(session, oldRowDef.getGroup());

        UPDATE_ROW_TAP.in();
        try {
            lock(session, storeData, oldRowDef, oldRow);
            lock(session, storeData, newRowDef, newRow);
            updateRowInternal(session, storeData, oldRowDef, oldRow, newRowDef, newRow, selector, propagateHKeyChanges);
        } finally {
            UPDATE_ROW_TAP.out();
            releaseStoreData(session, storeData);
        }
    }

    /** Delicate: Added to support GroupIndex building which only deals with FlattenedRows containing AbstractRows. */
    protected void lock(Session session, Row row) {
        RowData rowData = ((AbstractRow)row).rowData();
        Table table = row.rowType().table();
        SDType storeData = createStoreData(session, table.getGroup());
        try {
            lock(session, storeData, table.rowDef(), rowData);
        } finally {
            releaseStoreData(session, storeData);
        }
    }

    //
    // Store methods
    //

    @Override
    public AkibanInformationSchema getAIS(Session session) {
        return schemaManager.getAis(session);
    }

    /**
     * This is largely used for testing purposes, except:
     * - FullTextIndexServiceImpl#trackChange() uses it to insert full text index changes
     * - ExternalDataServiceImpl#loadTableFromRowReader() uses it to insert rows
     * 
     * Because code paths which call this method are manually generating 
     * the rows, this also ensures any identity values generated by sequence
     * are also populated correctly. But is is a separate step. 
     */
    @Override 
    public void writeNewRow(Session session, NewRow row) {
        RowData rowData = null;
        fillIdentityColumn(session, row);
        try {
             rowData = row.toRowData();
        } catch (EncodingException e) {
            throw new TableDefinitionMismatchException(e);
        }
        writeRow(session, rowData, null, null);
    }
    
    @Override
    public void writeRow(Session session, RowData rowData) {
        writeRow(session, rowData, null, null);
    }

    @Override
    public void writeRow(Session session, RowData rowData, TableIndex[] tableIndexes,
                         Collection<GroupIndex> groupIndexes) {
        RowDef rowDef = getGlobalRowDef(session, rowData);
        writeRow(session, rowDef, rowData, tableIndexes, groupIndexes);
    }

    @Override
    public void writeRow(Session session, RowDef rowDef, RowData rowData, TableIndex[] tableIndexes,
                         Collection<GroupIndex> groupIndexes) {
        Table table = rowDef.table();
        trackTableWrite(session, table);
        constraintHandler.handleInsert(session, table, rowData);
        onlineHelper.handleInsert(session, table, rowData);
        writeRow(session, rowDef, rowData, tableIndexes, null, true);
        WRITE_ROW_GI_TAP.in();
        try {
            maintainGroupIndexes(session,
                                 table,
                                 groupIndexes,
                                 rowData, null,
                                 StoreGIHandler.forTable(this, session, table),
                                 StoreGIHandler.Action.STORE);
        } finally {
            WRITE_ROW_GI_TAP.out();
        }
    }

    @Override
    public void deleteRow(Session session, RowData rowData, boolean cascadeDelete) {
        RowDef rowDef = getGlobalRowDef(session, rowData);
        deleteRow(session, rowDef, rowData, cascadeDelete);
    }

    @Override
    public void deleteRow(Session session, RowDef rowDef, RowData rowData, boolean cascadeDelete) {
        Table table = rowDef.table();
        trackTableWrite(session, table);
        constraintHandler.handleDelete(session, table, rowData);
        onlineHelper.handleDelete(session, table, rowData);
        DELETE_ROW_GI_TAP.in();
        try {
            if(cascadeDelete) {
                cascadeDeleteMaintainGroupIndex(session, table, rowData);
            } else { // one row, one update to group indexes
                maintainGroupIndexes(session,
                                     table,
                                     table.getGroupIndexes(),
                                     rowData,
                                     null,
                                     StoreGIHandler.forTable(this, session, table),
                                     StoreGIHandler.Action.DELETE);
            }
        } finally {
            DELETE_ROW_GI_TAP.out();
        }
        deleteRow(session, rowDef, rowData, cascadeDelete, null, true);
    }


    @Override
    public void updateRow(Session session, RowData oldRow, RowData newRow, ColumnSelector selector) {
        RowDef rowDef = getGlobalRowDef(session, oldRow);
        updateRow(session, rowDef, oldRow, rowDef, newRow, selector);
    }

    @Override
    public void updateRow(Session session, RowDef oldRowDef, RowData oldRow, RowDef newRowDef, RowData newRow, ColumnSelector selector) {
        Table table = oldRowDef.table();
        trackTableWrite(session, table);
        // Note: selector is only used by the MySQL adapter, which does not have any
        // constraints on this side; newRow will be complete when there are any.
        // Similarly, all cases where newRowDef is not the same as oldRowDef should
        // be disallowed when there are constraints present.
        assert (((selector == null) && (oldRowDef == newRowDef)) ||
                table.getForeignKeys().isEmpty())
            : table;
        constraintHandler.handleUpdatePre(session, table, oldRow, newRow);
        onlineHelper.handleUpdatePre(session, table, oldRow, newRow);
        if(canSkipGIMaintenance(table)) {
            updateRow(session, oldRowDef, oldRow, newRowDef, newRow, selector, true);
        } else {
            UPDATE_ROW_GI_TAP.in();
            try {
                RowData mergedRow = mergeRows(oldRowDef, oldRow, newRowDef, newRow, selector);
                BitSet changedColumnPositions = changedColumnPositions(oldRowDef, oldRow, newRowDef, mergedRow);
                Collection<GroupIndex> groupIndexes = table.getGroupIndexes();
                maintainGroupIndexes(session,
                                     table,
                                     groupIndexes,
                                     oldRow,
                                     changedColumnPositions,
                                     StoreGIHandler.forTable(this, session, table),
                                     StoreGIHandler.Action.DELETE);

                updateRow(session, oldRowDef, oldRow, newRowDef, mergedRow, null /*already merged*/, true);

                maintainGroupIndexes(session,
                                     table,
                                     groupIndexes,
                                     mergedRow,
                                     changedColumnPositions,
                                     StoreGIHandler.forTable(this, session, table),
                                     StoreGIHandler.Action.STORE);
            } finally {
                UPDATE_ROW_GI_TAP.out();
            }
        }
        constraintHandler.handleUpdatePost(session, table, oldRow, newRow);
        onlineHelper.handleUpdatePost(session, table, oldRow, newRow);
    }

    @Override
    public void writeIndexRows(Session session, Table table, RowData rowData, Collection<GroupIndex> indexes) {
        assert (table.getTableId() == rowData.getRowDefId());
        maintainGroupIndexes(session,
                             table,
                             indexes,
                             rowData,
                             null,
                             StoreGIHandler.forTable(this, session, table),
                             StoreGIHandler.Action.STORE);
    }

    @Override
    public void deleteIndexRows(Session session, Table table, RowData rowData, Collection<GroupIndex> indexes) {
        assert (table.getTableId() == rowData.getRowDefId());
        maintainGroupIndexes(session,
                             table,
                             indexes,
                             rowData,
                             null,
                             StoreGIHandler.forTable(this, session, table),
                             StoreGIHandler.Action.DELETE);
    }

    @Override
    public RowCollector newRowCollector(Session session,
                                        int scanFlags,
                                        int rowDefId,
                                        int indexId,
                                        byte[] columnBitMap,
                                        RowData start,
                                        ColumnSelector startColumns,
                                        RowData end,
                                        ColumnSelector endColumns,
                                        ScanLimit scanLimit)
    {
        NEW_COLLECTOR_TAP.in();
        RowCollector rc;
        try {
            if(start != null && startColumns == null) {
                startColumns = createNonNullFieldSelector(start);
            }
            if(end != null && endColumns == null) {
                endColumns = createNonNullFieldSelector(end);
            }
            RowDef rowDef = checkRequest(session, rowDefId, start, startColumns, end, endColumns);
            rc = OperatorBasedRowCollector.newCollector(session,
                                                        this,
                                                        scanFlags,
                                                        rowDef,
                                                        indexId,
                                                        columnBitMap,
                                                        start,
                                                        startColumns,
                                                        end,
                                                        endColumns,
                                                        scanLimit);
        } finally {
            NEW_COLLECTOR_TAP.out();
        }
        return rc;
    }

    @Override
    public void addSavedRowCollector(final Session session,
                                     final RowCollector rc) {
        final Integer tableId = rc.getTableId();
        final List<RowCollector> list = collectorsForTableId(session, tableId);
        if (!list.isEmpty()) {
            LOG.debug("Note: Nested RowCollector on tableId={} depth={}", tableId, list.size() + 1);
            assert list.get(list.size() - 1) != rc : "Redundant call";
            //
            // This disallows the patch because we agreed not to fix the
            // bug. However, these changes fix a memory leak, which is
            // important for robustness.
            //
            // throw new StoreException(122, "Bug 255 workaround is disabled");
        }
        list.add(rc);
    }

    @Override
    public RowCollector getSavedRowCollector(final Session session,
                                             final int tableId) throws CursorIsUnknownException {
        final List<RowCollector> list = collectorsForTableId(session, tableId);
        if (list.isEmpty()) {
            LOG.debug("Nested RowCollector on tableId={} depth={}", tableId, (list.size() + 1));
            throw new CursorIsUnknownException(tableId);
        }
        return list.get(list.size() - 1);
    }

    @Override
    public void removeSavedRowCollector(final Session session,
                                        final RowCollector rc) throws CursorIsUnknownException {
        final Integer tableId = rc.getTableId();
        final List<RowCollector> list = collectorsForTableId(session, tableId);
        if (list.isEmpty()) {
            throw new CursorIsUnknownException (tableId);
        }
        final RowCollector removed = list.remove(list.size() - 1);
        if (removed != rc) {
            throw new CursorCloseBadException(tableId);
        }
    }

    @Override
    @Deprecated
    public long getRowCount(Session session, boolean exact, RowData start, RowData end, byte[] columnBitMap) {
        // TODO: Compute a reasonable value. The value 2 is special because it is not 0 or 1 but will
        // still induce MySQL to use an index rather than a full table scan.
        return 2;
    }

    @Override
    public void dropGroup(final Session session, Group group) {
        group.getRoot().visit(new AbstractVisitor() {
            @Override
            public void visit(Table table) {
                removeTrees(session, table);
            }
        });
    }

    @Override
    public void dropSchema(Session session, Schema schema) {
        removeTrees(session, schema);
    }

    @Override
    public void dropNonSystemSchemas(Session session, Collection<Schema> allSchemas) {
        for (Schema schema : allSchemas) {
            if (!TableName.inSystemSchema(schema.getName())) {
                dropSchema(session, schema);
            }
        }
    }

    @Override
    public void truncateGroup(final Session session, final Group group) {
        group.getRoot().visit(new AbstractVisitor() {
            @Override
            public void visit(Table table) {
                // Foreign keys
                constraintHandler.handleTruncate(session, table);
                onlineHelper.handleTruncate(session, table);
                // Table indexes
                truncateIndexes(session, table.getIndexesIncludingInternal());
                // Table statuses
                table.rowDef().getTableStatus().truncate(session);
            }
        });
        // Group indexes
        truncateIndexes(session, group.getIndexes());
        // Group tree
        truncateTree(session, group);
    }

    @Override
    public void truncateTableStatus(final Session session, final int rowDefId) {
        Table table = getAIS(session).getTable(rowDefId);
        table.rowDef().getTableStatus().truncate(session);
    }

    @Override
    public void deleteIndexes(final Session session, final Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            removeTree(session, index);
        }
    }

    @Override
    public void removeTrees(Session session, Schema schema) {
        for (Sequence sequence : schema.getSequences().values()) {
            removeTree(session, sequence);
        }
        for (Table table : schema.getTables().values()) {
            removeTrees(session, table);
        }
    }

    @Override
    public void removeTrees(Session session, Table table) {
        // Table indexes
        for(Index index : table.getIndexesIncludingInternal()) {
            removeTree(session, index);
        }
        // Group indexes
        for(Index index : table.getGroupIndexes()) {
            removeTree(session, index);
        }
        // Sequence
        if(table.getIdentityColumn() != null) {
            deleteSequences(session, Collections.singleton(table.getIdentityColumn().getIdentityGenerator()));
        }
        // And the group tree
        removeTree(session, table.getGroup());
    }

    @Override
    public void removeTrees(Session session, Collection<? extends HasStorage> objects) {
        for(HasStorage object : objects) {
            removeTree(session, object);
        }
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    /** Pack row data according to storage format. */
    @SuppressWarnings("unchecked")
    public void packRowData(Session session, SDType storeData, RowData rowData) {
        getStorageDescription(storeData).packRowData((SType)this, session, storeData, rowData);
    }

    /** Expand row data according to storage format. */
    @SuppressWarnings("unchecked")
    public void expandRowData(Session session, SDType storeData, RowData rowData) {
        getStorageDescription(storeData).expandRowData((SType)this, session, storeData, rowData);
    }

    public OnlineHelper getOnlineHelper() {
        return onlineHelper;
    }

    //
    // Internal
    //

    private void writeRowInternal(Session session,
                                  SDType storeData,
                                  RowDef rowDef,
                                  RowData rowData,
                                  TableIndex[] indexes,
                                  BitSet tablesRequiringHKeyMaintenance,
                                  boolean propagateHKeyChanges) {
        Key hKey = getKey(session, storeData);
        /*
         * About propagateHKeyChanges as second to last argument to constructHKey (i.e. insertingRow):
         * - If true, we're inserting a row and may need to generate a PK (for no-pk tables)
         * - If this invocation is being done as part of hKey maintenance (propagate = false),
         *   then we are deleting and reinserting a row, and we don't want the PK value changed.
         * - See bug 1020342.
         */
        constructHKey(session, rowDef, rowData, hKey);
        /*
         * Don't check hKey uniqueness here. It would require a database access and we're not in
         * in a good position to report a meaningful uniqueness violation (e.g. on the PK).
         * Instead, rely on PK validation when indexes are maintained.
         */
        packRowData(session, storeData, rowData);
        store(session, storeData);
        if(rowDef.isAutoIncrement()) {
            final long location = rowDef.fieldLocation(rowData, rowDef.getAutoIncrementField());
            if(location != 0) {
                long autoIncrementValue = rowData.getIntegerValue((int) location, (int) (location >>> 32));
                rowDef.getTableStatus().setAutoIncrement(session, autoIncrementValue);
            }
        }

        boolean bumpCount = false;
        PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(this);
        for(TableIndex index : indexes) {
            long zValue = -1;
            SpatialColumnHandler spatialColumnHandler = null;
            if (index.isSpatial()) {
                spatialColumnHandler = new SpatialColumnHandler(index);
                zValue = spatialColumnHandler.zValue(rowData);
            }
            writeIndexRow(session, index, rowData, hKey, indexRow, spatialColumnHandler, zValue, false);
            // Only bump row count if PK row is written (may not be written during an ALTER)
            // Bump row count *after* uniqueness checks. Avoids drift of TableStatus#getApproximateRowCount. See bug1112940.
            bumpCount |= index.isPrimaryKey();
        }

        if(bumpCount) {
            rowDef.getTableStatus().rowsWritten(session, 1);
        }

        for(RowListener listener : listenerService.getRowListeners()) {
            listener.onInsertPost(session, rowDef.table(), hKey, rowData);
        }

        if(propagateHKeyChanges && rowDef.table().hasChildren()) {
            /*
             * Row being inserted might be the parent of orphan rows already present.
             * The hKeys of these orphan rows need to be maintained. The ones of interest
             * contain the PK from the inserted row, and nulls for other hKey fields nearer the root.
             */
            hKey.clear();
            Table table = rowDef.table();
            PersistitKeyAppender hKeyAppender = PersistitKeyAppender.create(hKey, table.getName());
            List<Column> pkColumns = table.getPrimaryKeyIncludingInternal().getColumns();
            for(HKeySegment segment : table.hKey().segments()) {
                RowDef segmentRowDef = segment.table().rowDef();
                hKey.append(segmentRowDef.table().getOrdinal());
                for(HKeyColumn hKeyColumn : segment.columns()) {
                    Column column = hKeyColumn.column();
                    if(pkColumns.contains(column)) {
                        RowDef columnTableRowDef = column.getTable().rowDef();
                        hKeyAppender.append(columnTableRowDef.getFieldDef(column.getPosition()), rowData);
                    } else {
                        hKey.append(null);
                    }
                }
            }
            propagateDownGroup(session, rowDef.table().getAIS(), storeData, tablesRequiringHKeyMaintenance, indexRow, false);
        }
    }

    protected void deleteRowInternal(Session session,
                                     SDType storeData,
                                     RowDef rowDef,
                                     RowData rowData,
                                     boolean cascadeDelete,
                                     BitSet tablesRequiringHKeyMaintenance,
                                     boolean propagateHKeyChanges)
    {
        Key hKey = getKey(session, storeData);
        constructHKey(session, rowDef, rowData, hKey);

        boolean existed = fetch(session, storeData);
        if(!existed) {
            throw new NoSuchRowException(hKey);
        }

        for(RowListener listener : listenerService.getRowListeners()) {
            listener.onDeletePre(session, rowDef.table(), hKey, rowData);
        }

        // Remove all indexes (before the group row is gone in-case listener needs it)
        PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(this);
        for(TableIndex index : rowDef.getIndexes()) {
            long zValue = -1;
            SpatialColumnHandler spatialColumnHandler = null;
            if (index.isSpatial()) {
                spatialColumnHandler = new SpatialColumnHandler(index);
                zValue = spatialColumnHandler.zValue(rowData);
            }
            deleteIndexRow(session, index, rowData, hKey, indexRow, spatialColumnHandler, zValue, false);
        }

        // Remove the group row
        clear(session, storeData);
        rowDef.getTableStatus().rowDeleted(session);

        // Maintain hKeys of any existing descendants (i.e. create orphans)
        if(propagateHKeyChanges && rowDef.table().hasChildren()) {
            propagateDownGroup(session, rowDef.table().getAIS(), storeData, tablesRequiringHKeyMaintenance, indexRow, cascadeDelete);
        }
    }

    private void updateRowInternal(Session session,
                                   SDType storeData,
                                   RowDef oldRowDef,
                                   RowData oldRow,
                                   RowDef newRowDef,
                                   RowData newRow,
                                   ColumnSelector selector,
                                   boolean propagateHKeyChanges)
    {
        Key hKey = getKey(session, storeData);
        constructHKey(session, oldRowDef, oldRow, hKey);

        boolean existed = fetch(session, storeData);
        if(!existed) {
            throw new NoSuchRowException(hKey);
        }

        RowData currentRow = new RowData();
        expandRowData(session, storeData, currentRow);
        RowData mergedRow = mergeRows(oldRowDef, currentRow, newRowDef, newRow, selector);

        BitSet tablesRequiringHKeyMaintenance = null;
        
        // This occurs when doing alter table adding or dropping a column,
        // don't be tricky here, drop and insert. 
        if (!oldRowDef.equals(newRowDef)) {
            tablesRequiringHKeyMaintenance = hKeyDependentTableOrdinals(oldRowDef);
        } else if (propagateHKeyChanges) {
            tablesRequiringHKeyMaintenance = analyzeFieldChanges(oldRowDef, oldRow, newRowDef, mergedRow);
        }

        // May still be null (i.e. no pk or fk changes), check again
        if(tablesRequiringHKeyMaintenance == null) {
            packRowData(session, storeData, mergedRow);

            for(RowListener listener : listenerService.getRowListeners()) {
                listener.onUpdatePre(session, oldRowDef.table(), hKey, oldRow, mergedRow);
            }

            store(session, storeData);

            for(RowListener listener : listenerService.getRowListeners()) {
                listener.onUpdatePost(session, oldRowDef.table(), hKey, oldRow, mergedRow);
            }

            PersistitIndexRowBuffer indexRowBuffer = new PersistitIndexRowBuffer(this);
            for(TableIndex index : oldRowDef.getIndexes()) {
                updateIndex(session, index, oldRowDef, currentRow, newRowDef, mergedRow, hKey, indexRowBuffer);
            }
        } else {
            // A PK or FK field has changed. Process the update by delete and insert.
            // tablesRequiringHKeyMaintenance contains the ordinals of the tables whose hKey could have been affected.
            deleteRow(session, oldRowDef, oldRow, false, tablesRequiringHKeyMaintenance, true);
            writeRow(session, newRowDef, mergedRow, null, tablesRequiringHKeyMaintenance, true); // May throw DuplicateKeyException
        }
    }

    /**
     * <p>
     *   Propagate any change to the HKey, signified by the Key contained within <code>storeData</code>, to all
     *   existing descendants.
     * </p>
     * <p>
     *   This is required from inserts, as an existing row can be adopted, and deletes, as an existing row can be
     *   orphaned. Updates that affect HKeys are processing as an explicit delete + insert pair.
     * </p>
     * <p>
     *   Flow and explanation:
     *   <ul>
     *     <li> The hKey, from storeData, is of a row R whose replacement R' is already present. </li>
     *     <li> For each descendant D of R, this method deletes and reinserts D. </li>
     *     <li> Reinsertion of D causes its hKey to be recomputed. </li>
     *     <li> This may depend on an ancestor being updated if part of D's hKey comes from the parent's PK index.
     *          That's OK because updates are processed pre-order (i.e. ancestors before descendants). </li>
     *     <li> Descendant D of R means is below R in the group (i.e. hKey(R) is a prefix of hKey(D)). </li>
     *   </ul>
     * </p>
     *
     * @param storeData Contains HKey for a changed row R. <i>State is modified.</i>
     * @param tablesRequiringHKeyMaintenance Ordinal values eligible for modification. <code>null</code> means all.
     * @param indexRowBuffer Buffer for performing index deletions. <i>State is modified.</i>
     * @param cascadeDelete <code>true</code> if rows should <i>not</i> be re-inserted.
     */
    protected void propagateDownGroup(Session session,
                                      AkibanInformationSchema ais,
                                      SDType storeData,
                                      BitSet tablesRequiringHKeyMaintenance,
                                      PersistitIndexRowBuffer indexRowBuffer,
                                      boolean cascadeDelete)
    {
        Iterator it = createDescendantIterator(session, storeData);
        PROPAGATE_CHANGE_TAP.in();
        try {
            Key hKey = getKey(session, storeData);
            RowData rowData = new RowData();
            while(it.hasNext()) {
                it.next();
                expandRowData(session, storeData, rowData);
                Table table = ais.getTable(rowData.getRowDefId());
                assert (table != null) : rowData.getRowDefId();
                int ordinal = table.getOrdinal();
                if(tablesRequiringHKeyMaintenance == null || tablesRequiringHKeyMaintenance.get(ordinal)) {
                    PROPAGATE_REPLACE_TAP.in();
                    try {
                        for(RowListener listener : listenerService.getRowListeners()) {
                            listener.onDeletePre(session, table, hKey, rowData);
                        }
                        // Don't call deleteRow as the hKey does not need recomputed.
                        clear(session, storeData);
                        table.rowDef().getTableStatus().rowDeleted(session);
                        for(TableIndex index : table.rowDef().getIndexes()) {
                            long zValue = -1;
                            SpatialColumnHandler spatialColumnHandler = null;
                            if (index.isSpatial()) {
                                spatialColumnHandler = new SpatialColumnHandler(index);
                                zValue = spatialColumnHandler.zValue(rowData);
                            }
                            deleteIndexRow(session, index, rowData, hKey, indexRowBuffer, spatialColumnHandler, zValue, false);
                        }
                        if(!cascadeDelete) {
                            // Reinsert it, recomputing the hKey and maintaining indexes
                            RowDef rowDef = getRowDef(ais, rowData.getRowDefId());
                            writeRow(session, rowDef, rowData, null, tablesRequiringHKeyMaintenance, false);
                        }
                    } finally {
                        PROPAGATE_REPLACE_TAP.out();
                    }
                }
            }
        } finally {
            PROPAGATE_CHANGE_TAP.out();
        }
    }

    private List<RowCollector> collectorsForTableId(final Session session, final int tableId) {
        List<RowCollector> list = session.get(COLLECTORS, tableId);
        if (list == null) {
            list = new ArrayList<>();
            session.put(COLLECTORS, tableId, list);
        }
        return list;
    }

    private RowDef checkRequest(Session session, int rowDefId, RowData start, ColumnSelector startColumns,
                                RowData end, ColumnSelector endColumns) throws IllegalArgumentException {
        if (start != null) {
            if (startColumns == null) {
                throw new IllegalArgumentException("non-null start row requires non-null ColumnSelector");
            }
            if( start.getRowDefId() != rowDefId) {
                throw new IllegalArgumentException("Start and end RowData must specify the same rowDefId");
            }
        }
        if (end != null) {
            if (endColumns == null) {
                throw new IllegalArgumentException("non-null end row requires non-null ColumnSelector");
            }
            if (end.getRowDefId() != rowDefId) {
                throw new IllegalArgumentException("Start and end RowData must specify the same rowDefId");
            }
        }
        final Table table = getAIS(session).getTable(rowDefId);
        if(table == null) {
            throw new RowDefNotFoundException(rowDefId);
        }
        assert (table.rowDef() != null) : table;
        return table.rowDef();
    }

    private void updateIndex(Session session,
                             TableIndex index,
                             RowDef oldRowDef,
                             RowData oldRow,
                             RowDef newRowDef,
                             RowData newRow,
                             Key hKey,
                             PersistitIndexRowBuffer indexRowBuffer) {
        int nkeys = index.getKeyColumns().size();
        IndexRowComposition indexRowComposition = index.indexRowComposition();
        if(!fieldsEqual(oldRowDef, oldRow, newRowDef, newRow, nkeys, indexRowComposition)) {
            UPDATE_INDEX_TAP.in();
            try {
                long oldZValue = -1;
                long newZValue = -1;
                SpatialColumnHandler spatialColumnHandler = null;
                if (index.isSpatial()) {
                    spatialColumnHandler = new SpatialColumnHandler(index);
                    oldZValue = spatialColumnHandler.zValue(oldRow);
                    newZValue = spatialColumnHandler.zValue(newRow);
                }
                deleteIndexRow(session, index, oldRow, hKey, indexRowBuffer, spatialColumnHandler, oldZValue, false);
                writeIndexRow(session, index, newRow, hKey, indexRowBuffer, spatialColumnHandler, newZValue, false);
            } finally {
                UPDATE_INDEX_TAP.out();
            }
        }
    }

    private void maintainGroupIndexes(Session session,
                                      Table table,
                                      Collection<GroupIndex> groupIndexes,
                                      RowData rowData,
                                      BitSet columnDifferences,
                                      StoreGIHandler handler,
                                      StoreGIHandler.Action action) {
        if(canSkipGIMaintenance(table)) {
            return;
        }
        if(groupIndexes == null) {
            groupIndexes = table.getGroupIndexes();
        }
        SDType storeData = createStoreData(session, table.getGroup());
        try {
            // the "false" at the end of constructHKey toggles whether the RowData should be modified to increment
            // the hidden PK field, if there is one. For PK-less rows, this field have already been incremented by now,
            // so we don't want to increment it again
            Key hKey = getKey(session, storeData);
            constructHKey(session, table.rowDef(), rowData, hKey);

            PersistitHKey persistitHKey = new PersistitHKey(createKey(), table.hKey());
            persistitHKey.copyFrom(hKey);

            for(GroupIndex groupIndex : groupIndexes) {
                if(columnDifferences == null || groupIndex.columnsOverlap(table, columnDifferences)) {
                    StoreAdapter adapter = createAdapter(session, SchemaCache.globalSchema(table.getAIS()));
                    StoreGIMaintenance plan = StoreGIMaintenancePlans
                            .forAis(table.getAIS())
                            .forRowType(groupIndex, adapter.schema().tableRowType(table));
                    plan.run(action, persistitHKey, rowData, adapter, handler);
                } else {
                    SKIP_GI_MAINTENANCE.hit();
                }
            }
        } finally {
            releaseStoreData(session, storeData);
        }
    }

    /*
     * This does the full cascading delete, updating both the group indexes for
     * each table affected and removing the rows.
     * It does this root to leaf order.
     */
    private void cascadeDeleteMaintainGroupIndex(Session session,
                                                 Table table,
                                                 RowData rowData)
    {
        Operator plan = PlanGenerator.generateBranchPlan(table.getAIS(), table);
        StoreAdapter adapter = createAdapter(session, SchemaCache.globalSchema(table.getAIS()));
        QueryContext queryContext = new SimpleQueryContext(adapter);
        QueryBindings queryBindings = queryContext.createBindings();
        Cursor cursor = API.cursor(plan, queryContext, queryBindings);

        List<Column> lookupCols = table.getPrimaryKeyIncludingInternal().getColumns();
        RowDataValueSource pSource = new RowDataValueSource();
        for(int i = 0; i < lookupCols.size(); ++i) {
            Column col = lookupCols.get(i);
            pSource.bind(col.getFieldDef(), rowData);
            queryBindings.setValue(i, pSource);
        }
        try {
            Row row;
            cursor.openTopLevel();
            while((row = cursor.next()) != null) {
                Table aTable = row.rowType().table();
                RowData data = adapter.rowData(aTable.rowDef(), row, new RowDataCreator());
                maintainGroupIndexes(session,
                                     aTable,
                                     aTable.getGroupIndexes(),
                                     data,
                                     null,
                                     StoreGIHandler.forTable(this, session, table),
                                     StoreGIHandler.Action.CASCADE);
            }
        } finally {
            cursor.closeTopLevel();
        }
    }

    /** Be very careful using this, most methods should take it explicitly and pass it down. */
    private RowDef getGlobalRowDef(Session session, RowData rowData) {
        AkibanInformationSchema ais = getAIS(session);
        return getRowDef(ais, rowData.getRowDefId());
    }


    //
    // Static helpers
    //

    protected static boolean bytesEqual(byte[] a, int aOffset, int aSize, byte[] b, int bOffset, int bSize) {
        if(aSize != bSize) {
            return false;
        }
        for(int i = 0; i < aSize; i++) {
            if(a[i + aOffset] != b[i + bOffset]) {
                return false;
            }
        }
        return true;
    }

    protected static boolean fieldsEqual(RowDef aDef,
                                         RowData a,
                                         RowDef bDef,
                                         RowData b,
                                         int nkeys,
                                         IndexRowComposition indexRowComposition) {
        for (int i = 0; i < nkeys; i++) {
            int fieldIndex = indexRowComposition.getFieldPosition(i);
            if(!fieldEqual(aDef, a, bDef, b, fieldIndex)) {
                return false;
            }
        }
        return true;
    }
    
    protected static boolean fieldEqual(RowDef aRowDef, RowData a, RowDef bRowDef, RowData b, int fieldPosition) {
        long aLoc = aRowDef.fieldLocation(a, fieldPosition);
        long bLoc = bRowDef.fieldLocation(b, fieldPosition);
        return bytesEqual(a.getBytes(), (int)aLoc, (int)(aLoc >>> 32),
                          b.getBytes(), (int)bLoc, (int)(bLoc >>> 32));
    }

    private static BitSet changedColumnPositions(RowDef aDef, RowData a, RowDef bDef, RowData b) {
        int fields = aDef.getFieldCount();
        BitSet differences = new BitSet(fields);
        for(int f = 0; f < fields; f++) {
            differences.set(f, !fieldEqual(aDef, a, bDef, b, f));
        }
        return differences;
    }

    protected static RowData mergeRows(RowDef oldRowDef, RowData currentRow, RowDef newRowDef, RowData newRowData, ColumnSelector selector) {
        if(selector == null || selector == ConstantColumnSelector.ALL_ON) {
            return newRowData;
        }
        NewRow mergedRow = NiceRow.fromRowData(currentRow, oldRowDef);
        NewRow newRow = new LegacyRowWrapper(newRowDef, newRowData);
        int fields = oldRowDef.getFieldCount();
        for(int i = 0; i < fields; i++) {
            if(selector.includesColumn(i)) {
                mergedRow.put(i, newRow.get(i));
            }
        }
        return mergedRow.toRowData();
    }

    private static boolean canSkipGIMaintenance(Table table) {
        return table.getGroupIndexes().isEmpty();
    }

    private static RowDef getRowDef(AkibanInformationSchema ais, int tableID) {
        Table table = ais.getTable(tableID);
        assert (table != null) : tableID;
        return table.rowDef();
    }

    private static ColumnSelector createNonNullFieldSelector(final RowData rowData) {
        assert rowData != null;
        return new ColumnSelector() {
            @Override
            public boolean includesColumn(int columnPosition) {
                return !rowData.isNull(columnPosition);
            }
        };
    }

    protected void fillIdentityColumn(Session session, NewRow row) {
        Table table = row.getRowDef().table();
        Column idColumn = table.getIdentityColumn();
        if (idColumn != null) {
            FieldDef fieldDef = idColumn.getFieldDef();
            Boolean defaultIdentity = idColumn.getDefaultIdentity();
            if (defaultIdentity == false ||
                    (defaultIdentity == true && row.isColumnNull(fieldDef.getFieldIndex()))) {
                Sequence sequence = idColumn.getIdentityGenerator();
                Long value = this.nextSequenceValue(session, sequence);
                row.put(fieldDef.getFieldIndex(), value);
            }
        }
    }
}
