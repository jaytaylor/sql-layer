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

package com.akiban.server.store;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.OperatorBasedRowCollector;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.akiban.server.TableStatistics;
import com.akiban.server.TableStatus;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.error.CursorCloseBadException;
import com.akiban.server.error.CursorIsUnknownException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.error.QueryTimedOutException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.error.TableChangedByDDLException;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.store.statistics.Histogram;
import com.akiban.server.store.statistics.HistogramEntry;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import com.persistit.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractStore<StoreType,ExceptionType extends Exception> implements Store {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStore.class.getName());

    private static final InOutTap WRITE_ROW_TAP = Tap.createTimer("write: write_row");
    private static final InOutTap NEW_COLLECTOR_TAP = Tap.createTimer("read: new_collector");
    private static final Session.MapKey<Integer,List<RowCollector>> COLLECTORS = Session.MapKey.mapNamed("collectors");

    protected final static int MAX_ROW_SIZE = 5000000;

    protected final LockService lockService;
    protected final SchemaManager schemaManager;
    protected IndexStatisticsService indexStatisticsService;


    protected AbstractStore(LockService lockService, SchemaManager schemaManager) {
        this.lockService = lockService;
        this.schemaManager = schemaManager;
    }


    //
    // AbstractStore
    //

    protected abstract StoreType createStoreType(Session session, TreeLink treeLink);

    protected abstract void releaseStoreType(Session session, StoreType storeType);

    protected abstract PersistitIndexRowBuffer readPKIndexRow(Session session,
                                                              Index pkIndex,
                                                              StoreType storeData,
                                                              RowDef rowDef,
                                                              RowData rowData);

    protected abstract void insertIntoIndex(Session session,
                                            Index index,
                                            RowData rowData,
                                            Key hKey,
                                            PersistitIndexRowBuffer indexRow);

    // TODO: These can be protected after OperatorStore refactoring

    public void constructHKey(Session session,
                              RowDef rowDef,
                              RowData rowData,
                              boolean insertingRow,
                              Key hKeyOut) {
        constructHKey(session, rowDef, rowData, insertingRow, null, hKeyOut);
    }

    public void constructHKey(Session session,
                              RowDef rowDef,
                              RowData rowData,
                              boolean insertingRow,
                              AtomicLong hiddenPk,
                              Key hKeyOut) {
        // Initialize the HKey being constructed
        PersistitKeyAppender hKeyAppender = PersistitKeyAppender.create(hKeyOut);
        hKeyAppender.key().clear();

        // Metadata for the row's table
        UserTable table = rowDef.userTable();
        FieldDef[] fieldDefs = rowDef.getFieldDefs();

        // Only set if parent row is looked up
        int i2hPosition = 0;
        IndexToHKey indexToHKey = null;
        StoreType parentStoreData = null;
        PersistitIndexRowBuffer parentPKIndexRow = null;

        // All columns of all segments of the HKey
        for(HKeySegment hKeySegment : table.hKey().segments()) {
            // Ordinal for this segment
            RowDef segmentRowDef = hKeySegment.table().rowDef();
            hKeyAppender.append(segmentRowDef.table().getOrdinal());
            // Segment's columns
            for(HKeyColumn hKeyColumn : hKeySegment.columns()) {
                UserTable hKeyColumnTable = hKeyColumn.column().getUserTable();
                if(hKeyColumnTable != table) {
                    // HKey column from row of parent table
                    if (parentStoreData == null) {
                        // Initialize parent metadata and state
                        RowDef parentRowDef = rowDef.getParentRowDef();
                        TableIndex parentPkIndex = parentRowDef.getPKIndex();
                        indexToHKey = parentPkIndex.indexToHKey();
                        parentStoreData = createStoreType(session, parentPkIndex.indexDef());
                        parentPKIndexRow = readPKIndexRow(session, parentPkIndex, parentStoreData, rowDef, rowData);
                    }
                    if(indexToHKey.isOrdinal(i2hPosition)) {
                        assert indexToHKey.getOrdinal(i2hPosition) == segmentRowDef.userTable().getOrdinal() : hKeyColumn;
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
                    FieldDef fieldDef = fieldDefs[column.getPosition()];
                    if(insertingRow && column.isAkibanPKColumn()) {
                        // Must be a PK-less table. Use unique id from TableStatus.
                        final long uniqueId;
                        if(hiddenPk == null) {
                            uniqueId = segmentRowDef.getTableStatus().createNewUniqueID(session);
                        } else {
                            uniqueId = hiddenPk.incrementAndGet();
                        }
                        hKeyAppender.append(uniqueId);
                        // Write rowId into the value part of the row also.
                        rowData.updateNonNullLong(fieldDef, uniqueId);
                    } else {
                        hKeyAppender.append(fieldDef, rowData);
                    }
                }
            }
        }
        if(parentStoreData != null) {
            releaseStoreType(session, parentStoreData);
        }
    }

    protected RowDef rowDefFromExplicitOrId(Session session, RowData rowData) {
        RowDef rowDef = rowData.getExplicitRowDef();
        if(rowDef == null) {
            rowDef = getRowDef(session, rowData.getRowDefId());
        }
        return rowDef;
    }

    protected boolean hasNullIndexSegments(RowData rowData, Index index)
    {
        IndexDef indexDef = index.indexDef();
        assert indexDef.getRowDef().getRowDefId() == rowData.getRowDefId();
        for (int i : indexDef.getFields()) {
            if (rowData.isNull(i)) {
                return true;
            }
        }
        return false;
    }

    /** Convert from new-format histogram to old for adapter. */
    protected TableStatistics.Histogram indexStatisticsToHistogram(Session session, Index index, Key key) {
        IndexStatistics stats = indexStatisticsService.getIndexStatistics(session, index);
        if (stats == null) {
            return null;
        }
        Histogram fromHistogram = stats.getHistogram(0, index.getKeyColumns().size());
        if (fromHistogram == null) {
            return null;
        }
        IndexDef indexDef = index.indexDef();
        RowDef indexRowDef = indexDef.getRowDef();
        TableStatistics.Histogram toHistogram = new TableStatistics.Histogram(index.getIndexId());
        RowData indexRowData = new RowData(new byte[4096]);
        Object[] indexValues = new Object[indexRowDef.getFieldCount()];
        long count = 0;
        for (HistogramEntry entry : fromHistogram.getEntries()) {
            // Decode the key.
            int keylen = entry.getKeyBytes().length;
            System.arraycopy(entry.getKeyBytes(), 0, key.getEncodedBytes(), 0, keylen);
            key.setEncodedSize(keylen);
            key.indexTo(0);
            int depth = key.getDepth();
            // Copy key fields to index row.
            for (int field : indexDef.getFields()) {
                if (--depth >= 0) {
                    indexValues[field] = key.decode();
                } else {
                    indexValues[field] = null;
                }
            }
            indexRowData.createRow(indexRowDef, indexValues);
            // Partial counts to running total less than key.
            count += entry.getLessCount();
            toHistogram.addSample(new TableStatistics.HistogramSample(indexRowData.copy(), count));
            count += entry.getEqualCount();
        }
        // Add final entry with all nulls.
        Arrays.fill(indexValues, null);
        indexRowData.createRow(indexRowDef, indexValues);
        toHistogram.addSample(new TableStatistics.HistogramSample(indexRowData.copy(), count));
        return toHistogram;
    }

    protected static boolean bytesEqual(byte[] a, int aoffset, int asize, byte[] b, int boffset, int bsize) {
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

    protected static boolean fieldsEqual(RowDef rowDef, RowData a, RowData b, int[] fieldIndexes)
    {
        for (int fieldIndex : fieldIndexes) {
            long aloc = rowDef.fieldLocation(a, fieldIndex);
            long bloc = rowDef.fieldLocation(b, fieldIndex);
            if (!bytesEqual(a.getBytes(), (int) aloc, (int) (aloc >>> 32),
                            b.getBytes(), (int) bloc, (int) (bloc >>> 32))) {
                return false;
            }
        }
        return true;
    }

    protected static boolean fieldEqual(RowDef rowDef, RowData a, RowData b, int fieldPosition)
    {
        long aloc = rowDef.fieldLocation(a, fieldPosition);
        long bloc = rowDef.fieldLocation(b, fieldPosition);
        return bytesEqual(a.getBytes(), (int) aloc, (int) (aloc >>> 32),
                          b.getBytes(), (int) bloc, (int) (bloc >>> 32));
    }

    protected BitSet analyzeFieldChanges(Session session, RowDef rowDef, RowData oldRow, RowData newRow)
    {
        BitSet tablesRequiringHKeyMaintenance;
        assert oldRow.getRowDefId() == newRow.getRowDefId();
        int fields = rowDef.getFieldCount();
        // Find the PK and FK fields
        BitSet keyField = new BitSet(fields);
        for (int pkFieldPosition : rowDef.getPKIndex().indexDef().getFields()) {
            keyField.set(pkFieldPosition, true);
        }
        for (int fkFieldPosition : rowDef.getParentJoinFields()) {
            keyField.set(fkFieldPosition, true);
        }
        // Find whether and where key fields differ
        boolean allEqual = true;
        for (int keyFieldPosition = keyField.nextSetBit(0);
             allEqual && keyFieldPosition >= 0;
             keyFieldPosition = keyField.nextSetBit(keyFieldPosition + 1)) {
            boolean fieldEqual = fieldEqual(rowDef, oldRow, newRow, keyFieldPosition);
            if (!fieldEqual) {
                allEqual = false;
            }
        }
        if (allEqual) {
            tablesRequiringHKeyMaintenance = null;
        } else {
            // A PK or FK field has changed, so the update has to be done as delete/insert. To minimize hkey
            // propagation work, find which tables (descendents of the updated table) are affected by hkey
            // changes.
            tablesRequiringHKeyMaintenance = hKeyDependentTableOrdinals(session, oldRow.getRowDefId());
        }
        return tablesRequiringHKeyMaintenance;
    }

    private BitSet hKeyDependentTableOrdinals(Session session, int rowDefId)
    {
        RowDef rowDef = getRowDef(session, rowDefId);
        UserTable table = rowDef.userTable();
        BitSet ordinals = new BitSet();
        for (UserTable hKeyDependentTable : table.hKeyDependentTables()) {
            int ordinal = hKeyDependentTable.getOrdinal();
            ordinals.set(ordinal, true);
        }
        return ordinals;
    }

    protected RowDef writeRowCheck(Session session, RowData rowData) {
        if (rowData.getRowSize() > MAX_ROW_SIZE) {
            LOG.warn("RowData size {} is larger than current limit of {} bytes" + rowData.getRowSize(), MAX_ROW_SIZE);
        }
        return writeCheck(session, rowData);
    }

    protected RowDef writeCheck(Session session, RowData rowData) {
        final RowDef rowDef = rowDefFromExplicitOrId(session, rowData);
        //checkNoGroupIndexes(rowDef.table());
        lockAndCheckVersion(session, rowDef);
        return rowDef;
    }


    //
    // Store methods
    //

    @Override
    public AkibanInformationSchema getAIS(Session session) {
        return schemaManager.getAis(session);
    }

    @Override
    public RowDef getRowDef(Session session, int rowDefID) {
        Table table = getAIS(session).getUserTable(rowDefID);
        if(table == null) {
            throw new RowDefNotFoundException(rowDefID);
        }
        return table.rowDef();
    }

    @Override
    public RowDef getRowDef(Session session, TableName tableName) {
        Table table = getAIS(session).getTable(tableName);
        if(table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table.rowDef();
    }

    @Override
    public void writeRow(Session session, RowData rowData) {
        writeRow(session, rowData, null, true);
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

    // This is to avoid circular dependencies in Guicer.
    // TODO: There is still a functional circularity: store needs
    // stats to clear them when deleting a group; stats need store to
    // persist the stats. It would be better to separate out the
    // higher level store functions from what other services require.
    @Override
    public void setIndexStatistics(IndexStatisticsService indexStatisticsService) {
        this.indexStatisticsService = indexStatisticsService;
    }

    @Override
    public TableStatistics getTableStatistics(final Session session, int tableId) {
        final RowDef rowDef = getRowDef(session, tableId);
        final TableStatistics ts = new TableStatistics(tableId);
        final TableStatus status = rowDef.getTableStatus();
        ts.setAutoIncrementValue(status.getAutoIncrement(session));
        ts.setRowCount(status.getRowCount(session));
        // TODO - get correct values
        ts.setMeanRecordLength(100);
        ts.setBlockSize(8192);
        for(Index index : rowDef.getIndexes()) {
            if(index.isSpatial()) {
                continue;
            }
            TableStatistics.Histogram histogram = indexStatisticsToHistogram(session, index, createKey());
            if(histogram != null) {
                ts.addHistogram(histogram);
            }
        }
        return ts;
    }

    @Override
    public long getRowCount(Session session, boolean exact, RowData start, RowData end, byte[] columnBitMap) {
        // TODO: Compute a reasonable value. The value 2 is special because it is not 0 or 1 but will
        // still induce MySQL to use an index rather than a full table scan.
        return 2;
    }

    @Override
    public void dropGroup(final Session session, Group group) {
        group.getRoot().traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
                removeTrees(session, table);
            }
        });
    }

    @Override
    public void truncateGroup(final Session session, final Group group) {
        final List<Index> indexes = new ArrayList<>();
        // Collect indexes, truncate table statuses
        group.getRoot().traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
                indexes.addAll(table.getIndexesIncludingInternal());
                table.rowDef().getTableStatus().truncate(session);
            }
        });

        indexes.addAll(group.getIndexes());
        truncateIndexes(session, indexes);

        // Truncate the group tree
        truncateTree(session, group);
    }

    @Override
    public void truncateTableStatus(final Session session, final int rowDefId) {
        getRowDef(session, rowDefId).getTableStatus().truncate(session);
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            truncateTree(session, index.indexDef());
        }
        // Delete any statistics associated with index.
        indexStatisticsService.deleteIndexStatistics(session, indexes);
    }

    @Override
    public void deleteIndexes(final Session session, final Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            final IndexDef indexDef = index.indexDef();
            if(indexDef != null) {
                removeTree(session, indexDef);
            }
        }
        indexStatisticsService.deleteIndexStatistics(session, indexes);
    }

    @Override
    public void removeTrees(Session session, UserTable table) {
        // Table indexes
        for(Index index : table.getIndexesIncludingInternal()) {
            removeTree(session, index.indexDef());
        }
        indexStatisticsService.deleteIndexStatistics(session, table.getIndexesIncludingInternal());

        // Group indexes
        for(Index index : table.getGroupIndexes()) {
            removeTree(session, index.indexDef());
        }
        indexStatisticsService.deleteIndexStatistics(session, table.getGroupIndexes());

        // Sequence
        if(table.getIdentityColumn() != null) {
            deleteSequences(session, Collections.singleton(table.getIdentityColumn().getIdentityGenerator()));
        }

        // And the group tree
        removeTree(session, table.getGroup());
    }

    @Override
    public void removeTrees(Session session, Collection<? extends TreeLink> treeLinks) {
        for(TreeLink link : treeLinks) {
            removeTree(session, link);
        }
    }


    //
    // Internal
    //

    protected abstract void preWriteRow(Session session, RowDef rowDef, RowData rowData, StoreType storeData);

    protected abstract Key getKey(Session session, StoreType storeType);

    protected abstract void packRowData(Session sesiosn, StoreType storeType, RowData rowData);

    protected abstract void save(Session session, StoreType storeType);

    protected abstract void propagateDownGroup(Session session,
                                               StoreType exchange,
                                               BitSet tablesRequiringHKeyMaintenance,
                                               PersistitIndexRowBuffer indexRowBuffer,
                                               boolean deleteIndexes,
                                               boolean cascadeDelete);

    protected void writeRow(Session session,
                            RowData rowData,
                            BitSet tablesRequiringHKeyMaintenance,
                            boolean propagateHKeyChanges)
    {
        RowDef rowDef = writeRowCheck(session, rowData);
        StoreType storeType = createStoreType(session, rowDef.getGroup());
        WRITE_ROW_TAP.in();
        try {
            preWriteRow(session, rowDef, rowData, storeType);
            writeRowInternal(session, storeType, rowDef, rowData, tablesRequiringHKeyMaintenance, propagateHKeyChanges);
        } finally {
            WRITE_ROW_TAP.out();
            releaseStoreType(session, storeType);
        }
    }

    private void writeRowInternal(Session session,
                                  StoreType storeType,
                                  RowDef rowDef,
                                  RowData rowData,
                                  BitSet tablesRequiringHKeyMaintenance,
                                  boolean propagateHKeyChanges) {
        Key hKey = getKey(session, storeType);
        /*
         * About propagateHKeyChanges as second to last argument to constructHKey (i.e. insertingRow):
         * - If this argument is true, it means that we're inserting a new row, and if the row's type
         *   has a generated PK, then a PK value needs to be generated.
         * - If this writeRow invocation is being done as part of hKey maintenance (called from
         *   propagateDownGroup with propagateHKeyChanges false), then we are deleting and reinserting
         *   a row, and we don't want the PK value changed.
         * - See bug 1020342.
         */
        constructHKey(session, rowDef, rowData, propagateHKeyChanges, hKey);
        /*
         * Don't check hKey uniqueness here. It would require a database access and we're not int
         * in a good position to report a meaningful uniqueness violation, e.g. on the PK, since
         * we don't have the PK value handy. Instead, rely on PK validation when indexes are maintained.
         */
        packRowData(session, storeType, rowData);
        save(session, storeType);
        if (rowDef.isAutoIncrement()) {
            final long location = rowDef.fieldLocation(rowData, rowDef.getAutoIncrementField());
            if (location != 0) {
                long autoIncrementValue = rowData.getIntegerValue((int) location, (int) (location >>> 32));
                rowDef.getTableStatus().setAutoIncrement(session, autoIncrementValue);
            }
        }

        // TODO: FullText
        // addChangeFor(rowDef.userTable(), session, hEx.getKey());
        PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(this);
        for(Index index : rowDef.getIndexes()) {
            insertIntoIndex(session, index, rowData, hKey, indexRow);
        }

        /*
         * Bump row count *after* uniqueness checks in insertIntoIndex.
         * Avoids spurious live value increases to the TableStatus row count.
         * See bug1112940.
         */
        rowDef.getTableStatus().rowsWritten(session, 1);

        if(propagateHKeyChanges && rowDef.userTable().hasChildren()) {
            /*
             * Row being inserted might be the parent of orphan rows already present.
             * The hKeys of these orphan rows need to be maintained. The ones of interest
             * contain the PK from the inserted row, and nulls for other hKey fields nearer the root.
             */
            hKey.clear();
            PersistitKeyAppender hKeyAppender = PersistitKeyAppender.create(hKey);
            UserTable table = rowDef.userTable();
            List<Column> pkColumns = table.getPrimaryKeyIncludingInternal().getColumns();
            List<HKeySegment> hKeySegments = table.hKey().segments();
            int s = 0;
            while (s < hKeySegments.size()) {
                HKeySegment segment = hKeySegments.get(s++);
                RowDef segmentRowDef = segment.table().rowDef();
                hKey.append(segmentRowDef.userTable().getOrdinal());
                List<HKeyColumn> hKeyColumns = segment.columns();
                int c = 0;
                while (c < hKeyColumns.size()) {
                    HKeyColumn hKeyColumn = hKeyColumns.get(c++);
                    Column column = hKeyColumn.column();
                    RowDef columnTableRowDef = column.getTable().rowDef();
                    if (pkColumns.contains(column)) {
                        hKeyAppender.append(columnTableRowDef.getFieldDef(column.getPosition()), rowData);
                    } else {
                        hKey.append(null);
                    }
                }
            }
            propagateDownGroup(session, storeType, tablesRequiringHKeyMaintenance, indexRow, true, false);
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
        final RowDef rowDef = getRowDef(session, rowDefId);
        if (rowDef == null) {
            throw new IllegalArgumentException("No RowDef for rowDefId " + rowDefId);
        }
        return rowDef;
    }

    private void lockAndCheckVersion(Session session, RowDef rowDef) {
        final LockService.Mode mode = LockService.Mode.SHARED;
        final int tableID = rowDef.getRowDefId();

        // Since this is called on a per-row basis, we can't rely on reentrancy.
        if(lockService.isTableClaimed(session, mode, tableID)) {
            return;
        }

        /*
         * No need to retry locks or back off already acquired. Other locker is DDL
         * and it performs needed backoff to prevent deadlocks.
         * Note that tryClaim() could be used and if false, throw TableChanged
         * right away. Instead, desire is for timeout to elapse here so that client
         * doesn't spin in a try lop.
         */
        try {
            if(session.hasTimeoutAfterNanos()) {
                long remaining = session.getRemainingNanosBeforeTimeout();
                if(remaining <= 0 || !lockService.tryClaimTableNanos(session, mode, tableID, remaining)) {
                    throw new QueryTimedOutException(session.getElapsedMillis());
                }
            } else {
                lockService.claimTableInterruptible(session, mode, tableID);
            }
        } catch(InterruptedException e) {
            throw new QueryCanceledException(session);
        }

        if(schemaManager.hasTableChanged(session, tableID)) {
            // Simple: Release claim so we hit this block again. Could also rollback transaction?
            lockService.releaseTable(session, LockService.Mode.SHARED, tableID);
            throw new TableChangedByDDLException(rowDef.table().getName());
        }
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
}
