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

package com.akiban.server.store;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.OperatorBasedRowCollector;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.AkServerUtil;
import com.akiban.server.TableStatistics;
import com.akiban.server.TableStatus;
import com.akiban.server.TableStatusCache;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.collation.CString;
import com.akiban.server.collation.CStringKeyCoder;
import com.akiban.server.error.CursorCloseBadException;
import com.akiban.server.error.CursorIsUnknownException;
import com.akiban.server.error.DisplayFilterSetException;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchRowException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.error.RowDataCorruptionException;
import com.akiban.server.rowdata.CorruptRowDataException;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.KeyState;
import com.persistit.Management;
import com.persistit.Management.DisplayFilter;
import com.persistit.Persistit;
import com.persistit.Tree;
import com.persistit.Value;
import com.persistit.encoding.CoderManager;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RollbackException;

public class PersistitStore implements Store {

    private static final Session.MapKey<Integer, List<RowCollector>> COLLECTORS = Session.MapKey.mapNamed("collectors");

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistitStore.class.getName());

    private static final InOutTap WRITE_ROW_TAP = Tap.createTimer("write: write_row");

    private static final InOutTap UPDATE_ROW_TAP = Tap.createTimer("write: update_row");

    private static final InOutTap DELETE_ROW_TAP = Tap.createTimer("write: delete_row");

    private static final InOutTap TABLE_INDEX_MAINTENANCE_TAP = Tap.createTimer("index: maintain_table");

    private static final InOutTap NEW_COLLECTOR_TAP = Tap.createTimer("read: new_collector");

    // an InOutTap would be nice, but pre-propagateDownGroup optimization, propagateDownGroup was called recursively
    // (via writeRow). PointTap handles this correctly, InOutTap does not, currently.
    private static final PointTap PROPAGATE_HKEY_CHANGE_TAP = Tap.createCount("write: propagate_hkey_change");
    private static final PointTap PROPAGATE_HKEY_CHANGE_ROW_REPLACE_TAP = Tap.createCount("write: propagate_hkey_change_row_replace");

    private final static int MAX_ROW_SIZE = 5000000;

    private final static int MAX_INDEX_TRANCHE_SIZE = 10 * 1024 * 1024;

    private final static int KEY_STATE_SIZE_OVERHEAD = 50;

    private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private boolean updateGroupIndexes;

    private boolean deferIndexes = false;

    RowDefCache rowDefCache;

    private final ConfigurationService config;

    private final TreeService treeService;

    private TableStatusCache tableStatusCache;

    private DisplayFilter originalDisplayFilter;

    private volatile IndexStatisticsService indexStatistics;

    private final Map<Tree, SortedSet<KeyState>> deferredIndexKeys = new HashMap<Tree, SortedSet<KeyState>>();

    private int deferredIndexKeyLimit = MAX_INDEX_TRANCHE_SIZE;

    public PersistitStore(boolean updateGroupIndexes, TreeService treeService, ConfigurationService config) {
        this.updateGroupIndexes = updateGroupIndexes;
        this.treeService = treeService;
        this.config = config;
    }

    @Override
    public synchronized void start() {
        tableStatusCache = treeService.getTableStatusCache();
        rowDefCache = new RowDefCache(tableStatusCache);
        try {
            CoderManager cm = getDb().getCoderManager();
            Management m = getDb().getManagement();
            cm.registerValueCoder(RowData.class, new RowDataValueCoder(this));
            cm.registerKeyCoder(CString.class, new CStringKeyCoder());
            originalDisplayFilter = m.getDisplayFilter();
            m.setDisplayFilter(new RowDataDisplayFilter(originalDisplayFilter));
        } catch (RemoteException e) {
            throw new DisplayFilterSetException (e.getMessage());
        }
    }

    @Override
    public synchronized void stop() {
        try {
            getDb().getManagement().setDisplayFilter(originalDisplayFilter);
        } catch (RemoteException e) {
            throw new DisplayFilterSetException (e.getMessage());
        }
        rowDefCache = null;
    }

    @Override
    public void crash() {
        stop();
    }

    @Override
    public Store cast() {
        return this;
    }

    @Override
    public Class<Store> castClass() {
        return Store.class;
    }

    @Override
    public PersistitStore getPersistitStore() {
        return this;
    }

    public TreeService treeService() {
        return treeService;
    }

    public Persistit getDb() {
        return treeService.getDb();
    }

    public Exchange getExchange(final Session session, final RowDef rowDef) {
        final RowDef groupRowDef = rowDef.isGroupTable() ? rowDef
                                   : rowDefCache.getRowDef(rowDef.getGroupRowDefId());
        return treeService.getExchange(session, groupRowDef);
    }

    public Exchange getExchange(final Session session, final Index index) {
        return treeService.getExchange(session, index.indexDef());
    }

    public Key getKey()
    {
        return treeService.getKey();
    }

    public void releaseExchange(final Session session, final Exchange exchange) {
        treeService.releaseExchange(session, exchange);
    }

    // Given a RowData for a table, construct an hkey for a row in the table.
    // For a table that does not contain its own hkey, this method uses the
    // parent join columns as needed to find the hkey of the parent table.
    public long constructHKey(Session session,
                              Exchange hEx,
                              RowDef rowDef,
                              RowData rowData,
                              boolean insertingRow) throws PersistitException
    {
        PersistitAdapter adapter = adapter(session);
        // Initialize the hkey being constructed
        long uniqueId = -1;
        PersistitKeyAppender hKeyAppender = PersistitKeyAppender.create(hEx.getKey());
        hKeyAppender.key().clear();
        // Metadata for the row's table
        UserTable table = rowDef.userTable();
        FieldDef[] fieldDefs = rowDef.getFieldDefs();
        // Metadata and other state for the parent table
        RowDef parentRowDef = null;
        TableIndex parentPK = null;
        if (rowDef.getParentRowDefId() != 0) {
            parentRowDef = rowDefCache.getRowDef(rowDef.getParentRowDefId());
            parentPK = parentRowDef.getPKIndex();
        }
        IndexToHKey indexToHKey = null;
        int i2hPosition = 0;
        Exchange parentPKExchange = null;
        PersistitIndexRowBuffer parentPKIndexRow = null;
        // Nested loop over hkey metadata: All the segments of an hkey, and all
        // the columns of a segment.
        List<HKeySegment> hKeySegments = table.hKey().segments();
        int s = 0;
        while (s < hKeySegments.size()) {
            HKeySegment hKeySegment = hKeySegments.get(s++);
            // Write the ordinal for this segment
            RowDef segmentRowDef = rowDefCache.getRowDef(hKeySegment.table().getTableId());
            hKeyAppender.append(segmentRowDef.getOrdinal());
            // Iterate over the segment's columns
            List<HKeyColumn> hKeyColumns = hKeySegment.columns();
            int c = 0;
            while (c < hKeyColumns.size()) {
                HKeyColumn hKeyColumn = hKeyColumns.get(c++);
                UserTable hKeyColumnTable = hKeyColumn.column().getUserTable();
                if (hKeyColumnTable != table) {
                    // Hkey column from row of parent table
                    if (parentPKExchange == null) {
                        // Initialize parent metadata and state
                        assert parentRowDef != null : rowDef;
                        assert parentPK != null : parentRowDef;
                        indexToHKey = parentPK.indexToHKey();
                        parentPKExchange = getExchange(session, parentPK);
                        parentPKIndexRow = readPKIndexRow(adapter, parentPK, parentPKExchange, rowDef, rowData);
                    }
                    if(indexToHKey.isOrdinal(i2hPosition)) {
                        assert indexToHKey.getOrdinal(i2hPosition) == segmentRowDef.getOrdinal() : hKeyColumn;
                        ++i2hPosition;
                    }
                    if (parentPKIndexRow != null) {
                        parentPKIndexRow.appendFieldTo(indexToHKey.getIndexRowPosition(i2hPosition), hKeyAppender.key());
                    } else {
                        hKeyAppender.appendNull(); // orphan row
                    }
                    ++i2hPosition;
                } else {
                    // Hkey column from rowData
                    Column column = hKeyColumn.column();
                    FieldDef fieldDef = fieldDefs[column.getPosition()];
                    if (insertingRow && column.isAkibanPKColumn()) {
                        // Must be a PK-less table. Use unique id from TableStatus.
                        uniqueId = tableStatusCache.createNewUniqueID(segmentRowDef.getRowDefId());
                        hKeyAppender.append(uniqueId);
                        // Write rowId into the value part of the row also.
                        rowData.updateNonNullLong(fieldDef, uniqueId);
                    } else {
                        hKeyAppender.append(fieldDef, rowData);
                    }
                }
            }
        }
        if (parentPKExchange != null) {
            releaseExchange(session, parentPKExchange);
        }
        return uniqueId;
    }

    void constructHKey(Exchange hEx, RowDef rowDef, int[] ordinals,
            int[] nKeyColumns, FieldDef[] hKeyFieldDefs, Object[] hKeyValues) throws PersistitInterruptedException {
        PersistitKeyAppender appender = PersistitKeyAppender.create(hEx.getKey());
        final Key hkey = hEx.getKey();
        hkey.clear();
        int k = 0;
        for (int i = 0; i < ordinals.length; i++) {
            appender.append(ordinals[i]);
            for (int j = 0; j < nKeyColumns[i]; j++) {
                FieldDef fieldDef = hKeyFieldDefs[k];
                if (fieldDef.isPKLessTableCounter()) {
                    // TODO: Maintain a counter elsewhere, maybe in the
                    // FieldDef. At the end of the bulk load,
                    // TODO: assign the counter to TableStatus.
                    long id = tableStatusCache.createNewUniqueID(fieldDef.getRowDef().getRowDefId());
                    hkey.append(id);
                } else {
                    appender.append(hKeyValues[k], fieldDef);
                }
                k++;
            }
        }
    }

    private static void constructIndexRow(Exchange exchange,
                                          RowData rowData,
                                          Index index,
                                          Key hKey,
                                          PersistitIndexRowBuffer indexRow) throws PersistitException
    {
        indexRow.resetForWrite(index, exchange.getKey(), exchange.getValue());
        indexRow.initialize(rowData, hKey);
        indexRow.close();
    }

    private PersistitIndexRowBuffer readPKIndexRow(PersistitAdapter adapter,
                                                   Index pkIndex,
                                                   Exchange exchange,
                                                   RowDef rowDef,
                                                   RowData rowData) throws PersistitException
    {
        PersistitKeyAppender keyAppender = PersistitKeyAppender.create(exchange.getKey());
        int[] fields = rowDef.getParentJoinFields();
        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            FieldDef fieldDef = rowDef.getFieldDef(fields[fieldIndex]);
            keyAppender.append(fieldDef, rowData);
        }
        exchange.fetch();
        PersistitIndexRowBuffer indexRow = null;
        if (exchange.getValue().isDefined()) {
            indexRow = new PersistitIndexRowBuffer(adapter);
            indexRow.resetForRead(pkIndex, exchange.getKey(), exchange.getValue());
        }
        return indexRow;
    }

    // --------------------- Implement Store interface --------------------

    @Override
    public RowDefCache getRowDefCache() {
        return rowDefCache;
    }

    @Override
    public void writeRow(Session session, RowData rowData)
        throws PersistitException
    {
        writeRow(session, rowData, null, true);
    }
    
    private void writeRow(Session session,
                          RowData rowData, 
                          BitSet tablesRequiringHKeyMaintenance, 
                          boolean propagateHKeyChanges) throws PersistitException 
    {
        final int rowDefId = rowData.getRowDefId();

        if (rowData.getRowSize() > MAX_ROW_SIZE) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("RowData size " + rowData.getRowSize() + " is larger than current limit of " + MAX_ROW_SIZE
                                 + " bytes");
            }
        }

        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        checkNoGroupIndexes(rowDef.table());
        Exchange hEx;
        hEx = getExchange(session, rowDef);
        WRITE_ROW_TAP.in();
        try {
            // Does the heavy lifting of looking up the full hkey in
            // parent's primary index if necessary.
            // About the propagateHKeyChanges flag: The last argument of constructHKey is insertingRow.
            // If this argument is true, it means that we're inserting a new row, and if the row's type
            // has a generated PK, then a PK value needs to be generated. If this writeRow invocation is
            // being done as part of hkey maintenance (called from propagateDownGroup with propagateHKeyChanges
            // false), then we are deleting and reinserting a row, and we don't want the PK value changed.
            // See bug 1020342.
            constructHKey(session, hEx, rowDef, rowData, propagateHKeyChanges);
            // Don't check hkey uniqueness. That requires a database access (hEx.isValueDefined()), and we are not
            // in a good position to report a meaningful uniqueness violation, e.g. on the PK, since we don't have
            // the PK value handy. Instead, rely on PK validation when indexes are maintained.

            packRowData(hEx, rowDef, rowData);
            // Store the h-row
            hEx.store();
            if (rowDef.isAutoIncrement()) {
                final long location = rowDef.fieldLocation(rowData, rowDef.getAutoIncrementField());
                if (location != 0) {
                    long autoIncrementValue = rowData.getIntegerValue((int) location, (int) (location >>> 32));
                    tableStatusCache.setAutoIncrement(rowDefId, autoIncrementValue);
                }
            }
            tableStatusCache.rowWritten(rowDefId);
            PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(adapter(session));
            for (Index index : rowDef.getIndexes()) {
                insertIntoIndex(session, index, rowData, hEx.getKey(), indexRow, deferIndexes);
            }

            if (propagateHKeyChanges) {
                // The row being inserted might be the parent of orphan rows
                // already present. The hkeys of these
                // orphan rows need to be maintained. The hkeys of interest
                // contain the PK from the inserted row,
                // and nulls for other hkey fields nearer the root.
                // TODO: optimizations
                // - If we knew that no descendent table had an orphan (e.g.
                // store this info in TableStatus),
                // then this propagation could be skipped.
                hEx.clear();
                Key hKey = hEx.getKey();
                PersistitKeyAppender hKeyAppender = PersistitKeyAppender.create(hKey);
                UserTable table = rowDef.userTable();
                List<Column> pkColumns = table.getPrimaryKeyIncludingInternal().getColumns();
                List<HKeySegment> hKeySegments = table.hKey().segments();
                int s = 0;
                while (s < hKeySegments.size()) {
                    HKeySegment segment = hKeySegments.get(s++);
                    RowDef segmentRowDef = rowDefCache.getRowDef(segment.table().getTableId());
                    hKey.append(segmentRowDef.getOrdinal());
                    List<HKeyColumn> hKeyColumns = segment.columns();
                    int c = 0;
                    while (c < hKeyColumns.size()) {
                        HKeyColumn hKeyColumn = hKeyColumns.get(c++);
                        Column column = hKeyColumn.column();
                        RowDef columnTableRowDef = rowDefCache.getRowDef(column.getTable().getTableId());
                        if (pkColumns.contains(column)) {
                            hKeyAppender.append(columnTableRowDef.getFieldDef(column.getPosition()), rowData);
                        } else {
                            hKey.append(null);
                        }
                    }
                }
                propagateDownGroup(session, hEx, tablesRequiringHKeyMaintenance, indexRow);
            }

            if (deferredIndexKeyLimit <= 0) {
                putAllDeferredIndexKeys(session);
            }
        } finally {
            WRITE_ROW_TAP.out();
            releaseExchange(session, hEx);
        }
    }

    @Override
    public void writeRowForBulkLoad(final Session session, Exchange hEx,
            RowDef rowDef, RowData rowData, int[] ordinals, int[] nKeyColumns,
            FieldDef[] hKeyFieldDefs, Object[] hKeyValues) throws PersistitException  {
        /*
         * if (verbose && LOG.isInfoEnabled()) { LOG.info("BulkLoad writeRow: "
         * + rowData.toString(rowDefCache)); }
         */

        constructHKey(hEx, rowDef, ordinals, nKeyColumns, hKeyFieldDefs,
                hKeyValues);
        packRowData(hEx, rowDef, rowData);
        // Store the h-row
        hEx.store();
        /*
         * for (final IndexDef indexDef : rowDef.getIndexDefs()) { // Insert the
         * index keys (except for the case of a // root table's PK index.) if
         * (!indexDef.isHKeyEquivalent()) { insertIntoIndex(indexDef, rowData,
         * hEx.getKey(), deferIndexes); } } if (deferredIndexKeyLimit <= 0) {
         * putAllDeferredIndexKeys(); }
         */
        return;
    }

    // TODO - remove - this is used only by the PersistitStoreAdapter in
    // bulk loader.
    @Override
    public void updateTableStats(final Session session, RowDef rowDef,
            long rowCount) {
        // no-up for now
    }

    @Override
    public void deleteRow(Session session, RowData rowData)
        throws PersistitException
    {
        deleteRow(session, rowData, null);
        // TODO: It should be possible to optimize propagateDownGroup for inserts too
        // deleteRow(session, rowData, hKeyDependentTableOrdinals(rowData.getRowDefId()));
    }

    private void deleteRow(Session session, RowData rowData, BitSet tablesRequiringHKeyMaintenance)
        throws PersistitException
    {
        int rowDefId = rowData.getRowDefId();
        RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        checkNoGroupIndexes(rowDef.table());
        Exchange hEx = null;
        DELETE_ROW_TAP.in();
        try {
            hEx = getExchange(session, rowDef);

            constructHKey(session, hEx, rowDef, rowData, false);
            hEx.fetch();
            //
            // Verify that the row exists
            //
            if (!hEx.getValue().isDefined()) {
                throw new NoSuchRowException(hEx.getKey());
            }
            //
            // Verify that the row hasn't changed. Note: at some point
            // we may want to optimize the protocol to send only PK and
            // FK fields in oldRowData, in which case this test will
            // need to change.
            //
            // TODO - review. With covering indexes, that day has come.
            // We can no longer do this comparison when the "old" row
            // has only its PK fields.
            //
            // final int oldStart = rowData.getInnerStart();
            // final int oldSize = rowData.getInnerSize();
            // if (!bytesEqual(rowData.getBytes(), oldStart, oldSize,
            // hEx
            // .getValue().getEncodedBytes(), 0, hEx.getValue()
            // .getEncodedSize())) {
            // throw new StoreException(HA_ERR_RECORD_CHANGED,
            // "Record changed at key " + hEx.getKey());
            // }

            // Remove the h-row
            hEx.remove();
            tableStatusCache.rowDeleted(rowDefId);

            // Remove the indexes, including the PK index
            PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(adapter(session));
            for (Index index : rowDef.getIndexes()) {
                deleteIndex(session, index, rowData, hEx.getKey(), indexRow);
            }

            // The row being deleted might be the parent of rows that
            // now become orphans. The hkeys
            // of these rows need to be maintained.
            propagateDownGroup(session, hEx, tablesRequiringHKeyMaintenance, indexRow);
        } finally {
            DELETE_ROW_TAP.out();
            releaseExchange(session, hEx);
        }
    }

    @Override
    public void updateRow(Session session,
                          RowData oldRowData,
                          RowData newRowData,
                          ColumnSelector columnSelector)
        throws PersistitException
    {
        updateRow(session, oldRowData, newRowData, null);
    }

    @Override
    public void updateRow(Session session,
                          RowData oldRowData,
                          RowData newRowData,
                          ColumnSelector columnSelector,
                          Index[] indexesToInsert)
        throws PersistitException {
        int rowDefId = oldRowData.getRowDefId();

        if (newRowData.getRowDefId() != rowDefId) {
            throw new IllegalArgumentException("RowData values have different rowDefId values: ("
                                                       + rowDefId + "," + newRowData.getRowDefId() + ")");
        }
        // RowDefs may be different (e.g. during an ALTER)
        // Only non-pk or grouping columns could have change in this scenario
        RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        RowDef newRowDef = rowDefFromExplicitOrId(newRowData);
        boolean insideAlter = (rowDef != newRowDef);
        checkNoGroupIndexes(rowDef.table());
        Exchange hEx = null;
        UPDATE_ROW_TAP.in();
        try {
            hEx = getExchange(session, rowDef);
            constructHKey(session, hEx, rowDef, oldRowData, false);
            hEx.fetch();
            //
            // Verify that the row exists
            //
            if (!hEx.getValue().isDefined()) {
                throw new NoSuchRowException (hEx.getKey());
            }
            // Combine current version of row with the version coming in
            // on the update request.
            // This is done by taking only the values of columns listed
            // in the column selector.
            RowData currentRow = new RowData(EMPTY_BYTE_ARRAY);
            expandRowData(hEx, currentRow);
            RowData mergedRowData = 
                columnSelector == null 
                ? newRowData
                : mergeRows(rowDef, currentRow, newRowData, columnSelector);
            BitSet tablesRequiringHKeyMaintenance = insideAlter ? null : analyzeFieldChanges(rowDef, oldRowData, mergedRowData);
            if (tablesRequiringHKeyMaintenance == null) {
                // No PK or FK fields have changed. Just update the row.
                packRowData(hEx, newRowDef, mergedRowData);
                // Store the h-row
                hEx.store();
                // Update the indexes
                PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(adapter(session));
                boolean asInsert = (indexesToInsert != null);
                for (Index index : (asInsert ? indexesToInsert : newRowDef.getIndexes())) {
                    if(asInsert) {
                        insertIntoIndex(session, index, newRowData, hEx.getKey(), indexRow, deferIndexes);
                    } else {
                        updateIndex(session, index, rowDef, currentRow, mergedRowData, hEx.getKey(), indexRow);
                    }
                }
            } else {
                // A PK or FK field has changed. The row has to be deleted and reinserted, and hkeys of descendent
                // rows maintained. tablesRequiringHKeyMaintenance contains the ordinals of the tables whose hkeys
                // could possible be affected.
                deleteRow(session, oldRowData, tablesRequiringHKeyMaintenance);
                writeRow(session, mergedRowData, tablesRequiringHKeyMaintenance, true); // May throw DuplicateKeyException
            }
        } finally {
            UPDATE_ROW_TAP.out();
            releaseExchange(session, hEx);
        }
    }
    
    private BitSet analyzeFieldChanges(RowDef rowDef, RowData oldRow, RowData newRow)
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
            tablesRequiringHKeyMaintenance = hKeyDependentTableOrdinals(oldRow.getRowDefId());
        }
        return tablesRequiringHKeyMaintenance;
    }

    private BitSet hKeyDependentTableOrdinals(int rowDefId)
    {
        RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        UserTable table = rowDef.userTable();
        BitSet ordinals = new BitSet();
        for (UserTable hKeyDependentTable : table.hKeyDependentTables()) {
            int ordinal = hKeyDependentTable.rowDef().getOrdinal();
            ordinals.set(ordinal, true);
        }
        return ordinals;
    }

    private void checkNoGroupIndexes(Table table) {
        if (updateGroupIndexes && !table.getGroupIndexes().isEmpty()) {
            throw new UnsupportedOperationException("PersistitStore can't update group indexes; found on " + table);
        }
    }

    // tablesRequiringHKeyMaintenance is non-null only when we're implementing an updateRow as delete/insert, due
    // to a PK or FK column being updated.
    private void propagateDownGroup(Session session,
                                    Exchange exchange,
                                    BitSet tablesRequiringHKeyMaintenance,
                                    PersistitIndexRowBuffer indexRow)
            throws PersistitException
    {
        // exchange is positioned at a row R that has just been replaced by R', (because we're processing an update
        // that has to be implemented as delete/insert). hKey is the hkey of R. The replacement, R', is already
        // present. For each descendent* D of R, this method deletes and reinserts D. Reinsertion of D causes its
        // hkey to be recomputed. This may depend on an ancestor being updated (if part of D's hkey comes from
        // the parent's PK index). That's OK because updates are processed preorder, (i.e., ancestors before
        // descendents). This method will modify the state of exchange.
        //
        // * D is a descendent of R means that D is below R in the group. I.e., hkey(R) is a prefix of hkey(D).
        PROPAGATE_HKEY_CHANGE_TAP.hit();
        Key hKey = exchange.getKey();
        KeyFilter filter = new KeyFilter(hKey, hKey.getDepth() + 1, Integer.MAX_VALUE);
        RowData descendentRowData = new RowData(EMPTY_BYTE_ARRAY);
        while (exchange.next(filter)) {
            expandRowData(exchange, descendentRowData);
            int descendentRowDefId = descendentRowData.getRowDefId();
            RowDef descendentRowDef = rowDefCache.getRowDef(descendentRowDefId);
            int descendentOrdinal = descendentRowDef.getOrdinal();
            if ((tablesRequiringHKeyMaintenance == null || tablesRequiringHKeyMaintenance.get(descendentOrdinal))) {
                PROPAGATE_HKEY_CHANGE_ROW_REPLACE_TAP.hit();
                // Delete the current row from the tree. Don't call deleteRow, because we don't need to recompute
                // the hkey.
                exchange.remove();
                tableStatusCache.rowDeleted(descendentRowDefId);
                for (Index index : descendentRowDef.getIndexes()) {
                    deleteIndex(session, index, descendentRowData, exchange.getKey(), indexRow);
                }
                // Reinsert it, recomputing the hkey and maintaining indexes
                writeRow(session, descendentRowData, tablesRequiringHKeyMaintenance, false);
            }
        }
    }

    @Override
    public void dropGroup(Session session, int rowDefId) throws PersistitException {
        RowDef groupRowDef = rowDefCache.getRowDef(rowDefId);
        if (!groupRowDef.isGroupTable()) {
            groupRowDef = rowDefCache.getRowDef(groupRowDef.getGroupRowDefId());
        }
        for(RowDef userRowDef : groupRowDef.getUserTableRowDefs()) {
            removeTrees(session, userRowDef.table());
        }
        // tableStatusCache entries updated elsewhere
    }

    @Override
    public void truncateGroup(final Session session, final int rowDefId) throws PersistitException {
        RowDef groupRowDef = rowDefCache.getRowDef(rowDefId);
        if (!groupRowDef.isGroupTable()) {
            groupRowDef = rowDefCache.getRowDef(groupRowDef.getGroupRowDefId());
        }

        //
        // Truncate the index trees
        //
        for (RowDef userRowDef : groupRowDef.getUserTableRowDefs()) {
            for (Index index : userRowDef.getIndexes()) {
                truncateIndex(session, Collections.singleton(index));
            }
        }
        for (Index index : groupRowDef.getGroupIndexes()) {
            truncateIndex(session, Collections.singleton(index));
        }

        //
        // Truncate the group tree
        //
        final Exchange hEx = getExchange(session, groupRowDef);
        hEx.removeAll();
        releaseExchange(session, hEx);
        for (int i = 0; i < groupRowDef.getUserTableRowDefs().length; i++) {
            final int childRowDefId = groupRowDef.getUserTableRowDefs()[i].getRowDefId();
            tableStatusCache.truncate(childRowDefId);
        }
    }

    // This is to avoid circular dependencies in Guicer.  
    // TODO: There is still a functional circularity: store needs
    // stats to clear them when deleting a group; stats need store to
    // persist the stats. It would be better to separate out the
    // higher level store functions from what other services require.
    public void setIndexStatistics(IndexStatisticsService indexStatistics) {
        this.indexStatistics = indexStatistics;
    }

    @Override
    public void truncateIndex(Session session, Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            Exchange iEx = getExchange(session, index);
            try {
                iEx.removeAll();
                if (index.isGroupIndex()) {
                    new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ROW_COUNT, treeService, iEx.getTree()).set(0);
                }
            } catch (PersistitException e) {
                throw new PersistitAdapterException(e);
            }
            releaseExchange(session, iEx);
        }
        // Delete any statistics associated with index.
        indexStatistics.deleteIndexStatistics(session, indexes);
    }

    @Override
    public void truncateTableStatus(final Session session, final int rowDefId) throws RollbackException, PersistitException {
        tableStatusCache.truncate(rowDefId);
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
    public void removeSavedRowCollector(final Session session,
            final RowCollector rc) throws CursorIsUnknownException {
        final Integer tableId = rc.getTableId();
        final List<RowCollector> list = collectorsForTableId(session, tableId);
        if (list.isEmpty()) {
            throw new CursorIsUnknownException (tableId);
        }
        final RowCollector removed = list.remove(list.size() - 1);
        if (removed != rc) {
            throw new CursorCloseBadException (tableId);
        }
    }

    private List<RowCollector> collectorsForTableId(final Session session,
            final int tableId) {
        List<RowCollector> list = session.get(COLLECTORS, tableId);
        if (list == null) {
            list = new ArrayList<RowCollector>();
            session.put(COLLECTORS, tableId, list);
        }
        return list;
    }

    private RowDef checkRequest(int rowDefId,RowData start, ColumnSelector startColumns,
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
        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        if (rowDef == null) {
            throw new IllegalArgumentException("No RowDef for rowDefId " + rowDefId);
        }
        return rowDef;
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

    @Override
    public RowCollector newRowCollector(Session session,
                                        int rowDefId,
                                        int indexId,
                                        int scanFlags,
                                        RowData start,
                                        RowData end,
                                        byte[] columnBitMap,
                                        ScanLimit scanLimit)
    {
        return newRowCollector(session, scanFlags, rowDefId, indexId, columnBitMap, start, null, end, null, scanLimit);
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
            RowDef rowDef = checkRequest(rowDefId, start, startColumns, end, endColumns);
            rc = OperatorBasedRowCollector.newCollector(config,
                                                        session,
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

    public final static long HACKED_ROW_COUNT = 2;

    @Override
    public long getRowCount(final Session session, final boolean exact,
            final RowData start, final RowData end, final byte[] columnBitMap) {
        //
        // TODO: Compute a reasonable value. The value "2" is a hack -
        // special because it's not 0 or 1, but small enough to induce
        // MySQL to use an index rather than full table scan.
        //
        return HACKED_ROW_COUNT; // TODO: delete the HACKED_ROW_COUNT field when
                                 // this gets fixed
        // final int tableId = start.getRowDefId();
        // final TableStatus status = tableManager.getTableStatus(tableId);
        // return status.getRowCount();
    }

    @Override
    public TableStatistics getTableStatistics(final Session session, int tableId) {
        final RowDef rowDef = rowDefCache.getRowDef(tableId);
        final TableStatistics ts = new TableStatistics(tableId);
        final TableStatus status = rowDef.getTableStatus();
        try {
            if (rowDef.isGroupTable()) {
                ts.setRowCount(2);
                ts.setAutoIncrementValue(-1);
            } else {
                ts.setAutoIncrementValue(status.getAutoIncrement());
                ts.setRowCount(status.getRowCount());
            }
            // TODO - get correct values
            ts.setMeanRecordLength(100);
            ts.setBlockSize(8192);
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
        for (Index index : rowDef.getIndexes()) {
            TableStatistics.Histogram histogram = indexStatisticsToHistogram(session, 
                                                                             index);
            if (histogram != null) {
                ts.addHistogram(histogram);
            }
        }
        return ts;
    }

    /** Convert from new-format histogram to old for adapter. */
    protected TableStatistics.Histogram indexStatisticsToHistogram(Session session,
                                                                   Index index) {
        IndexStatistics stats = indexStatistics.getIndexStatistics(session, index);
        if (stats == null) {
            return null;
        }
        IndexStatistics.Histogram fromHistogram = stats.getHistogram(index.getKeyColumns().size());
        if (fromHistogram == null) {
            return null;
        }
        IndexDef indexDef = index.indexDef();
        RowDef indexRowDef = indexDef.getRowDef();
        TableStatistics.Histogram toHistogram = new TableStatistics.Histogram(index.getIndexId());
        Key key = treeService.createKey();
        RowData indexRowData = new RowData(new byte[4096]);
        Object[] indexValues = new Object[indexRowDef.getFieldCount()];
        long count = 0;
        for (IndexStatistics.HistogramEntry entry : fromHistogram.getEntries()) {
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
            toHistogram.addSample(new TableStatistics.HistogramSample(indexRowData.copy(),
                                                                      count));
            count += entry.getEqualCount();
        }
        // Add final entry with all nulls.
        Arrays.fill(indexValues, null);
        indexRowData.createRow(indexRowDef, indexValues);
        toHistogram.addSample(new TableStatistics.HistogramSample(indexRowData.copy(),
                                                                  count));
        return toHistogram;
    }

    boolean hasNullIndexSegments(RowData rowData, Index index)
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

    private void checkNotGroupIndex(Index index) {
        if (index.isGroupIndex()) {
            throw new UnsupportedOperationException("can't update group indexes from PersistitStore: " + index);
        }
    }

    private void insertIntoIndex(Session session,
                                 Index index,
                                 RowData rowData,
                                 Key hkey,
                                 PersistitIndexRowBuffer indexRow,
                                 boolean deferIndexes) throws PersistitException
    {
        checkNotGroupIndex(index);
        Exchange iEx = getExchange(session, index);
        constructIndexRow(iEx, rowData, index, hkey, indexRow);
        checkUniqueness(index, rowData, iEx);
        if (deferIndexes) {
            // TODO: bug767737, deferred indexing does not handle uniqueness
            synchronized (deferredIndexKeys) {
                SortedSet<KeyState> keySet = deferredIndexKeys.get(iEx.getTree());
                if (keySet == null) {
                    keySet = new TreeSet<KeyState>();
                    deferredIndexKeys.put(iEx.getTree(), keySet);
                }
                KeyState ks = new KeyState(iEx.getKey());
                keySet.add(ks);
                deferredIndexKeyLimit -= (ks.getBytes().length + KEY_STATE_SIZE_OVERHEAD);
            }
        } else {
            try {
                iEx.store();
            } catch (PersistitException e) {
                throw new PersistitAdapterException(e);
            }
        }
        releaseExchange(session, iEx);
    }

    private void checkUniqueness(Index index, RowData rowData, Exchange iEx) throws PersistitException
    {
        if (index.isUnique() && !hasNullIndexSegments(rowData, index)) {
            Key key = iEx.getKey();
            int segmentCount = index.indexDef().getIndexKeySegmentCount();
            // An index that isUniqueAndMayContainNulls has the extra null-separating field.
            if (index.isUniqueAndMayContainNulls()) {
                segmentCount++;
            }
            key.setDepth(segmentCount);
            if (keyExistsInIndex(index, iEx)) {
                throw new DuplicateKeyException(index.getIndexName().getName(), key);
            }
        }
    }

    private boolean keyExistsInIndex(Index index, Exchange exchange) throws PersistitException
    {
        boolean keyExistsInIndex;
        // Passing -1 as the last argument of traverse leaves the exchange's key and value unmodified.
        // (0 would leave just the value unmodified.)
        if (index.isUnique()) {
            // The Persistit Key stores exactly the index key, so just check whether the key exists.
            // TODO:
            // The right thing to do is traverse(EQ, false, -1) but that returns true, even when the
            // tree is empty. Peter says this is a bug (1023549)
            keyExistsInIndex = exchange.traverse(Key.Direction.EQ, true, -1);
        } else {
            // Check for children by traversing forward from the current key. That can change the key, so
            // we have to make a copy and then restore the original key later, as the caller depends on the
            // exchange's state. Copying/restoring the value is not necessary, because passing 0 as the last
            // argument of traverse causes the value not to be retrieved, leaving the current value in place.
            keyExistsInIndex = exchange.traverse(Key.Direction.GTEQ, true, -1);
        }
        return keyExistsInIndex;
    }

    private void putAllDeferredIndexKeys(final Session session) {
        synchronized (deferredIndexKeys) {
            for (final Map.Entry<Tree, SortedSet<KeyState>> entry : deferredIndexKeys
                    .entrySet()) {
                final Exchange iEx = treeService.getExchange(session, entry.getKey());
                try {
                    buildIndexAddKeys(entry.getValue(), iEx);
                    entry.getValue().clear();
                } finally {
                    treeService.releaseExchange(session, iEx);
                }
            }
            deferredIndexKeyLimit = MAX_INDEX_TRANCHE_SIZE;
        }
    }

    public void updateIndex(Session session,
                            Index index,
                            RowDef rowDef,
                            RowData oldRowData,
                            RowData newRowData,
                            Key hkey,
                            PersistitIndexRowBuffer indexRow)
            throws PersistitException
    {
        checkNotGroupIndex(index);
        IndexDef indexDef = index.indexDef();
        if (!fieldsEqual(rowDef, oldRowData, newRowData, indexDef.getFields())) {
            TABLE_INDEX_MAINTENANCE_TAP.in();
            try {
                Exchange oldExchange = getExchange(session, index);
                constructIndexRow(oldExchange, oldRowData, index, hkey, indexRow);
                Exchange newExchange = getExchange(session, index);
                constructIndexRow(newExchange, newRowData, index, hkey, indexRow);

                checkUniqueness(index, newRowData, newExchange);
                oldExchange.remove();
                newExchange.store();

                releaseExchange(session, newExchange);
                releaseExchange(session, oldExchange);
            } finally {
                TABLE_INDEX_MAINTENANCE_TAP.out();
            }
        }
    }

    void deleteIndex(Session session,
                     Index index,
                     RowData rowData,
                     Key hkey,
                     PersistitIndexRowBuffer indexRow)
            throws PersistitException {
        checkNotGroupIndex(index);
        final Exchange iEx = getExchange(session, index);
        constructIndexRow(iEx, rowData, index, hkey, indexRow);
        boolean removed = iEx.remove();
        releaseExchange(session, iEx);
    }

    static boolean bytesEqual(byte[] a, int aoffset, int asize,
                              byte[] b, int boffset, int bsize) {
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

    public static boolean fieldsEqual(RowDef rowDef, RowData a, RowData b, int[] fieldIndexes)
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

    public static boolean fieldEqual(RowDef rowDef, RowData a, RowData b, int fieldPosition)
    {
        long aloc = rowDef.fieldLocation(a, fieldPosition);
        long bloc = rowDef.fieldLocation(b, fieldPosition);
        return bytesEqual(a.getBytes(), (int) aloc, (int) (aloc >>> 32),
                          b.getBytes(), (int) bloc, (int) (bloc >>> 32));
    }

    public void packRowData(final Exchange hEx, final RowDef rowDef,
            final RowData rowData) {
        final Value value = hEx.getValue();
        value.put(rowData);
        final int at = value.getEncodedSize() - rowData.getInnerSize();
        int storedTableId = treeService.aisToStore(rowDef, rowData.getRowDefId());
        /*
         * Overwrite rowDefId field within the Value instance with the absolute
         * rowDefId.
         */
        AkServerUtil.putInt(hEx.getValue().getEncodedBytes(), at + RowData.O_ROW_DEF_ID - RowData.LEFT_ENVELOPE_SIZE,
                storedTableId);
    }

    public void expandRowData(final Exchange exchange, final RowData rowData) {
        final Value value = exchange.getValue();
        try {
            value.get(rowData);
        }
        catch(CorruptRowDataException e) {
            LOG.error("Corrupt RowData at key {}: {}", exchange.getKey(), e.getMessage());
            throw new RowDataCorruptionException(exchange.getKey());
        }
        rowData.prepareRow(0);
        int rowDefId = treeService.storeToAis(exchange.getVolume(), rowData.getRowDefId());
        /*
         * Overwrite the rowDefId field within the RowData instance with the
         * relative rowDefId.
         */
        AkServerUtil.putInt(rowData.getBytes(), RowData.O_ROW_DEF_ID, rowDefId);
    }

    @Override
    public void buildAllIndexes(Session session, boolean deferIndexes) {
        Collection<Index> indexes = new HashSet<Index>();
        for(RowDef rowDef : rowDefCache.getRowDefs()) {
            if(rowDef.isUserTable()) {
                indexes.addAll(Arrays.asList(rowDef.getIndexes()));
            }
        }
        buildIndexes(session, indexes, deferIndexes);
    }

    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean defer) {
        flushIndexes(session);
        Set<RowDef> userRowDefs = new HashSet<RowDef>();
        Set<RowDef> groupRowDefs = new HashSet<RowDef>();
        Set<Index> indexesToBuild = new HashSet<Index>();
        for(Index index : indexes) {
            IndexDef indexDef = index.indexDef();
            if(indexDef == null) {
                throw new IllegalArgumentException("indexDef was null for index: " + index);
            }
            indexesToBuild.add(index);
            RowDef rowDef = indexDef.getRowDef();
            userRowDefs.add(rowDef);
            RowDef groupDef = rowDefCache.getRowDef(rowDef.getGroupRowDefId());
            if(groupDef != null) {
                groupRowDefs.add(groupDef);
            }
        }
        PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(adapter(session));
        for (RowDef rowDef : groupRowDefs) {
            RowData rowData = new RowData(new byte[MAX_ROW_SIZE]);
            rowData.createRow(rowDef, new Object[0]);

            byte[] columnBitMap = new byte[(rowDef.getFieldCount() + 7) / 8];
            // Project onto all columns of selected user tables
            for (RowDef user : rowDef.getUserTableRowDefs()) {
                if (userRowDefs.contains(user)) {
                    for (int bit = 0; bit < user.getFieldCount(); bit++) {
                        int c = bit + user.getColumnOffset();
                        columnBitMap[c / 8] |= (1 << (c % 8));
                    }
                }
            }
            int indexKeyCount = 0;
            Exchange hEx = getExchange(session, rowDef);
            hEx.getKey().clear();
            // while (hEx.traverse(Key.GT, hFilter, Integer.MAX_VALUE)) {
            try {
                while (hEx.next(true)) {
                    expandRowData(hEx, rowData);
                    int tableId = rowData.getRowDefId();
                    RowDef userRowDef = rowDefCache.getRowDef(tableId);
                    if (userRowDefs.contains(userRowDef)) {
                        for (Index index : userRowDef.getIndexes()) {
                            if(indexesToBuild.contains(index)) {
                                insertIntoIndex(session, index, rowData, hEx.getKey(), indexRow, defer);
                                indexKeyCount++;
                            }
                        }
                        if (deferredIndexKeyLimit <= 0) {
                            putAllDeferredIndexKeys(session);
                        }
                    }
                }
            } catch (PersistitException e) {
                throw new PersistitAdapterException(e);
            }
            flushIndexes(session);
            LOG.debug("Inserted {} index keys into {}", indexKeyCount, rowDef.table().getName());
        }
    }

    private void removeTrees(Session session, Collection<TreeLink> treeLinks) {
        Exchange ex = null;
        try {
            for(TreeLink link : treeLinks) {
                ex = treeService.getExchange(session, link);
                ex.removeTree();
                releaseExchange(session, ex);
                ex = null;
            }
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        } finally {
            if(ex != null) {
                releaseExchange(session, ex);
            }
        }
    }

    @Override
    public void removeTrees(Session session, Table table) {
        Collection<TreeLink> treeLinks = new ArrayList<TreeLink>();
        // Add all index trees
        final Collection<TableIndex> tableIndexes = table.isUserTable() ? ((UserTable)table).getIndexesIncludingInternal() : table.getIndexes();
        final Collection<GroupIndex> groupIndexes = table.getGroupIndexes();
        for(Index index : tableIndexes) {
            treeLinks.add(index.indexDef());
        }
        for(Index index : groupIndexes) {
            treeLinks.add(index.indexDef());
        }
        // Drop the sequence trees too. 
        if (table.isUserTable() && ((UserTable)table).getIdentityColumn() != null) {
            treeLinks.add(((UserTable)table).getIdentityColumn().getIdentityGenerator());
        } else if (table.isGroupTable()) {
            for (UserTable userTable : table.getAIS().getUserTables().values()) {
                if (userTable.getGroup() == table.getGroup() &&
                        userTable.getIdentityColumn() != null) {
                    treeLinks.add(userTable.getIdentityColumn().getIdentityGenerator());
                }
            }
        }
        
        // And the group tree
        treeLinks.add(table.rowDef());
        // And drop them all
        removeTrees(session, treeLinks);
        indexStatistics.deleteIndexStatistics(session, tableIndexes);
        indexStatistics.deleteIndexStatistics(session, groupIndexes);
    }

    public void flushIndexes(final Session session) {
        try {
            putAllDeferredIndexKeys(session);
        } catch (PersistitAdapterException e) {
            LOG.debug("Exception while trying to flush deferred index keys", e);
            throw e;
        }
    }

    public void deleteIndexes(final Session session, final Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            final IndexDef indexDef = index.indexDef();
            if(indexDef == null) {
                throw new IllegalArgumentException("indexDef is null for index: " + index);
            }
            try {
                Exchange iEx = getExchange(session, index);
                iEx.removeTree();
            } catch (PersistitException e) {
                LOG.debug("Exception while removing index tree: " + indexDef, e);
                throw new PersistitAdapterException(e);
            }
        }
        indexStatistics.deleteIndexStatistics(session, indexes);
    }
    
    @Override
    public void deleteSequences (Session session, Collection<? extends Sequence> sequences) {
        Collection<TreeLink> links = new ArrayList<TreeLink>();
        links.addAll(sequences);
        removeTrees(session, links);
    }

    private void buildIndexAddKeys(final SortedSet<KeyState> keys,
            final Exchange iEx) {
        final long start = System.nanoTime();
        try {
            for (final KeyState keyState : keys) {
                keyState.copyTo(iEx.getKey());
                iEx.store();
            }
        } catch (PersistitException e) {
            LOG.error(e.getMessage());
            throw new PersistitAdapterException(e);
        }
        final long elapsed = System.nanoTime() - start;
        if (LOG.isInfoEnabled()) {
            LOG.debug("Index builder inserted {} keys into index tree {} in {} seconds", new Object[]{
                    keys.size(),
                    iEx.getTree().getName(),
                    elapsed / 1000000000
            });
        }
    }

    private RowData mergeRows(RowDef rowDef, RowData currentRow, RowData newRowData, ColumnSelector columnSelector) {
        NewRow mergedRow = NiceRow.fromRowData(currentRow, rowDef);
        NewRow newRow = new LegacyRowWrapper(newRowData, this);
        int fields = rowDef.getFieldCount();
        for (int i = 0; i < fields; i++) {
            if (columnSelector.includesColumn(i)) {
                mergedRow.put(i, newRow.get(i));
            }
        }
        return mergedRow.toRowData();
    }

    private RowDef rowDefFromExplicitOrId(RowData rowData) {
        RowDef rowDef = rowData.getExplicitRowDef();
        if(rowDef == null) {
            rowDef = rowDefCache.getRowDef(rowData.getRowDefId());
        }
        return rowDef;
    }

    @Override
    public boolean isDeferIndexes() {
        return deferIndexes;
    }

    @Override
    public void setDeferIndexes(final boolean defer) {
        deferIndexes = defer;
    }

    public void traverse(Session session, RowDef rowDef, TreeRecordVisitor visitor)
            throws PersistitException, InvalidOperationException {
        assert rowDef.isGroupTable() : rowDef;
        Exchange exchange = getExchange(session, rowDef).append(
                Key.BEFORE);
        try {
            visitor.initialize(this, exchange);
            while (exchange.next(true)) {
                visitor.visit();
            }
        } finally {
            releaseExchange(session, exchange);
        }
    }

    public <V extends IndexVisitor> V traverse(Session session, Index index, V visitor)
            throws PersistitException, InvalidOperationException {
        Exchange exchange = getExchange(session, index).append(Key.BEFORE);
        try {
            visitor.initialize(exchange);
            while (exchange.next(true)) {
                visitor.visit();
            }
        } finally {
            releaseExchange(session, exchange);
        }
        return visitor;
    }

    public TableStatus getTableStatus(Table table) {
        TableStatus ts = null;
        RowDef rowDef = rowDefCache.getRowDef(table.getTableId());
        if (rowDef != null) {
            ts = rowDef.getTableStatus();
        }
        return ts;
    }

    private static PersistitAdapter adapter(Session session)
    {
        return (PersistitAdapter) session.get(StoreAdapter.STORE_ADAPTER_KEY);
    }
}
