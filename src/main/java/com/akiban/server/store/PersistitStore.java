/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.store;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexRowComposition;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.Table;
import com.akiban.qp.persistitadapter.OperatorBasedRowCollector;
import com.akiban.server.*;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.service.config.ConfigurationService;
import com.persistit.exception.PersistitInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.error.CursorCloseBadException;
import com.akiban.server.error.CursorIsUnknownException;
import com.akiban.server.error.DisplayFilterSetException;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchRowException;
import com.akiban.server.error.PersistItErrorException;
import com.akiban.server.error.RowDataCorruptionException;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.util.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.KeyState;
import com.persistit.Management.DisplayFilter;
import com.persistit.Persistit;
import com.persistit.Tree;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

public class PersistitStore implements Store {

    private static final Session.MapKey<Integer, List<RowCollector>> COLLECTORS = Session.MapKey.mapNamed("collectors");
    final static int INITIAL_BUFFER_SIZE = 1024;

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistitStore.class.getName());

    private static final Tap.InOutTap WRITE_ROW_TAP = Tap.createTimer("write: write_row");

    private static final Tap.InOutTap UPDATE_ROW_TAP = Tap.createTimer("write: update_row");

    private static final Tap.InOutTap DELETE_ROW_TAP = Tap.createTimer("write: delete_row");

    private static final Tap.InOutTap TABLE_INDEX_MAINTENANCE_TAP = Tap.createTimer("index: maintain_table");

    private static final Tap.InOutTap NEW_COLLECTOR_TAP = Tap.createTimer("read: new_collector");

    private final static int MEGA = 1024 * 1024;

    private final static int MAX_ROW_SIZE = 5000000;

    private final static int MAX_INDEX_TRANCHE_SIZE = 10 * MEGA;

    private final static int KEY_STATE_SIZE_OVERHEAD = 50;

    private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private boolean updateGroupIndexes;

    private boolean deferIndexes = false;

    RowDefCache rowDefCache;

    final ConfigurationService config;

    final TreeService treeService;

    TableStatusCache tableStatusCache;

    boolean forceToDisk = false; // default to "group commit"

    private DisplayFilter originalDisplayFilter;

    private PersistitStoreIndexManager indexManager;

    private final Map<Tree, SortedSet<KeyState>> deferredIndexKeys = new HashMap<Tree, SortedSet<KeyState>>();

    private int deferredIndexKeyLimit = MAX_INDEX_TRANCHE_SIZE;

    public PersistitStore(boolean updateGroupIndexes, TreeService treeService, ConfigurationService config) {
        this.updateGroupIndexes = updateGroupIndexes;
        this.treeService = treeService;
        this.config = config;
    }

    public synchronized void start() {
        tableStatusCache = treeService.getTableStatusCache();
        indexManager = new PersistitStoreIndexManager(this, treeService);
        rowDefCache = new RowDefCache(tableStatusCache);
        try {
            originalDisplayFilter = getDb().getManagement().getDisplayFilter();
            getDb().getManagement().setDisplayFilter(
                    new RowDataDisplayFilter(this, treeService,
                            originalDisplayFilter));
        } catch (RemoteException e) {
            throw new DisplayFilterSetException (e.getMessage());
        }
    }

    public synchronized void stop() {
        try {
            getDb().getManagement().setDisplayFilter(originalDisplayFilter);
        } catch (RemoteException e) {
            throw new DisplayFilterSetException (e.getMessage());
        }
        indexManager = null;
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

    public Persistit getDb() {
        return treeService.getDb();
    }

    public Exchange getExchange(final Session session, final RowDef rowDef) {
        final RowDef groupRowDef = rowDef.isGroupTable() ? rowDef
                                   : rowDefCache.getRowDef(rowDef.getGroupRowDefId());
        return treeService.getExchange(session, groupRowDef);
    }

    public Exchange getExchange(final Session session, final Index index) {
        return treeService.getExchange(session, (IndexDef)index.indexDef());
    }

    public Key getKey(Session session) throws PersistitException
    {
        return treeService.getKey(session);
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
        // Initialize the hkey being constructed
        long uniqueId = -1;
        PersistitKeyAppender hKeyAppender = new PersistitKeyAppender(hEx.getKey());
        hKeyAppender.key().clear();
        // Metadata for the row's table
        UserTable table = rowDef.userTable();
        FieldDef[] fieldDefs = rowDef.getFieldDefs();
        // Metadata and other state for the parent table
        RowDef parentRowDef = null;
        if (rowDef.getParentRowDefId() != 0) {
            parentRowDef = rowDefCache.getRowDef(rowDef.getParentRowDefId());
        }
        IndexToHKey indexToHKey = null;
        int i2hPosition = 0;
        Exchange parentPKExchange = null;
        boolean parentExists = false;
        // Nested loop over hkey metadata: All the segments of an hkey, and all
        // the columns of a segment.
        List<HKeySegment> hKeySegments = table.hKey().segments();
        int s = 0;
        while (s < hKeySegments.size()) {
            HKeySegment hKeySegment = hKeySegments.get(s++);
            // Write the ordinal for this segment
            RowDef segmentRowDef = rowDefCache.getRowDef(hKeySegment.table()
                    .getTableId());
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
                        Index parentPK = parentRowDef.getPKIndex();
                        indexToHKey = parentPK.indexToHKey();
                        parentPKExchange = getExchange(session, parentPK);
                        constructParentPKIndexKey(new PersistitKeyAppender(parentPKExchange.getKey()), rowDef, rowData);
                        parentExists = parentPKExchange.hasChildren();
                        if (parentExists) {
                            boolean hasNext = parentPKExchange.next(true);
                            assert hasNext : rowData;
                        }
                        // parent does not necessarily exist. rowData could be
                        // an orphan
                    }
                    if(indexToHKey.isOrdinal(i2hPosition)) {
                        assert indexToHKey.getOrdinal(i2hPosition) == segmentRowDef.getOrdinal() : hKeyColumn;
                        ++i2hPosition;
                    }
                    if (parentExists) {
                        appendKeyFieldFromKey(parentPKExchange.getKey(), hKeyAppender.key(),
                                              indexToHKey.getIndexRowPosition(i2hPosition));
                    }
                    else {
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
                        hKeyAppender.append(fieldDef, rowData);;
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
        PersistitKeyAppender appender = new PersistitKeyAppender(hEx.getKey());
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

    public static void constructIndexKey(PersistitKeyAppender iKeyAppender, RowData rowData, Index index, Key hKey)
    {
        IndexRowComposition indexRowComp = index.indexRowComposition();
        iKeyAppender.key().clear();
        for(int indexPos = 0; indexPos < indexRowComp.getLength(); ++indexPos) {
            if(indexRowComp.isInRowData(indexPos)) {
                int fieldPos = indexRowComp.getFieldPosition(indexPos);
                RowDef rowDef = ((IndexDef)index.indexDef()).getRowDef();
                iKeyAppender.append(rowDef.getFieldDef(fieldPos), rowData);
            }
            else if(indexRowComp.isInHKey(indexPos)) {
                appendKeyFieldFromKey(hKey, iKeyAppender.key(), indexRowComp.getHKeyPosition(indexPos));
            }
            else {
                throw new IllegalStateException("Invalid IndexRowComposition: " + indexRowComp);
            }
        }
    }

    public void constructHKeyFromIndexKey(final Key hKey, final Key indexKey, final Index index)
    {
        final IndexToHKey indexToHKey = index.indexToHKey();
        hKey.clear();
        for(int i = 0; i < indexToHKey.getLength(); ++i) {
            if(indexToHKey.isOrdinal(i)) {
                hKey.append(indexToHKey.getOrdinal(i));
            }
            else {
                final int depth = indexToHKey.getIndexRowPosition(i);
                if (depth < 0 || depth > indexKey.getDepth()) {
                    throw new IllegalStateException(
                            "IndexKey too shallow - requires depth=" + depth
                                    + ": " + indexKey);
                }
                appendKeyFieldFromKey(indexKey, hKey, depth);
            }
        }
    }

    void constructParentPKIndexKey(PersistitKeyAppender iKeyAppender, final RowDef rowDef, final RowData rowData) {
        iKeyAppender.key().clear();
        appendKeyFields(iKeyAppender, rowDef, rowData, rowDef.getParentJoinFields());
    }

    void appendKeyFields(PersistitKeyAppender appender, final RowDef rowDef,
            final RowData rowData, final int[] fields) {
        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            final FieldDef fieldDef = rowDef.getFieldDef(fields[fieldIndex]);
            appender.append(fieldDef, rowData);
        }
    }

    static private void appendKeyFieldFromKey(final Key fromKey, final Key toKey,
            final int depth) {
        fromKey.indexTo(depth);
        int from = fromKey.getIndex();
        fromKey.indexTo(depth + 1);
        int to = fromKey.getIndex();
        if (from >= 0 && to >= 0 && to > from) {
            System.arraycopy(fromKey.getEncodedBytes(), from,
                    toKey.getEncodedBytes(), toKey.getEncodedSize(), to - from);
            toKey.setEncodedSize(toKey.getEncodedSize() + to - from);
        }
    }

    // --------------------- Implement Store interface --------------------

    @Override
    public RowDefCache getRowDefCache() {
        return rowDefCache;
    }

    private static <T> T errorIfNull(String description, T object) {
        if (object == null) {
            throw new NullPointerException(description
                    + " is null; did you call startUp()?");
        }
        return object;
    }

    /**
     * WRites a row
     * 
     * @param rowData
     *            the row data
     * @throws PersistitException 
     */
    @Override
    public void writeRow(final Session session, final RowData rowData) throws PersistitException {
        final int rowDefId = rowData.getRowDefId();

        if (rowData.getRowSize() > MAX_ROW_SIZE) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("RowData size " + rowData.getRowSize() + " is larger than current limit of " + MAX_ROW_SIZE
                                 + " bytes");
            }
        }

        WRITE_ROW_TAP.in();
        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        checkNoGroupIndexes(rowDef.table());
        Exchange hEx;
        hEx = getExchange(session, rowDef);
        try {
            //
            // Does the heavy lifting of looking up the full hkey in
            // parent's primary index if necessary.
            //
            constructHKey(session, hEx, rowDef, rowData, true);
            if (hEx.isValueDefined()) {
                throw new DuplicateKeyException("PRIMARY", hEx.getKey());
            }

            packRowData(hEx, rowDef, rowData);
            // Store the h-row
            hEx.store();
            if (rowDef.isAutoIncrement()) {
                final long location = rowDef.fieldLocation(rowData, rowDef.getAutoIncrementField());
                if (location != 0) {
                    final long autoIncrementValue = rowData.getIntegerValue((int) location, (int) (location >>> 32));
                    tableStatusCache.setAutoIncrement(rowDefId, autoIncrementValue);
                }
            }
            tableStatusCache.rowWritten(rowDefId);

            for (Index index : rowDef.getIndexes()) {
                //
                // Insert the index keys (except for the case of a
                // root table's PK index.)
                //
                if (!index.isHKeyEquivalent()) {
                    insertIntoIndex(session, index, rowData, hEx.getKey(), deferIndexes);
                }
            }

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
            PersistitKeyAppender hKeyAppender = new PersistitKeyAppender(hKey);
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
            propagateDownGroup(session, hEx);

            if (deferredIndexKeyLimit <= 0) {
                putAllDeferredIndexKeys(session);
            }
        } finally {
            releaseExchange(session, hEx);
            WRITE_ROW_TAP.out();
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
    public void deleteRow(final Session session, final RowData rowData) throws PersistitException {
        DELETE_ROW_TAP.in();
        final int rowDefId = rowData.getRowDefId();

        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        checkNoGroupIndexes(rowDef.table());
        Exchange hEx = null;
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
            for (Index index : rowDef.getIndexes()) {
                if (!index.isHKeyEquivalent()) {
                    deleteIndex(session, index, rowData, hEx.getKey());
                }
            }

            // The row being deleted might be the parent of rows that
            // now become orphans. The hkeys
            // of these rows need to be maintained.
            propagateDownGroup(session, hEx);
        } finally {
            releaseExchange(session, hEx);
            DELETE_ROW_TAP.out();
        }
    }

    @Override
    public void updateRow(final Session session, final RowData oldRowData,
            final RowData newRowData, final ColumnSelector columnSelector) throws PersistitException {
        final int rowDefId = oldRowData.getRowDefId();

        if (newRowData.getRowDefId() != rowDefId) {
            throw new IllegalArgumentException("RowData values have different rowDefId values: ("
                                                       + rowDefId + "," + newRowData.getRowDefId() + ")");
        }
        UPDATE_ROW_TAP.in();
        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        checkNoGroupIndexes(rowDef.table());
        Exchange hEx = null;
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
            final RowData mergedRowData = columnSelector == null ? newRowData
                    : mergeRows(rowDef, currentRow, newRowData, columnSelector);
            // Verify that it hasn't changed. Note: at some point we
            // may want to optimize the protocol to send only PK and FK
            // fields in oldRowData, in which case this test will need
            // to change.
            if (!fieldsEqual(rowDef, oldRowData, mergedRowData, ((IndexDef)rowDef.getPKIndex().indexDef()).getFields())
                    || !fieldsEqual(rowDef, oldRowData, mergedRowData, rowDef.getParentJoinFields())) {
                deleteRow(session, oldRowData);
                writeRow(session, mergedRowData); // May throw DuplicateKeyException
            } else {
                packRowData(hEx, rowDef, mergedRowData);
                // Store the h-row
                hEx.store();

                // Update the indexes
                //
                for (Index index : rowDef.getIndexes()) {
                    if (!index.isHKeyEquivalent()) {
                        updateIndex(session, index, rowDef, currentRow, mergedRowData, hEx.getKey());
                    }
                }
            }
        } finally {
            releaseExchange(session, hEx);
            UPDATE_ROW_TAP.out();
        }
    }

    private void checkNoGroupIndexes(Table table) {
        if (updateGroupIndexes && !table.getGroupIndexes().isEmpty()) {
            throw new UnsupportedOperationException("PersistitStore can't update group indexes; found on " + table);
        }
    }

    private void propagateDownGroup(Session session, Exchange exchange)
            throws PersistitException {
        // exchange is positioned at a row R that has just been replaced by R',
        // (because we're processing an update
        // that has to be implemented as delete/insert). hKey is the hkey of R.
        // The replacement, R', is already present.
        // For each descendent* D of R, this method deletes and reinserts D.
        // Reinsertion of D causes its hkey to be
        // recomputed. This may depend on an ancestor being updated (if part of
        // D's hkey comes from the parent's
        // PK index). That's OK because updates are processed preorder, (i.e.,
        // ancestors before descendents).
        // This method will modify the state of exchange.
        //
        // * D is a descendent of R means that D is below R in the group. I.e.,
        // hkey(R) is a prefix of hkey(D).
        //
        // TODO: Optimizations
        // - Don't have to visit children that contain their own hkey
        // - Don't have to visit children whose hkey contains no changed column
        Key hKey = exchange.getKey();
        KeyFilter filter = new KeyFilter(hKey, hKey.getDepth() + 1,
                Integer.MAX_VALUE);
        RowData descendentRowData = new RowData(EMPTY_BYTE_ARRAY);
        while (exchange.next(filter)) {
            Value value = exchange.getValue();
            int descendentRowDefId = AkServerUtil.getInt(
                    value.getEncodedBytes(), RowData.O_ROW_DEF_ID
                            - RowData.LEFT_ENVELOPE_SIZE);
            RowDef descendentRowDef = rowDefCache.getRowDef(descendentRowDefId);
            expandRowData(exchange, descendentRowData);
            // Delete the current row from the tree
            exchange.remove();
            tableStatusCache.rowDeleted(descendentRowDefId);
            for (Index index : descendentRowDef.getIndexes()) {
                if (!index.isHKeyEquivalent()) {
                    deleteIndex(session, index, descendentRowData, exchange.getKey());
                }
            }
            // Reinsert it, recomputing the hkey
            writeRow(session, descendentRowData);
        }
    }

    /**
     * Remove data from the <b>entire group</b> that this RowDef ID is contained
     * in. This includes all table and index data for all user and group tables
     * in the group.
     * 
     * @param session
     *            Session to work on.
     * @param rowDefId
     *            RowDef ID to select group to truncate
     * @throws PersistitException 
     */
    @Override
    public void truncateGroup(final Session session, final int rowDefId) throws PersistitException {
        RowDef groupRowDef = rowDefCache.getRowDef(rowDefId);
        if (!groupRowDef.isGroupTable()) {
            groupRowDef = rowDefCache.getRowDef(groupRowDef.getGroupRowDefId());
        }

        //
        // Remove the index trees
        //
        for (RowDef userRowDef : groupRowDef.getUserTableRowDefs()) {
            for (Index index : userRowDef.getIndexes()) {
                removeIndexTree(session, index);
            }
        }
        for (Index index : groupRowDef.getGroupIndexes()) {
            removeIndexTree(session, index);
        }

        //
        // remove the htable tree
        //
        final Exchange hEx = getExchange(session, groupRowDef);
        hEx.removeAll();
        releaseExchange(session, hEx);
        for (int i = 0; i < groupRowDef.getUserTableRowDefs().length; i++) {
            final int childRowDefId = groupRowDef.getUserTableRowDefs()[i].getRowDefId();
            tableStatusCache.truncate(childRowDefId);
        }
    }

    protected final void removeIndexTree(Session session, Index index) {
        if (!index.isHKeyEquivalent()) {
            Exchange iEx = getExchange(session, index);
            try {
                iEx.removeAll();
            } catch (PersistitException e) {
                throw new PersistItErrorException (e);
            }
            releaseExchange(session, iEx);
        }

        // index analysis only exists on table indexes for now; if/when we analyze GIs, the if should be removed
        if (index.isTableIndex()) {
            try {
                indexManager.deleteIndexAnalysis(session, index);
            } catch (PersistitException e) {
                throw new PersistItErrorException (e);
            }
        }
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
        if(start != null && startColumns == null) {
            startColumns = createNonNullFieldSelector(start);
        }
        if(end != null && endColumns == null) {
            endColumns = createNonNullFieldSelector(end);
        }
        RowDef rowDef = checkRequest(rowDefId, start, startColumns, end, endColumns);
        RowCollector rc = OperatorBasedRowCollector.newCollector(config,
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
        NEW_COLLECTOR_TAP.out();
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

            indexManager.populateTableStatistics(session, ts);
        } catch (PersistitException e) {
            throw new PersistItErrorException (e);
        }
        return ts;
    }

    @Override
    public void analyzeTable(final Session session, final int tableId) {
        final RowDef rowDef = rowDefCache.getRowDef(tableId);
        indexManager.analyzeTable(session, rowDef);
    }

    @Override
    public void analyzeTable(Session session, int tableId, int sampleSize) {
        final RowDef rowDef = rowDefCache.getRowDef(tableId);
        indexManager.analyzeTable(session, rowDef, sampleSize);
    }

    boolean hasNullIndexSegments(final RowData rowData, final Index index) {
        IndexDef indexDef = (IndexDef)index.indexDef();
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

    void insertIntoIndex(final Session session, final Index index, final RowData rowData,
                         final Key hkey, final boolean deferIndexes) {
        checkNotGroupIndex(index);
        final Exchange iEx = getExchange(session, index);
        constructIndexKey(new PersistitKeyAppender(iEx.getKey()), rowData, index, hkey);

        checkUniqueness(index, rowData, iEx);

        iEx.getValue().clear();
        if (deferIndexes) {
            // TODO: bug767737, deferred indexing does not handle uniqueness
            synchronized (deferredIndexKeys) {
                SortedSet<KeyState> keySet = deferredIndexKeys.get(iEx.getTree());
                if (keySet == null) {
                    keySet = new TreeSet<KeyState>();
                    deferredIndexKeys.put(iEx.getTree(), keySet);
                }
                final KeyState ks = new KeyState(iEx.getKey());
                keySet.add(ks);
                deferredIndexKeyLimit -= (ks.getBytes().length + KEY_STATE_SIZE_OVERHEAD);
            }
        } else {
            try {
                iEx.store();
            } catch (PersistitException e) {
                throw new PersistItErrorException(e);
            }
        }
        releaseExchange(session, iEx);
    }

    private void checkUniqueness(Index index, RowData rowData, Exchange iEx)
    {
        if (index.isUnique() && !hasNullIndexSegments(rowData, index)) {
            final Key key = iEx.getKey();
            KeyState ks = new KeyState(key);
            key.setDepth(((IndexDef) index.indexDef()).getIndexKeySegmentCount());
            try {
                if (iEx.hasChildren()) {
                    throw new DuplicateKeyException(index.getIndexName().getName(), key);
                }
            } catch (PersistitException e) {
                throw new PersistItErrorException(e);
            }
            ks.copyTo(key);
        }
    }

    void putAllDeferredIndexKeys(final Session session) {
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

    public void updateIndex(final Session session, final Index index,
                            final RowDef rowDef, final RowData oldRowData,
                            final RowData newRowData, final Key hkey)
            throws PersistitException
    {
        checkNotGroupIndex(index);
        IndexDef indexDef = (IndexDef)index.indexDef();
        if (!fieldsEqual(rowDef, oldRowData, newRowData, indexDef.getFields())) {
            TABLE_INDEX_MAINTENANCE_TAP.in();
            try {
                Exchange oldExchange = getExchange(session, index);
                constructIndexKey(new PersistitKeyAppender(oldExchange.getKey()), oldRowData, index, hkey);
                Exchange newExchange = getExchange(session, index);
                constructIndexKey(new PersistitKeyAppender(newExchange.getKey()), newRowData, index, hkey);

                checkUniqueness(index, newRowData, newExchange);

                oldExchange.getValue().clear();
                newExchange.getValue().clear();

                oldExchange.remove();
                newExchange.store();

                releaseExchange(session, newExchange);
                releaseExchange(session, oldExchange);
            } finally {
                TABLE_INDEX_MAINTENANCE_TAP.out();
            }
        }
    }

    void deleteIndex(final Session session, final Index index, final RowData rowData, final Key hkey)
            throws PersistitException {
        checkNotGroupIndex(index);
        final Exchange iEx = getExchange(session, index);
        constructIndexKey(new PersistitKeyAppender(iEx.getKey()), rowData, index, hkey);
        boolean removed = iEx.remove();
        releaseExchange(session, iEx);
    }

    static boolean bytesEqual(final byte[] a, final int aoffset, final int asize,
            final byte[] b, final int boffset, final int bsize) {
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

    public static boolean fieldsEqual(final RowDef rowDef, final RowData a, final RowData b,
            final int[] fieldIndexes) {
        for (int index = 0; index < fieldIndexes.length; index++) {
            final int fieldIndex = fieldIndexes[index];
            final long aloc = rowDef.fieldLocation(a, fieldIndex);
            final long bloc = rowDef.fieldLocation(b, fieldIndex);
            if (!bytesEqual(a.getBytes(), (int) aloc, (int) (aloc >>> 32),
                    b.getBytes(), (int) bloc, (int) (bloc >>> 32))) {
                return false;
            }
        }
        return true;
    }

    public void packRowData(final Exchange hEx, final RowDef rowDef,
            final RowData rowData) {
        final int start = rowData.getInnerStart();
        final int size = rowData.getInnerSize();
        hEx.getValue().ensureFit(size);
        System.arraycopy(rowData.getBytes(), start, hEx.getValue()
                .getEncodedBytes(), 0, size);
        int storedTableId = treeService.aisToStore(rowDef,
                rowData.getRowDefId());
        AkServerUtil.putInt(hEx.getValue().getEncodedBytes(),
                RowData.O_ROW_DEF_ID - RowData.LEFT_ENVELOPE_SIZE,
                storedTableId);
        hEx.getValue().setEncodedSize(size);
    }

    public void expandRowData(final Exchange exchange, final RowData rowData) {
        // TODO this needs to be a more specific exception
        final int size = exchange.getValue().getEncodedSize();
        final int rowDataSize = size + RowData.ENVELOPE_SIZE;
        final byte[] valueBytes = exchange.getValue().getEncodedBytes();
        byte[] rowDataBytes = rowData.getBytes();

        if (rowDataSize < RowData.MINIMUM_RECORD_LENGTH
                || rowDataSize > RowData.MAXIMUM_RECORD_LENGTH) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Value at " + exchange.getKey()
                        + " is not a valid row - skipping");
            }
            throw new RowDataCorruptionException (exchange.getKey());
        }

        int rowDefId = AkServerUtil.getInt(valueBytes, RowData.O_ROW_DEF_ID
                - RowData.LEFT_ENVELOPE_SIZE);
        rowDefId = treeService.storeToAis(exchange.getVolume(), rowDefId);
        if (rowDataSize > rowDataBytes.length) {
            rowDataBytes = new byte[rowDataSize + INITIAL_BUFFER_SIZE];
            rowData.reset(rowDataBytes);
        }

        //
        // Assemble the Row in a byte array to allow column
        // elision
        //
        AkServerUtil.putInt(rowDataBytes, RowData.O_LENGTH_A, rowDataSize);
        AkServerUtil.putShort(rowDataBytes, RowData.O_SIGNATURE_A,
                RowData.SIGNATURE_A);
        System.arraycopy(valueBytes, 0, rowDataBytes, RowData.O_FIELD_COUNT,
                size);
        AkServerUtil.putShort(rowDataBytes, RowData.O_SIGNATURE_B + rowDataSize,
                RowData.SIGNATURE_B);
        AkServerUtil.putInt(rowDataBytes, RowData.O_LENGTH_B + rowDataSize,
                rowDataSize);
        AkServerUtil.putInt(rowDataBytes, RowData.O_ROW_DEF_ID, rowDefId);
        rowData.prepareRow(0);
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

    public void buildIndexes(final Session session, final Collection<? extends Index> indexes, final boolean defer) {
        flushIndexes(session);

        final Set<RowDef> userRowDefs = new HashSet<RowDef>();
        final Set<RowDef> groupRowDefs = new HashSet<RowDef>();
        final Set<Index> indexesToBuild = new HashSet<Index>();

        for(Index index : indexes) {
            IndexDef indexDef = (IndexDef)index.indexDef();
            if(indexDef == null) {
                throw new IllegalArgumentException("indexDef was null for index: " + index);
            }
            if(!index.isHKeyEquivalent()) {
                indexesToBuild.add(index);
                final RowDef rowDef = indexDef.getRowDef();
                userRowDefs.add(rowDef);
                final RowDef groupDef = rowDefCache.getRowDef(rowDef.getGroupRowDefId());
                if(groupDef != null) {
                    groupRowDefs.add(groupDef);
                }
            }
        }

        for (final RowDef rowDef : groupRowDefs) {
            final RowData rowData = new RowData(new byte[MAX_ROW_SIZE]);
            rowData.createRow(rowDef, new Object[0]);

            final byte[] columnBitMap = new byte[(rowDef.getFieldCount() + 7) / 8];
            // Project onto all columns of selected user tables
            for (final RowDef user : rowDef.getUserTableRowDefs()) {
                if (userRowDefs.contains(user)) {
                    for (int bit = 0; bit < user.getFieldCount(); bit++) {
                        final int c = bit + user.getColumnOffset();
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
                    final int tableId = rowData.getRowDefId();
                    final RowDef userRowDef = rowDefCache.getRowDef(tableId);
                    if (userRowDefs.contains(userRowDef)) {
                        for (Index index : userRowDef.getIndexes()) {
                            if(indexesToBuild.contains(index)) {
                                insertIntoIndex(session, index, rowData, hEx.getKey(), defer);
                                indexKeyCount++;
                            }
                        }
                        if (deferredIndexKeyLimit <= 0) {
                            putAllDeferredIndexKeys(session);
                        }
                    }
                }
            } catch (PersistitException e) {
                throw new PersistItErrorException (e);
            }
            flushIndexes(session);
            LOG.debug("Inserted {} index keys into {}", indexKeyCount, rowDef.table().getName());
        }
    }

    @Override
    public void removeTrees(Session session, Table table) {
        Exchange hEx = null;
        Exchange iEx = null;
        Collection<Index> indexes = new ArrayList<Index>();
        indexes.addAll(table.isUserTable() ? ((UserTable)table).getIndexesIncludingInternal() : table.getIndexes());
        indexes.addAll(table.getGroupIndexes());

        try {
            hEx = getExchange(session, (RowDef)table.rowDef());
            for(Index index : indexes) {
                if(!index.isHKeyEquivalent()) {
                    iEx = getExchange(session, index);
                    iEx.removeTree();
                    releaseExchange(session, iEx);
                    iEx = null;
                }
            }
            hEx.removeTree();
        } catch (PersistitException e) {
            throw new PersistItErrorException (e);
        } finally {
            if(hEx != null) {
                releaseExchange(session, hEx);
            }
            if(iEx != null) {
                releaseExchange(session, iEx);
            }
        }
    }

    public void flushIndexes(final Session session) {
        try {
            putAllDeferredIndexKeys(session);
        } catch (PersistItErrorException e) {
            LOG.debug("Exception while trying to flush deferred index keys", e);
            throw e;
        }
    }

    public void deleteIndexes(final Session session, final Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            final IndexDef indexDef = (IndexDef) index.indexDef();
            if(indexDef == null) {
                throw new IllegalArgumentException("indexDef is null for index: " + index);
            }
            try {
                Exchange iEx = getExchange(session, index);
                iEx.removeTree();
            } catch (PersistitException e) {
                LOG.debug("Exception while removing index tree: " + indexDef, e);
                throw new PersistItErrorException(e);
            }
        }
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
            throw new PersistItErrorException (e);
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

    private RowData mergeRows(RowDef rowDef, RowData currentRow,
            RowData newRowData, ColumnSelector columnSelector) {
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
        if (index.isHKeyEquivalent()) {
            throw new IllegalArgumentException("HKeyEquivalent not allowed: " + index);
        }
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

}
