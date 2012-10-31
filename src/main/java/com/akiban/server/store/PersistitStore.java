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

import com.akiban.ais.model.*;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.OperatorBasedRowCollector;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.*;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.collation.CString;
import com.akiban.server.collation.CStringKeyCoder;
import com.akiban.server.error.*;
import com.akiban.server.rowdata.*;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
import com.persistit.*;
import com.persistit.Management.DisplayFilter;
import com.persistit.encoding.CoderManager;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.*;

public class PersistitStore implements Store, Service {

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

    private final ConfigurationService config;

    private final TreeService treeService;

    private final SchemaManager schemaManager;

    private final LockService lockService;

    private TableStatusCache tableStatusCache;

    private DisplayFilter originalDisplayFilter;

    private volatile IndexStatisticsService indexStatistics;

    private final Map<Tree, SortedSet<KeyState>> deferredIndexKeys = new HashMap<Tree, SortedSet<KeyState>>();

    private int deferredIndexKeyLimit = MAX_INDEX_TRANCHE_SIZE;

    public PersistitStore(boolean updateGroupIndexes, TreeService treeService, ConfigurationService config,
                          SchemaManager schemaManager, LockService lockService) {
        this.updateGroupIndexes = updateGroupIndexes;
        this.treeService = treeService;
        this.config = config;
        this.schemaManager = schemaManager;
        this.lockService = lockService;
    }

    @Override
    public synchronized void start() {
        tableStatusCache = treeService.getTableStatusCache();
        try {
            CoderManager cm = getDb().getCoderManager();
            Management m = getDb().getManagement();
            cm.registerValueCoder(RowData.class, new RowDataValueCoder());
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
            getDb().getCoderManager().unregisterValueCoder(RowData.class);
            getDb().getCoderManager().unregisterKeyCoder(CString.class);
            getDb().getManagement().setDisplayFilter(originalDisplayFilter);
        } catch (RemoteException e) {
            throw new DisplayFilterSetException (e.getMessage());
        }
    }

    @Override
    public void crash() {
        stop();
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

    public Exchange getExchange(Session session, Group group) {
        return treeService.getExchange(session, group);
    }

    public Exchange getExchange(final Session session, final RowDef rowDef) {
        return treeService.getExchange(session, rowDef.getGroup());
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
        RowDef parentRowDef = rowDef.getParentRowDef();
        TableIndex parentPK = null;
        if (parentRowDef != null) {
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
            RowDef segmentRowDef = hKeySegment.table().rowDef();
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

    private static void constructIndexRow(Exchange exchange,
                                          RowData rowData,
                                          Index index,
                                          Key hKey,
                                          PersistitIndexRowBuffer indexRow,
                                          boolean forInsert) throws PersistitException
    {
        indexRow.resetForWrite(index, exchange.getKey(), exchange.getValue());
        indexRow.initialize(rowData, hKey);
        indexRow.close(forInsert);
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

    public AkibanInformationSchema getAIS(Session session) {
        return schemaManager.getAis(session);
    }

    public RowDef getRowDef(Session session, TableName tableName) {
        Table table = getAIS(session).getTable(tableName);
        if(table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table.rowDef();
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

        lockService.tableClaim(session, LockService.Mode.SHARED, rowDefId);

        final RowDef rowDef = rowDefFromExplicitOrId(session, rowData);
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
                    RowDef segmentRowDef = segment.table().rowDef();
                    hKey.append(segmentRowDef.getOrdinal());
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
                propagateDownGroup(session, hEx, tablesRequiringHKeyMaintenance, indexRow, true);
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
    public void deleteRow(Session session, RowData rowData)
        throws PersistitException
    {
        deleteRow(session, rowData, true);
        // TODO: It should be possible to optimize propagateDownGroup for inserts too
        // deleteRow(session, rowData, hKeyDependentTableOrdinals(rowData.getRowDefId()));
    }

    @Override
    public void deleteRow(Session session, RowData rowData, boolean deleteIndexes) throws PersistitException
    {
        deleteRow(session, rowData, deleteIndexes, null, true);
    }

    private void deleteRow(Session session, RowData rowData, boolean deleteIndexes,
                           BitSet tablesRequiringHKeyMaintenance, boolean propagateHKeyChanges)
        throws PersistitException
    {
        int rowDefId = rowData.getRowDefId();
        RowDef rowDef = rowDefFromExplicitOrId(session, rowData);
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

            // Remove the h-row
            hEx.remove();
            tableStatusCache.rowDeleted(rowDefId);

            // Remove the indexes, including the PK index
            PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(adapter(session));
            if(deleteIndexes) {
                for (Index index : rowDef.getIndexes()) {
                    deleteIndex(session, index, rowData, hEx.getKey(), indexRow);
                }
            }

            // The row being deleted might be the parent of rows that
            // now become orphans. The hkeys
            // of these rows need to be maintained.
            if(propagateHKeyChanges) {
                propagateDownGroup(session, hEx, tablesRequiringHKeyMaintenance, indexRow, deleteIndexes);
            }
        } finally {
            DELETE_ROW_TAP.out();
            releaseExchange(session, hEx);
        }
    }

    @Override
    public void updateRow(Session session,
                          RowData oldRowData,
                          RowData newRowData,
                          ColumnSelector columnSelector,
                          Index[] indexes)
        throws PersistitException
    {
        updateRow(session, oldRowData, newRowData, columnSelector, indexes, (indexes != null), true);
    }

    private void updateRow(Session session,
                           RowData oldRowData,
                           RowData newRowData,
                           ColumnSelector columnSelector,
                           Index[] indexesToMaintain,
                           boolean indexesAsInsert,
                           boolean propagateHKeyChanges)
        throws PersistitException
    {
        int rowDefId = oldRowData.getRowDefId();
        if (newRowData.getRowDefId() != rowDefId) {
            throw new IllegalArgumentException("RowData values have different rowDefId values: ("
                                                       + rowDefId + "," + newRowData.getRowDefId() + ")");
        }

        // RowDefs may be different (e.g. during an ALTER)
        // Only non-pk or grouping columns could have change in this scenario
        RowDef rowDef = rowDefFromExplicitOrId(session, oldRowData);
        RowDef newRowDef = rowDefFromExplicitOrId(session, newRowData);
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
            BitSet tablesRequiringHKeyMaintenance =
                    propagateHKeyChanges
                    ? analyzeFieldChanges(session, rowDef, oldRowData, mergedRowData)
                    : null;
            if (tablesRequiringHKeyMaintenance == null) {
                // No PK or FK fields have changed. Just update the row.
                packRowData(hEx, newRowDef, mergedRowData);
                // Store the h-row
                hEx.store();
                // Update the indexes
                PersistitAdapter adapter = adapter(session);
                PersistitIndexRowBuffer indexRowBuffer = new PersistitIndexRowBuffer(adapter);
                Index[] indexes = (indexesToMaintain == null) ? rowDef.getIndexes() : indexesToMaintain;
                for (Index index : indexes) {
                    if(indexesAsInsert) {
                        insertIntoIndex(session, index, mergedRowData, hEx.getKey(), indexRowBuffer, deferIndexes);
                    } else {
                        updateIndex(session, index, rowDef, currentRow, mergedRowData, hEx.getKey(), indexRowBuffer);
                    }
                }
            } else {
                // A PK or FK field has changed. The row has to be deleted and reinserted, and hkeys of descendent
                // rows maintained. tablesRequiringHKeyMaintenance contains the ordinals of the tables whose hkeys
                // could possible be affected.
                deleteRow(session, oldRowData, true, tablesRequiringHKeyMaintenance, true);
                writeRow(session, mergedRowData, tablesRequiringHKeyMaintenance, true); // May throw DuplicateKeyException
            }
        } finally {
            UPDATE_ROW_TAP.out();
            releaseExchange(session, hEx);
        }
    }

    private BitSet analyzeFieldChanges(Session session, RowDef rowDef, RowData oldRow, RowData newRow)
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
                                    PersistitIndexRowBuffer indexRowBuffer,
                                    boolean deleteIndexes)
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
            RowDef descendentRowDef = getRowDef(session, descendentRowDefId);
            int descendentOrdinal = descendentRowDef.getOrdinal();
            if ((tablesRequiringHKeyMaintenance == null || tablesRequiringHKeyMaintenance.get(descendentOrdinal))) {
                PROPAGATE_HKEY_CHANGE_ROW_REPLACE_TAP.hit();
                // Delete the current row from the tree. Don't call deleteRow, because we don't need to recompute
                // the hkey.
                exchange.remove();
                tableStatusCache.rowDeleted(descendentRowDefId);
                if(deleteIndexes) {
                    for (Index index : descendentRowDef.getIndexes()) {
                        deleteIndex(session, index, descendentRowData, exchange.getKey(), indexRowBuffer);
                    }
                }
                // Reinsert it, recomputing the hkey and maintaining indexes
                writeRow(session, descendentRowData, tablesRequiringHKeyMaintenance, false);
            }
        }
    }

    @Override
    public void dropGroup(Session session, Group group) throws PersistitException {
        for(Table table : group.getRoot().getAIS().getUserTables().values()) {
            if(table.getGroup() == group) {
                removeTrees(session, table);
            }
        }
        // tableStatusCache entries updated elsewhere
    }

    @Override
    public void truncateGroup(final Session session, final Group group) throws PersistitException {
        List<Index> indexes = new ArrayList<Index>();
        // Collect indexes, truncate table statuses
        for(UserTable table : group.getRoot().getAIS().getUserTables().values()) {
            if(table.getGroup() == group) {
                indexes.addAll(table.getIndexesIncludingInternal());
                tableStatusCache.truncate(table.getTableId());
            }
        }
        indexes.addAll(group.getIndexes());
        truncateIndexes(session, indexes);

        // Truncate the group tree
        final Exchange hEx = getExchange(session, group);
        hEx.removeAll();
        releaseExchange(session, hEx);
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
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
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
            RowDef rowDef = checkRequest(session, rowDefId, start, startColumns, end, endColumns);
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
        final RowDef rowDef = getRowDef(session, tableId);
        final TableStatistics ts = new TableStatistics(tableId);
        final TableStatus status = rowDef.getTableStatus();
        try {
            ts.setAutoIncrementValue(status.getAutoIncrement());
            ts.setRowCount(status.getRowCount());
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
        constructIndexRow(iEx, rowData, index, hkey, indexRow, true);
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
            // Check for children by traversing forward from the current key.
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

    private void updateIndex(Session session,
                             Index index,
                             RowDef rowDef,
                             RowData oldRowData,
                             RowData newRowData,
                             Key hKey,
                             PersistitIndexRowBuffer indexRowBuffer)
            throws PersistitException
    {
        checkNotGroupIndex(index);
        IndexDef indexDef = index.indexDef();
        if (!fieldsEqual(rowDef, oldRowData, newRowData, indexDef.getFields())) {
            TABLE_INDEX_MAINTENANCE_TAP.in();
            try {
                Exchange oldExchange = getExchange(session, index);
                deleteIndexRow(session, index, oldExchange, oldRowData, hKey, indexRowBuffer);
                Exchange newExchange = getExchange(session, index);
                constructIndexRow(newExchange, newRowData, index, hKey, indexRowBuffer, true);
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

    private void deleteIndexRow(Session session,
                                Index index,
                                Exchange exchange,
                                RowData rowData,
                                Key hKey,
                                PersistitIndexRowBuffer indexRowBuffer)
        throws PersistitException
    {
        // Non-unique index: The exchange's key has all fields of the index row. If there is such a row it will be
        //     deleted, if not, exchange.remove() does nothing.
        // PK index: The exchange's key has the key fields of the index row, and a null separator of 0. If there is
        //     such a row it will be deleted, if not, exchange.remove() does nothing. Because PK columns are NOT NULL,
        //     the null separator's value must be 0.
        // Unique index with no nulls: Like the PK case.
        // Unique index with nulls: isUniqueAndMayContainNulls is true. The exchange's key is written with the
        //     key of the index row. There may be duplicates due to nulls, and they will have different null separator
        //     values and the hkeys will differ. Look through these until the desired hkey is found, and delete that
        //     row. If the hkey is missing, then the row is already not present.
        PersistitAdapter adapter = adapter(session);
        if (index.isUniqueAndMayContainNulls()) {
            // Can't use a PIRB, because we need to get the hkey. Need a PersistitIndexRow.
            IndexRowType indexRowType = adapter.schema().indexRowType(index);
            PersistitIndexRow indexRow = adapter.takeIndexRow(indexRowType);
            constructIndexRow(exchange, rowData, index, hKey, indexRow, false);
            Key.Direction direction = Key.Direction.GTEQ;
            while (exchange.traverse(direction, true)) {
                indexRow.copyFromExchange(exchange); // Gets the current state of the exchange into oldIndexRow
                PersistitHKey rowHKey = (PersistitHKey) indexRow.hKey();
                if (rowHKey.key().compareTo(hKey) == 0) {
                    exchange.remove();
                    break;
                }
                direction = Key.Direction.GT;
            }
            adapter.returnIndexRow(indexRow);
        } else {
            constructIndexRow(exchange, rowData, index, hKey, indexRowBuffer, false);
            exchange.remove();
        }
    }

    private void deleteIndex(Session session,
                             Index index,
                             RowData rowData,
                             Key hkey,
                             PersistitIndexRowBuffer indexRowBuffer)
            throws PersistitException {
        checkNotGroupIndex(index);
        Exchange iEx = getExchange(session, index);
        deleteIndexRow(session, index, iEx, rowData, hkey, indexRowBuffer);
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
        int storedTableId = treeService.aisToStore(rowDef.getGroup(), rowData.getRowDefId());
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

    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean defer) {
        flushIndexes(session);
        Set<Group> groups = new HashSet<Group>();
        Map<Integer,RowDef> userRowDefs = new HashMap<Integer,RowDef>();
        Set<Index> indexesToBuild = new HashSet<Index>();
        for(Index index : indexes) {
            IndexDef indexDef = index.indexDef();
            if(indexDef == null) {
                throw new IllegalArgumentException("indexDef was null for index: " + index);
            }
            indexesToBuild.add(index);
            RowDef rowDef = indexDef.getRowDef();
            userRowDefs.put(rowDef.getRowDefId(), rowDef);
            groups.add(rowDef.table().getGroup());
        }
        PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(adapter(session));
        for (Group group : groups) {
            RowData rowData = new RowData(new byte[MAX_ROW_SIZE]);

            int indexKeyCount = 0;
            Exchange hEx = getExchange(session, group);
            try {
                hEx.clear();
                while (hEx.next(true)) {
                    expandRowData(hEx, rowData);
                    int tableId = rowData.getRowDefId();
                    RowDef userRowDef = userRowDefs.get(tableId);
                    if (userRowDef != null) {
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
            LOG.debug("Inserted {} index keys into group {}", indexKeyCount, group.getName());
        }
    }

    @Override
    public void removeTrees(Session session, Collection<? extends TreeLink> treeLinks) {
        try {
            for(TreeLink link : treeLinks) {
                if(!schemaManager.treeRemovalIsDelayed()) {
                    Exchange ex = treeService.getExchange(session, link);
                    ex.removeTree();
                    // Do not releaseExchange, causes caching and leak for now unused tree
                }
                schemaManager.treeWasRemoved(session, link.getSchemaName(), link.getTreeName());
            }
        } catch (PersistitException e) {
            LOG.debug("Exception removing tree from Persistit", e);
            throw new PersistitAdapterException(e);
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
        treeLinks.add(table.getGroup());
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
        List<TreeLink> links = new ArrayList<TreeLink>(indexes.size());
        for(Index index : indexes) {
            final IndexDef indexDef = index.indexDef();
            if(indexDef == null) {
                throw new IllegalStateException("indexDef is null for index: " + index);
            }
            links.add(indexDef);
        }
        removeTrees(session, links);
        indexStatistics.deleteIndexStatistics(session, indexes);
    }
    
    @Override
    public void deleteSequences (Session session, Collection<? extends Sequence> sequences) {
        removeTrees(session, sequences);
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
        NewRow newRow = new LegacyRowWrapper(rowDef, newRowData);
        int fields = rowDef.getFieldCount();
        for (int i = 0; i < fields; i++) {
            if (columnSelector.includesColumn(i)) {
                mergedRow.put(i, newRow.get(i));
            }
        }
        return mergedRow.toRowData();
    }

    private RowDef rowDefFromExplicitOrId(Session session, RowData rowData) {
        RowDef rowDef = rowData.getExplicitRowDef();
        if(rowDef == null) {
            rowDef = getRowDef(session, rowData.getRowDefId());
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

    public void traverse(Session session, Group group, TreeRecordVisitor visitor) throws PersistitException {
        Exchange exchange = getExchange(session, group);
        try {
            exchange.clear().append(Key.BEFORE);
            visitor.initialize(session, this, exchange);
            while (exchange.next(true)) {
                visitor.visit();
            }
        } finally {
            releaseExchange(session, exchange);
        }
    }

    public <V extends IndexVisitor> V traverse(Session session, Index index, V visitor) throws PersistitException {
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
        if(table.rowDef() != null) {
            ts = table.rowDef().getTableStatus();
        }
        return ts;
    }

    private static PersistitAdapter adapter(Session session)
    {
        return (PersistitAdapter) session.get(StoreAdapter.STORE_ADAPTER_KEY);
    }
}
