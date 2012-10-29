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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import com.akiban.ais.model.*;
import com.akiban.server.AkServerUtil;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.LegacyUtils;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.ColumnSet;
import com.akiban.server.api.dml.scan.Cursor;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.CursorState;
import com.akiban.server.api.dml.scan.LegacyOutputConverter;
import com.akiban.server.api.dml.scan.BufferedLegacyOutputRouter;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.RowDataLegacyOutputRouter;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.encoding.EncodingException;
import com.akiban.server.error.ConcurrentScanAndUpdateException;
import com.akiban.server.error.CursorIsFinishedException;
import com.akiban.server.error.CursorIsUnknownException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.OldAISException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.error.RowOutputException;
import com.akiban.server.error.ScanRetryAbandonedException;
import com.akiban.server.error.TableDefinitionChangedException;
import com.akiban.server.error.TableDefinitionMismatchException;
import com.akiban.server.service.dxl.BasicDXLMiddleman.ScanData;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.RowCollector;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.GrowableByteBuffer;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
import com.google.inject.Inject;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BasicDMLFunctions extends ClientAPIBase implements DMLFunctions {

    private static final ColumnSelector ALL_COLUMNS_SELECTOR = ConstantColumnSelector.ALL_ON;
    private static final AtomicLong cursorsCount = new AtomicLong();

    private final static Logger logger = LoggerFactory.getLogger(BasicDMLFunctions.class);
    private final DDLFunctions ddlFunctions;
    private final Scanner scanner;
    private static final int SCAN_RETRY_COUNT = 0;

    private static PointTap SCAN_RETRY_ABANDON_TAP = Tap.createCount("BasicDMLFunctions: scan abandons");

    @Inject
    BasicDMLFunctions(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store, TreeService treeService, DDLFunctions ddlFunctions) {
        super(middleman, schemaManager, store, treeService);
        this.ddlFunctions = ddlFunctions;
        this.scanner = new Scanner();
    }

    interface ScanHooks {
        void loopStartHook();
        void preWroteRowHook();
        void scanSomeFinishedWellHook();
    }

    static final ScanHooks DEFAULT_SCAN_HOOK = new ScanHooks() {
        @Override
        public void loopStartHook() {
        }

        @Override
        public void preWroteRowHook() {
        }

        @Override
        public void scanSomeFinishedWellHook() {
        }
    };

    @Override
    public TableStatistics getTableStatistics(Session session, int tableId,
            boolean updateFirst) 
    {
        logger.trace("stats for {} updating: {}", tableId, updateFirst);
        if (updateFirst) {
            ddlFunctions.updateTableStatistics(session,
                                               ddlFunctions.getTableName(session, tableId),
                                               null);
        }
        return store().getTableStatistics(session, tableId);
    }

    @Override
    public CursorId openCursor(Session session, int knownAIS, ScanRequest request)
    {
        checkAISGeneration(session, knownAIS);
        logger.trace("opening scan:    {} -> {}", System.identityHashCode(request), request);
        if (request.scanAllColumns()) {
            request = scanAllColumns(session, request);
        }
        final CursorId cursorId = newUniqueCursor(session.sessionId(), request.getTableId());
        reopen(session, cursorId, request, true);

        // double check our AIS generation. This is a bit superfluous since we're supposed to be in a DDL-DML r/w lock.
        checkAISGeneration(knownAIS, session, cursorId);
        logger.trace("cursor for scan: {} -> {}", System.identityHashCode(request), cursorId);
        return cursorId;
    }

    private void checkAISGeneration(int knownGeneration, Session session, CursorId cursorId) throws OldAISException {
        try {
            checkAISGeneration(session, knownGeneration);
        } catch (OldAISException e) {
            closeCursor(session, cursorId);
            throw e;
        }
    }

    private void checkAISGeneration(Session session, int knownGeneration) throws OldAISException {
        int currentGeneration = ddlFunctions.getGenerationAsInt(session);
        if (currentGeneration != knownGeneration) {
            throw new OldAISException(knownGeneration, currentGeneration);
        }
    }

    private Cursor reopen(Session session, CursorId cursorId, ScanRequest request, boolean mustBeFresh)
    {
        final RowCollector rc = getRowCollector(session, request);
        final Cursor cursor = new Cursor(rc, request.getScanLimit(), request);
        Object old = putScanData(session, cursorId, new ScanData(request, cursor));
        if (mustBeFresh) {
            assert old == null : old;
        }
        
        return cursor;
    }

    private ScanRequest scanAllColumns(final Session session, final ScanRequest request) {
        Table table = ddlFunctions.getAIS(session).getTable(
                ddlFunctions.getTableName(session, request.getTableId())
        );
        final int colsCount = table.getColumns().size();
        Set<Integer> allColumns = new HashSet<Integer>(colsCount);
        for (int i = 0; i < colsCount; ++i) {
            allColumns.add(i);
        }
        final byte[] allColumnsBytes = ColumnSet.packToLegacy(allColumns);
        return new ScanRequest() {
            @Override
            public int getIndexId() {
                return request.getIndexId();
            }

            @Override
            public int getScanFlags() {
                return request.getScanFlags();
            }

            @Override
            public RowData getStart() {
                return request.getStart();
            }

            @Override
            public ColumnSelector getStartColumns() {
                return null;
            }

            @Override
            public RowData getEnd() {
                return request.getEnd();
            }

            @Override
            public ColumnSelector getEndColumns() {
                return null;
            }

            @Override
            public byte[] getColumnBitMap() {
                return allColumnsBytes;
            }

            @Override
            public int getTableId() {
                return request.getTableId();
            }

            @Override
            public ScanLimit getScanLimit() {
                return ScanLimit.NONE;
            }

            @Override
            public void dropScanLimit() {
            }

            @Override
            public boolean scanAllColumns() {
                return true;
            }
        };
    }

    protected RowCollector getRowCollector(Session session, ScanRequest request) {
        RowCollector rowCollector = store().newRowCollector(session,
                                                            request.getScanFlags(),
                                                            request.getTableId(),
                                                            request.getIndexId(),
                                                            request.getColumnBitMap(),
                                                            request.getStart(),
                                                            request.getStartColumns(),
                                                            request.getEnd(),
                                                            request.getEndColumns(),
                                                            request.getScanLimit());
        if (rowCollector.checksLimit()) {
            request.dropScanLimit();
        }
        return rowCollector;
    }

    protected CursorId newUniqueCursor(long sessionId, int tableId) {
        return new CursorId(sessionId, cursorsCount.incrementAndGet(), tableId);
    }

    @Override
    public CursorState getCursorState(Session session, CursorId cursorId) {
        final ScanData extraData = getScanData(session, cursorId);
        if (extraData == null || extraData.getCursor() == null) {
            return CursorState.UNKNOWN_CURSOR;
        }
        return extraData.getCursor().getState();
    }


    @Override
    public void scanSome(Session session, CursorId cursorId, LegacyRowOutput output) throws BufferFullException
    {
        scanSome(session, cursorId, output, DEFAULT_SCAN_HOOK);
    }

    void scanSome(Session session, CursorId cursorId, LegacyRowOutput output, ScanHooks scanHooks) 
        throws BufferFullException
    {
        logger.trace("scanning from {}", cursorId);
        ArgumentValidation.notNull("cursor", cursorId);
        ArgumentValidation.notNull("output", output);

        Cursor cursor = getScanData(session, cursorId).getCursor();
        if (cursor == null) {
            throw new CursorIsUnknownException(cursorId.getTableId());
        }
        if (CursorState.CONCURRENT_MODIFICATION.equals(cursor.getState())) {
            throw new ConcurrentScanAndUpdateException(cursorId);
        }
        if (CursorState.DDL_MODIFICATION.equals(cursor.getState())) {
            throw new TableDefinitionChangedException(cursorId);
        }
        if (cursor.isFinished()) {
            throw new CursorIsFinishedException(cursorId);
        }
        
        output.mark();
        try {
            if (!cursor.getRowCollector().hasMore()) { // this implies it's a freshly opened CursorId
                assert CursorState.FRESH.equals(cursor.getState()) : cursor.getState();
                cursor.getRowCollector().open();
            }
            scanner.doScan(cursor, cursorId, output, scanHooks);
            scanHooks.scanSomeFinishedWellHook();
        } catch (RollbackException e) {
            logger.trace("PersistIt error; aborting", e);
            output.rewind();
            SCAN_RETRY_ABANDON_TAP.hit();
            throw new ScanRetryAbandonedException(SCAN_RETRY_COUNT);
        }
    }

    private static class PooledConverter {
        private final LegacyOutputConverter converter;
        private final BufferedLegacyOutputRouter router;

        public PooledConverter(Session session, DMLFunctions dmlFunctions) {
            router = new BufferedLegacyOutputRouter(1024 * 1024, true);
            converter = new LegacyOutputConverter(session, dmlFunctions);
            router.addHandler(converter);
        }

        public LegacyRowOutput getLegacyOutput() {
            return router;
        }

        public void setConverter(RowOutput output, Set<Integer> columns) {
            converter.setOutput(output);
            converter.setColumnsToScan(columns);
        }
    }

    private final BlockingQueue<PooledConverter> convertersPool = new LinkedBlockingDeque<PooledConverter>();

    private PooledConverter getPooledConverter(Session session, RowOutput output, Set<Integer> columns) {
        PooledConverter converter = convertersPool.poll();
        if (converter == null) {
            logger.debug("Allocating new PooledConverter");
            converter = new PooledConverter(session, this);
        }
        try {
            converter.setConverter(output, columns);
        } catch (NoSuchTableException e) {
            releasePooledConverter(converter);
            throw e;
        } catch (RuntimeException e) {
            releasePooledConverter(converter);
            throw e;
        }
        return converter;
    }

    private void releasePooledConverter(PooledConverter which) {
        if (!convertersPool.offer(which)) {
            logger.warn("Failed to release PooledConverter "
                    + which
                    + " to pool. "
                    + "This could result in superfluous allocations, and may happen because too many pools are being "
                    + "released at the same time.");
        }
    }

    @Override
    public void scanSome(Session session, CursorId cursorId, RowOutput output)
    {
        scanSome(session, cursorId, output, DEFAULT_SCAN_HOOK);
    }

    public void scanSome(Session session, CursorId cursorId, RowOutput output, ScanHooks scanHooks)
    {
        logger.trace("scanning from {}", cursorId);
        final ScanData scanData = getScanData(session, cursorId);
        assert scanData != null;
        Set<Integer> scanColumns = scanData.scanAll() ? null : scanData.getScanColumns();
        final PooledConverter converter = getPooledConverter(session, output, scanColumns);
        try {
            scanSome(session, cursorId, converter.getLegacyOutput(), scanHooks);
        }
        catch (BufferFullException e) {
            throw new RowOutputException(converter.getLegacyOutput().getRowsCount());
        } finally {
            releasePooledConverter(converter);
        }
    }

    static class Scanner {
        /**
         * Do the actual scan. Refactored out of scanSome for ease of unit testing.
         *
         * @param cursor
         *            the cursor itself; used to check status and get a row
         *            collector
         * @param cursorId
         *            the cursor id; used only to report errors
         * @param output
         *            the output; see
         *            {@link #scanSome(Session, CursorId, LegacyRowOutput)}
         * @param scanHooks the scan hooks to use
         * @throws Exception 
         * @see #scanSome(Session, CursorId, LegacyRowOutput)
         */
        protected void doScan(Cursor cursor,
                                        CursorId cursorId,
                                        LegacyRowOutput output,
                                        ScanHooks scanHooks)
            throws BufferFullException
        {
            assert cursor != null;
            assert cursorId != null;
            assert output != null;

            if (cursor.isClosed()) {
                logger.error("Shouldn't have gotten a closed cursor. id = {} state = {}", cursorId, cursor.getState());
                throw new CursorIsFinishedException(cursorId);
            }

            final RowCollector rc = cursor.getRowCollector();
            final ScanLimit limit = cursor.getLimit();
            try {
                if (!rc.hasMore()) {
                    cursor.setFinished();
                    return;
                }
                cursor.setScanning();
                if (output.getOutputToMessage()) {
                    collectRowsIntoBuffer(cursor, output, limit, scanHooks);
                } else {
                    collectRows(cursor, output, limit, scanHooks);
                }
                assert cursor.isFinished();
            } catch (BufferFullException e) {
                throw e; // Don't want this to be handled as an Exception
            } catch (RollbackException e) {
                throw e; // Pass this up to be handled in scanSome
            } catch (InvalidOperationException e) {
                cursor.setFinished();
                throw e;
            }
        }

        // Returns true if cursor ran out of rows before reaching the limit, false otherwise.
        private void collectRowsIntoBuffer(Cursor cursor, LegacyRowOutput output, ScanLimit limit, ScanHooks scanHooks)
            throws BufferFullException
        {
            RowCollector rc = cursor.getRowCollector();
            rc.outputToMessage(true);
            GrowableByteBuffer buffer = output.getOutputBuffer();
            if (!buffer.hasArray()) {
                throw new IllegalArgumentException("buffer must have array");
            }
            boolean limitReached = false;
            while (!limitReached && !cursor.isFinished()) {
                scanHooks.loopStartHook();
                int bufferLastPos = buffer.position();
                if (!rc.collectNextRow(buffer)) {
                    if (rc.hasMore()) {
                        throw new BufferFullException();
                    }
                    cursor.setFinished();
                } else {
                    int bufferPos = buffer.position();
                    assert bufferPos > bufferLastPos : String.format("false: %d >= %d", bufferPos, bufferLastPos);
                    RowData rowData = getRowData(buffer.array(), bufferLastPos, bufferPos - bufferLastPos);
                    limitReached = limit.limitReached(rowData);
                    scanHooks.preWroteRowHook();
                    output.wroteRow(limitReached);
                    if (limitReached || !rc.hasMore()) {
                        cursor.setFinished();
                    }
                }
            }
        }

        protected RowData getRowData(byte[] bytes, int offset, int length) {
            RowData rowData = new RowData(bytes, offset, length);
            rowData.prepareRow(offset);
            return rowData;
        }

        // Returns true if cursor ran out of rows before reaching the limit, false otherwise.
        private void collectRows(Cursor cursor, LegacyRowOutput output, ScanLimit limit, ScanHooks scanHooks)
        {
            RowCollector rc = cursor.getRowCollector();
            rc.outputToMessage(false);
            while (!cursor.isFinished()) {
                scanHooks.loopStartHook();
                RowData rowData = rc.collectNextRow();
                if (rowData == null || (!rc.checksLimit() && limit.limitReached(rowData))) {
                    cursor.setFinished();
                }
                else {
                    // Copy rowData because further query processing will reuse underlying buffer.
                    output.addRow(rowData.copy());
                }
            }
        }
    }

    @Override
    public void closeCursor(Session session, CursorId cursorId)
            throws CursorIsUnknownException
    {
        logger.trace("closing cursor {}", cursorId);
        ArgumentValidation.notNull("cursor ID", cursorId);
        final ScanData scanData = removeScanData(session, cursorId);
        if (scanData == null) {
            throw new CursorIsUnknownException(cursorId.getTableId());
        }

        Cursor removedCursor = scanData.getCursor();
        removedCursor.getRowCollector().close();
    }

    @Override
    public Set<CursorId> getCursors(Session session) {
        Map<CursorId,ScanData> cursors = getScanDataMap(session);
        if (cursors == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(cursors.keySet());
    }

    @Override
    public RowData convertNewRow(NewRow row) {
        logger.trace("converting to RowData: {}", row);
        return row.toRowData();
    }

    @Override
    public NewRow wrapRowData(Session session, RowData rowData) {
        logger.trace("wrapping in NewRow: {}", rowData);
        RowDef rowDef = ddlFunctions.getRowDef(session, rowData.getRowDefId());
        return new LegacyRowWrapper(rowDef, rowData);
    }

    @Override
    public NewRow convertRowData(Session session, RowData rowData) {
        logger.trace("converting to NewRow: {}", rowData);
        RowDef rowDef = ddlFunctions.getRowDef(session, rowData.getRowDefId());
        return NiceRow.fromRowData(rowData, rowDef);
    }

    @Override
    public List<NewRow> convertRowDatas(Session session, List<RowData> rowDatas)
    {
        logger.trace("converting {} RowData(s) to NewRow", rowDatas.size());
        if (rowDatas.isEmpty()) {
            return Collections.emptyList();
        }

        List<NewRow> converted = new ArrayList<NewRow>(rowDatas.size());
        int lastRowDefId = -1;
        RowDef rowDef = null;
        for (RowData rowData : rowDatas) {
            int currRowDefId = rowData.getRowDefId();
            if ((rowDef == null) || (currRowDefId != lastRowDefId)) {
                lastRowDefId = currRowDefId;
                rowDef = ddlFunctions.getRowDef(session, currRowDefId);
            }
            converted.add(NiceRow.fromRowData(rowData, rowDef));
        }
        return converted;
    }

    @Override
    public Long writeRow(Session session, NewRow row)
    {
        logger.trace("writing a row");
        final RowData rowData = niceRowToRowData(row);
        try {
            store().writeRow(session, rowData);
        } catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
        }
        return null;
    }

    @Override
    public void deleteRow(Session session, NewRow row)
    {
        logger.trace("deleting a row");
        final RowData rowData = niceRowToRowData(row);
        try {
            store().deleteRow(session, rowData);
        } catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
        }
    }

    private RowData niceRowToRowData(NewRow row) 
    {
        try {
            return row.toRowData();
        } catch (EncodingException e) {
            throw new TableDefinitionMismatchException(e);
        }
    }

    @Override
    public void updateRow(Session session, NewRow oldRow, NewRow newRow, ColumnSelector columnSelector)
    {
        logger.trace("updating a row");
        final RowData oldData = niceRowToRowData(oldRow);
        final RowData newData = niceRowToRowData(newRow);

        final int tableId = LegacyUtils.matchRowDatas(oldData, newData);
        checkForModifiedCursors(
                session,
                oldRow, newRow,
                columnSelector == null ? ALL_COLUMNS_SELECTOR : columnSelector,
                tableId
        );

        try {
            store().updateRow(session, oldData, newData, columnSelector, null);
        } catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
        }
            
    }

    private void checkForModifiedCursors(
            Session session, NewRow oldRow, NewRow newRow, ColumnSelector columnSelector, int tableId) {
        boolean hKeyIsModified = isHKeyModified(session, oldRow, newRow, columnSelector, tableId);

        Map<CursorId,ScanData> cursorsMap = getScanDataMap(session);
        if (cursorsMap == null) {
            return;
        }
        for (ScanData scanData : cursorsMap.values()) {
            Cursor cursor = scanData.getCursor();
            if (cursor.isClosed()) {
                continue;
            }
            RowCollector rc = cursor.getRowCollector();
            if (hKeyIsModified) {
                // check whether the update is on this scan or its ancestors
                int scanTableId = rc.getTableId();
                while (scanTableId > 0) {
                    if (scanTableId == tableId) {
                        cursor.setScanModified();
                        break;
                    }
                    scanTableId = ddlFunctions.getRowDef(session, scanTableId).getParentRowDefId();
                }
            }
            else {
                IndexDef indexDef = rc.getIndexDef();
                if (indexDef == null) {
                    Index index = ddlFunctions.getRowDef(session, rc.getTableId()).getPKIndex();
                    indexDef = index != null ? index.indexDef() : null;
                }
                if (indexDef != null) {
                    assert indexDef.getIndex().isTableIndex();
                    TableIndex index = (TableIndex) indexDef.getIndex();
                    if (index.getTable().getTableId() != tableId) {
                        continue;
                    }
                    for (int field : indexDef.getFields()) {
                        if (columnSelector.includesColumn(field)
                                && !AkServerUtil.equals(oldRow.get(field), newRow.get(field)))
                        {
                            cursor.setScanModified();
                            break;
                        }
                    }
                }
            }
        }
    }

    private boolean isHKeyModified(Session session, NewRow oldRow, NewRow newRow, ColumnSelector columns, int tableId)
    {
        UserTable userTable = ddlFunctions.getAIS(session).getUserTable(tableId);
        HKey hKey = userTable.hKey();
        for (HKeySegment segment : hKey.segments()) {
            for (HKeyColumn hKeyColumn : segment.columns()) {
                Column column = hKeyColumn.column();
                if (column.getTable() != userTable) {
                    continue;
                }
                int pos = column.getPosition();
                if (columns.includesColumn(pos) && !AkServerUtil.equals(oldRow.get(pos), newRow.get(pos))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Determine if a UserTable can be truncated 'quickly' through the Store interface.
     * This is possible if the entire group can be truncated. Specifically, all other
     * tables in the group must have no rows.
     * @param session Session to operation on
     * @param userTable UserTable to determine if a fast truncate is possible on
     * @return true if store.truncateGroup() used, false otherwise
     * @throws Exception 
     */
    private boolean canFastTruncate(Session session, UserTable userTable) {
        List<UserTable> tableList = new ArrayList<UserTable>();
        tableList.add(userTable.getGroup().getRoot());
        while(!tableList.isEmpty()) {
            UserTable table = tableList.remove(tableList.size() - 1);
            if(table != userTable) {
                TableStatistics stats = getTableStatistics(session, table.getTableId(), false);
                if(stats.getRowCount() > 0) {
                    return false;
                }
            }
            for(Join join : table.getChildJoins()) {
                tableList.add(join.getChild());
            }
        }
        return true;
    }

    @Override
    public void truncateTable(final Session session, final int tableId)
    {
        logger.trace("truncating tableId={}", tableId);
        final int knownAIS = ddlFunctions.getGenerationAsInt(session);
        final Table table = ddlFunctions.getTable(session, tableId);
        final UserTable utable = table.isUserTable() ? (UserTable)table : null;

        if(utable == null || canFastTruncate(session, utable)) {
            try {
                store().truncateGroup(session, table.getGroup());
            } catch (PersistitException ex) {
                throw new PersistitAdapterException(ex);
            }
                
            return;
        }

        // We can't do a "fast truncate" for whatever reason, so we have to delete row by row
        // (one reason is orphan row maintenance). Do so with a full table scan.

        // Store.deleteRow() requires all index columns to be in the passed RowData to properly clean everything up
        Set<Integer> keyColumns = new HashSet<Integer>();
        for(Index index : utable.getIndexesIncludingInternal()) {
            for(IndexColumn col : index.getKeyColumns()) {
                int pos = col.getColumn().getPosition();
                keyColumns.add(pos);
            }
        }

        RowDataLegacyOutputRouter output = new RowDataLegacyOutputRouter();
        output.addHandler(new RowDataLegacyOutputRouter.Handler() {
            private LegacyRowWrapper rowWrapper = new LegacyRowWrapper();

            @Override
            public void handleRow(RowData rowData) {
                rowWrapper.setRowData(rowData);
                deleteRow(session, rowWrapper);
            }

            @Override
            public void mark() {
                // nothing to do
            }

            @Override
            public void rewind() {
                // nothing to do
            }
        });


        final CursorId cursorId;
        ScanRequest all = new ScanAllRequest(tableId, keyColumns);
        cursorId = openCursor(session, knownAIS, all);

        InvalidOperationException thrown = null;
        try {
            scanSome(session, cursorId, output);
        } catch (InvalidOperationException e) {
            throw e; 
        } catch (BufferFullException e) {
            throw new RuntimeException("Internal error, buffer full: " + e);
        } finally {
            try {
                closeCursor(session, cursorId);
            } catch (CursorIsUnknownException e) {
                thrown = e;
            }
        }
        try {
            store().truncateTableStatus(session, tableId);
        } catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
        }
        if (thrown != null) {
            throw thrown;
        }
    }
}
