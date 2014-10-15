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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import com.foundationdb.ais.model.*;
import com.foundationdb.server.AkServerUtil;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.TableStatistics;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.api.LegacyUtils;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.ConstantColumnSelector;
import com.foundationdb.server.api.dml.scan.BufferFullException;
import com.foundationdb.server.api.dml.scan.ColumnSet;
import com.foundationdb.server.api.dml.scan.Cursor;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.CursorState;
import com.foundationdb.server.api.dml.scan.LegacyOutputConverter;
import com.foundationdb.server.api.dml.scan.BufferedLegacyOutputRouter;
import com.foundationdb.server.api.dml.scan.LegacyRowOutput;
import com.foundationdb.server.api.dml.scan.LegacyRowWrapper;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.api.dml.scan.RowDataLegacyOutputRouter;
import com.foundationdb.server.api.dml.scan.RowOutput;
import com.foundationdb.server.api.dml.scan.ScanAllRequest;
import com.foundationdb.server.api.dml.scan.ScanLimit;
import com.foundationdb.server.api.dml.scan.ScanRequest;
import com.foundationdb.server.rowdata.encoding.EncodingException;
import com.foundationdb.server.error.ConcurrentScanAndUpdateException;
import com.foundationdb.server.error.CursorIsFinishedException;
import com.foundationdb.server.error.CursorIsUnknownException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.OldAISException;
import com.foundationdb.server.error.RowOutputException;
import com.foundationdb.server.error.ScanRetryAbandonedException;
import com.foundationdb.server.error.TableDefinitionChangedException;
import com.foundationdb.server.error.TableDefinitionMismatchException;
import com.foundationdb.server.service.dxl.BasicDXLMiddleman.ScanData;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.RowCollector;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.GrowableByteBuffer;
import com.google.inject.Inject;
import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BasicDMLFunctions extends ClientAPIBase implements DMLFunctions {

    private static final ColumnSelector ALL_COLUMNS_SELECTOR = ConstantColumnSelector.ALL_ON;
    private static final AtomicLong cursorsCount = new AtomicLong();

    private final static Logger logger = LoggerFactory.getLogger(BasicDMLFunctions.class);
    private final DDLFunctions ddlFunctions;
    private final IndexStatisticsService indexStatisticsService;
    private final ListenerService listenerService;
    private final Scanner scanner;
    private static final int SCAN_RETRY_COUNT = 0;

    @Inject
    BasicDMLFunctions(BasicDXLMiddleman middleman, SchemaManager schemaManager, Store store, DDLFunctions ddlFunctions,
                      IndexStatisticsService indexStatisticsService, ListenerService listenerService) {
        super(middleman, schemaManager, store);
        this.ddlFunctions = ddlFunctions;
        this.indexStatisticsService = indexStatisticsService;
        this.listenerService = listenerService;
        this.scanner = new Scanner();
    }

    @Override
    public TableStatistics getTableStatistics(Session session, int tableId, boolean updateFirst)
    {
        logger.trace("stats for {} updating: {}", tableId, updateFirst);
        Table table = ddlFunctions.getTable(session, tableId);
        if (updateFirst) {
            ddlFunctions.updateTableStatistics(session, table.getName(), null);
        }
        return indexStatisticsService.getTableStatistics(session, table);
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
        Set<Integer> allColumns = new HashSet<>(table.getColumns().size());
        for (Column column : table.getColumns())
        {
            allColumns.add(column.getPosition());
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
            scanner.doScan(cursor, cursorId, output);
        } catch (RollbackException e) {
            logger.trace("PersistIt error; aborting", e);
            output.rewind();
            throw new ScanRetryAbandonedException(SCAN_RETRY_COUNT);
        }
    }

    private static class PooledConverter {
        private final LegacyOutputConverter converter;
        private final BufferedLegacyOutputRouter router;

        public PooledConverter(DMLFunctions dmlFunctions) {
            router = new BufferedLegacyOutputRouter(1024 * 1024, true);
            converter = new LegacyOutputConverter(dmlFunctions);
            router.addHandler(converter);
        }

        public LegacyRowOutput getLegacyOutput() {
            return router;
        }

        public void setConverter(Session session, RowOutput output, Set<Integer> columns) {
            converter.reset(session, output, columns);
        }

        public void clearConverter() {
            converter.clearSession();
        }
    }

    private final BlockingQueue<PooledConverter> convertersPool = new LinkedBlockingDeque<>();

    private PooledConverter getPooledConverter(Session session, RowOutput output, Set<Integer> columns) {
        PooledConverter converter = convertersPool.poll();
        if (converter == null) {
            logger.debug("Allocating new PooledConverter");
            converter = new PooledConverter(this);
        }
        try {
            converter.setConverter(session, output, columns);
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
        which.clearConverter();
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
        logger.trace("scanning from {}", cursorId);
        final ScanData scanData = getScanData(session, cursorId);
        assert scanData != null;
        Set<Integer> scanColumns = scanData.scanAll() ? null : scanData.getScanColumns();
        final PooledConverter converter = getPooledConverter(session, output, scanColumns);
        try {
            scanSome(session, cursorId, converter.getLegacyOutput());
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
         */
        protected void doScan(Cursor cursor,
                              CursorId cursorId,
                              LegacyRowOutput output)
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
                    collectRowsIntoBuffer(cursor, output, limit);
                } else {
                    collectRows(cursor, output, limit);
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
        private void collectRowsIntoBuffer(Cursor cursor, LegacyRowOutput output, ScanLimit limit)
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
        private void collectRows(Cursor cursor, LegacyRowOutput output, ScanLimit limit)
        {
            RowCollector rc = cursor.getRowCollector();
            rc.outputToMessage(false);
            while (!cursor.isFinished()) {
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

        List<NewRow> converted = new ArrayList<>(rowDatas.size());
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
    public void writeRow(Session session, NewRow row)
    {
        logger.trace("writing a row");
        store().writeNewRow(session, row);
    }

    @Override
    public void writeRows(Session session, List<RowData> rows) {
        logger.trace("writing {} rows", rows.size());
        for(RowData rowData : rows) {
            store().writeRow(session, rowData);
        }
    }

    @Override
    public void deleteRow(Session session, NewRow row, boolean cascadeDelete)
    {
        logger.trace("deleting a row (cascade: {})", cascadeDelete);
        final RowData rowData = niceRowToRowData(row);
        store().deleteRow(session, rowData, cascadeDelete);
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

        store().updateRow(session, oldData, newData, columnSelector);
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
                    scanTableId = ddlFunctions.getTable(session, scanTableId).getParentTable().getTableId();
                }
            }
            else {
                TableIndex index = rc.getPredicateIndex();
                if (index == null) {
                    index = ddlFunctions.getRowDef(session, rc.getTableId()).getPKIndex();
                }
                if (index != null) {
                    if (index.getTable().getTableId() != tableId) {
                        continue;
                    }
                    int nkeys = index.getKeyColumns().size();
                    IndexRowComposition indexRowComposition = index.indexRowComposition();
                    for (int i = 0; i < nkeys; i++) {
                        int field = indexRowComposition.getFieldPosition(i);
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
        Table table = ddlFunctions.getAIS(session).getTable(tableId);
        HKey hKey = table.hKey();
        for (HKeySegment segment : hKey.segments()) {
            for (HKeyColumn hKeyColumn : segment.columns()) {
                Column column = hKeyColumn.column();
                if (column.getTable() != table) {
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
     * Determine if a Table can be truncated 'quickly' through the Store interface.
     * This is possible if the entire group can be truncated. Specifically, all other
     * tables in the group must have no rows.
     * @param session Session to operation on
     * @param table Table to determine if a fast truncate is possible on
     * @param descendants <code>true</code> to ignore descendants of
     * <code>table</code> in the check
     * @return true if store.truncateGroup() used, false otherwise
     * @throws Exception 
     */
    private boolean canFastTruncate(Session session, Table table, boolean descendants) {
        if(!table.getFullTextIndexes().isEmpty()) {
            return false;
        }
        List<Table> tableList = new ArrayList<>();
        tableList.add(table.getGroup().getRoot());
        while(!tableList.isEmpty()) {
            Table aTable = tableList.remove(tableList.size() - 1);
            if(aTable != table) {
                TableStatistics stats = getTableStatistics(session, aTable.getTableId(), false);
                if(stats.getRowCount() > 0) {
                    return false;
                }
            }
            if((aTable != table) || !descendants) {
                for(Join join : aTable.getChildJoins()) {
                    tableList.add(join.getChild());
                }
            }
        }
        return true;
    }

    @Override
    public void truncateTable(final Session session, final int tableId)
    {
        truncateTable(session, tableId, false);
    }

    @Override
    public void truncateTable(final Session session, final int tableId, final boolean descendants)
    {
        logger.trace("truncating tableId={}", tableId);
        final int knownAIS = ddlFunctions.getGenerationAsInt(session);
        final TableName name = ddlFunctions.getTableName(session, tableId);
        final Table utable = ddlFunctions.getTable(session, name);

        if(canFastTruncate(session, utable, descendants)) {
            store().truncateGroup(session, utable.getGroup());
            // All other tables in the group have no rows. Only need to truncate this table.
            for(TableListener listener : listenerService.getTableListeners()) {
                listener.onTruncate(session, utable, true);
            }
            return;
        }

        slowTruncate(session, knownAIS, utable, tableId, descendants);
    }

    private void slowTruncate(final Session session, final int knownAIS, 
                              final Table utable, final int tableId,
                              final boolean descendants) {
        if (descendants) {
            for(Join join : utable.getChildJoins()) {
                Table ctable = join.getChild();
                slowTruncate(session, knownAIS, ctable, ctable.getTableId(), descendants);
            }
        }

        // We can't do a "fast truncate" for whatever reason, so we have to delete row by row
        // (one reason is orphan row maintenance). Do so with a full table scan.

        // Store.deleteRow() requires all index columns to be in the passed RowData to properly clean everything up
        Set<Integer> keyColumns = new HashSet<>();
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
                deleteRow(session, rowWrapper, false);
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
        store().truncateTableStatus(session, tableId);

        for(TableListener listener : listenerService.getTableListeners()) {
            listener.onTruncate(session, utable, false);
        }

        if (thrown != null) {
            throw thrown;
        }
    }
}
