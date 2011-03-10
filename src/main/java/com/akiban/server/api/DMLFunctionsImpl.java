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

package com.akiban.server.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.dml.*;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.ColumnSet;
import com.akiban.server.api.dml.scan.Cursor;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.CursorIsFinishedException;
import com.akiban.server.api.dml.scan.CursorIsUnknownException;
import com.akiban.server.api.dml.scan.CursorState;
import com.akiban.server.api.dml.scan.LegacyOutputConverter;
import com.akiban.server.api.dml.scan.BufferedLegacyOutputRouter;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.RowDataLegacyOutputRouter;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.RowOutputException;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.encoding.EncodingException;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.stats.StatisticsService;
import com.akiban.server.store.RowCollector;
import com.akiban.server.util.RowDefNotFoundException;
import com.akiban.message.ErrorCode;
import com.akiban.util.ArgumentValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DMLFunctionsImpl extends ClientAPIBase implements DMLFunctions {

    private static final Class<?> MODULE_NAME = DMLFunctionsImpl.class;
    private static final AtomicLong cursorsCount = new AtomicLong();
    private static final Object OPEN_CURSORS_MAP = new Object();

    private final static Logger logger = LoggerFactory.getLogger(DMLFunctionsImpl.class);
    private final DDLFunctions ddlFunctions;

    public DMLFunctionsImpl(DDLFunctions ddlFunctions) {
        this.ddlFunctions = ddlFunctions;
    }

    @Override
    public TableStatistics getTableStatistics(Session session, int tableId,
            boolean updateFirst) throws NoSuchTableException,
            GenericInvalidOperationException
    {
        logger.trace("stats for {} updating: {}", tableId, updateFirst);
        try {
            if (updateFirst) {
                store().analyzeTable(session, tableId);
            }
            return store().getTableStatistics(session, tableId);
        } catch (Exception e) {
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(NoSuchTableException.class, ioe);
            throw new GenericInvalidOperationException(e);
        }
    }

    private static final class ScanData {
        private final Cursor cursor;
        private final byte[] scanColumns;
        private final boolean scanAll;
        private Set<Integer> scanColumnsUnpacked;

        ScanData(ScanRequest request, Cursor cursor) {
            scanColumns = request.getColumnBitMap();
            scanAll = request.scanAllColumns();
            this.cursor = cursor;
        }

        public Set<Integer> getScanColumns() {
            if (scanColumnsUnpacked == null) {
                scanColumnsUnpacked = ColumnSet.unpackFromLegacy(scanColumns);
            }
            return scanColumnsUnpacked;
        }

        public Cursor getCursor() {
            return cursor;
        }

        public boolean scanAll() {
            return scanAll;
        }

        @Override
        public String toString() {
            return String.format("ScanData[cursor=%s, columns=%s]", cursor,
                    getScanColumns());
        }
    }

    @Override
    public CursorId openCursor(Session session, ScanRequest request)
            throws NoSuchTableException, NoSuchColumnException,
            NoSuchIndexException, GenericInvalidOperationException
    {
        logger.trace("opening scan:    {} -> {}", System.identityHashCode(request), request);
        if (request.scanAllColumns()) {
            request = scanAllColumns(session, request);
        }
        final RowCollector rc = getRowCollector(session, request);
        final CursorId cursorId = newUniqueCursor(rc.getTableId());
        final Cursor cursor = new Cursor(rc);
        Object old = session.put(MODULE_NAME, cursorId, new ScanData(request, cursor));
        assert old == null : old;

        Map<CursorId,Cursor> cursors = session.get(MODULE_NAME, OPEN_CURSORS_MAP);
        if (cursors == null) {
            cursors = new HashMap<CursorId,Cursor>();
            session.put(MODULE_NAME, OPEN_CURSORS_MAP, cursors);
        }
        Cursor oldCursor = cursors.put(cursorId, cursor);
        assert oldCursor == null : String.format("%s -> %s conflicted with %s", cursor, cursors, oldCursor);
        logger.trace("cursor for scan: {} -> {}", System.identityHashCode(request), cursorId);
        return cursorId;
    }

    private ScanRequest scanAllColumns(final Session session, final ScanRequest request) throws NoSuchTableException {
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
            public RowData getStart()
                    throws NoSuchTableException {
                return request.getStart();
            }

            @Override
            public RowData getEnd()
                    throws NoSuchTableException {
                return request.getEnd();
            }

            @Override
            public byte[] getColumnBitMap() {
                return allColumnsBytes;
            }

            @Override
            public int getTableId()
                    throws NoSuchTableException {
                return request.getTableId();
            }

            @Override
            public boolean scanAllColumns() {
                return true;
            }
        };
    }

    protected RowCollector getRowCollector(Session session, ScanRequest request)
            throws NoSuchTableException, NoSuchColumnException,
            NoSuchIndexException, GenericInvalidOperationException {
        try {
            return store().newRowCollector(session,
                                           request.getTableId(),
                                           request.getIndexId(),
                                           request.getScanFlags(),
                                           request.getStart(),
                                           request.getEnd(),
                                           request.getColumnBitMap());
        } catch (RowDefNotFoundException e) {
            throw new NoSuchTableException(request.getTableId(), e);
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    protected CursorId newUniqueCursor(int tableId) {
        return new CursorId(cursorsCount.incrementAndGet(), tableId);
    }

    @Override
    public CursorState getCursorState(Session session, CursorId cursorId) {
        final ScanData extraData = session.get(MODULE_NAME, cursorId);
        if (extraData == null || extraData.getCursor() == null) {
            return CursorState.UNKNOWN_CURSOR;
        }
        return extraData.getCursor().getState();
    }

    @Override
    public boolean scanSome(Session session, CursorId cursorId, LegacyRowOutput output, int limit)
            throws CursorIsFinishedException,
                   CursorIsUnknownException,
                   RowOutputException,
                   BufferFullException,
                   GenericInvalidOperationException

    {
        logger.trace("scanning up to {} row(s) from {}", limit, cursorId);
        ArgumentValidation.notNull("cursor", cursorId);
        ArgumentValidation.notNull("output", output);

        final Cursor cursor = session.<ScanData> get(MODULE_NAME, cursorId).getCursor();
        if (cursor == null) {
            throw new CursorIsUnknownException(cursorId);
        }
        return doScan(cursor, cursorId, output, limit);
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

        public void setConverter(RowOutput output, Set<Integer> columns)
                throws NoSuchTableException {
            converter.setOutput(output);
            converter.setColumnsToScan(columns);
        }
    }

    private final BlockingQueue<PooledConverter> convertersPool = new LinkedBlockingDeque<PooledConverter>();

    private PooledConverter getPooledConverter(RowOutput output, Set<Integer> columns)
        throws NoSuchTableException {
        PooledConverter converter = convertersPool.poll();
        if (converter == null) {
            logger.debug("Allocating new PooledConverter");
            converter = new PooledConverter(this);
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
    public boolean scanSome(Session session, CursorId cursorId, RowOutput output, int limit)
        throws CursorIsFinishedException,
               CursorIsUnknownException,
               RowOutputException,
               NoSuchTableException,
               GenericInvalidOperationException
    {
        logger.trace("scanning up to {} row(s) from {}", limit, cursorId);
        final ScanData scanData = session.get(MODULE_NAME, cursorId);
        assert scanData != null;
        Set<Integer> scanColumns = scanData.scanAll() ? null : scanData.getScanColumns();
        final PooledConverter converter = getPooledConverter(output, scanColumns);
        try {
            return scanSome(session, cursorId, converter.getLegacyOutput(), limit);
        }
        catch (BufferFullException e) {
            throw new RowOutputException(e);
        } finally {
            releasePooledConverter(converter);
        }
    }

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
     *            {@link #scanSome(Session, CursorId, LegacyRowOutput , int)}
     * @param limit
     *            the limit, or negative value if none; ee
     *            {@link #scanSome(Session, CursorId, LegacyRowOutput , int)}
     * @return whether more rows remain to be scanned; see
     *         {@link #scanSome(Session, CursorId, LegacyRowOutput , int)}
     * @throws CursorIsFinishedException
     *             see
     *             {@link #scanSome(Session, CursorId, LegacyRowOutput , int)}
     * @throws RowOutputException
     *             see
     *             {@link #scanSome(Session, CursorId, LegacyRowOutput , int)}
     * @throws GenericInvalidOperationException
     *             see
     *             {@link #scanSome(Session, CursorId, LegacyRowOutput , int)}
     * @throws BufferFullException
     *             see
     *             {@link #scanSome(Session, CursorId, LegacyRowOutput , int)}
     * @see #scanSome(Session, CursorId, LegacyRowOutput , int)
     */
    protected static boolean doScan(Cursor cursor,
                                    CursorId cursorId,
                                    LegacyRowOutput output,
                                    int limit)
            throws CursorIsFinishedException,
                   RowOutputException,
                   GenericInvalidOperationException,
                   BufferFullException {
        assert cursor != null;
        assert cursorId != null;
        assert output != null;

        if (cursor.isFinished()) {
            throw new CursorIsFinishedException(cursorId);
        }

        final RowCollector rc = cursor.getRowCollector();
        try {
            if (!rc.hasMore()) {
                cursor.setFinished();
                return false;
            }
            cursor.setScanning();
            if (output.getOutputToMessage()) {
                collectRowsIntoBuffer(cursor, output, limit);
            } else {
                collectRows(cursor, output, limit);
            }
            assert !cursor.isFinished() == rc.hasMore();
            return !cursor.isFinished();
        } catch (BufferFullException e) {
            throw e; // Don't want this to be handled as an Exception
        } catch (Exception e) {
            cursor.setFinished();
            throw new GenericInvalidOperationException(e);
        }
    }

    // Returns true if cursor ran out of rows before reaching the limit, false otherwise.
    private static void collectRowsIntoBuffer(Cursor cursor, LegacyRowOutput output, int limit)
        throws Exception
    {
        RowCollector rc = cursor.getRowCollector();
        rc.outputToMessage(true);
        ByteBuffer buffer = output.getOutputBuffer();
        boolean limitReached = (limit == 0);
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
                output.wroteRow();
                if (limit > 0) {
                    limitReached = (--limit) == 0;
                }
            }
        }
    }

    // Returns true if cursor ran out of rows before reaching the limit, false otherwise.
    private static void collectRows(Cursor cursor, LegacyRowOutput output, int limit)
        throws Exception
    {
        RowCollector rc = cursor.getRowCollector();
        rc.outputToMessage(false);
        boolean limitReached = (limit == 0);
        RowData rowData;
        while (!limitReached && !cursor.isFinished()) {
            rowData = rc.collectNextRow();
            if (rowData == null) {
                cursor.setFinished();
            } else {
                output.addRow(rowData);
                if (limit > 0) {
                    limitReached = (--limit) == 0;
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
        final ScanData scanData = session.remove(MODULE_NAME, cursorId);
        if (scanData == null) {
            throw new CursorIsUnknownException(cursorId);
        }
        Map<CursorId,Cursor> cursors = session.get(MODULE_NAME, OPEN_CURSORS_MAP);
        // cursors should not be null, since the cursor isn't null and creating
        // it guarantees a Set<Cursor>
        Cursor removedCursor = cursors.remove(cursorId);
        removedCursor.getRowCollector().close();
    }

    @Override
    public Set<CursorId> getCursors(Session session) {
        Map<CursorId,Cursor> cursors = session.get(MODULE_NAME, OPEN_CURSORS_MAP);
        if (cursors == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(cursors.keySet());
    }

    @Override
    public RowData convertNewRow(NewRow row) throws NoSuchTableException {
        logger.trace("converting to RowData: {}", row);
        return row.toRowData();
    }

    @Override
    public NewRow convertRowData(RowData rowData) throws NoSuchTableException {
        logger.trace("converting to NewRow: {}", rowData);
        RowDef rowDef = ddlFunctions.getRowDef(rowData.getRowDefId());
        return NiceRow.fromRowData(rowData, rowDef);
    }

    @Override
    public List<NewRow> convertRowDatas(List<RowData> rowDatas)
            throws NoSuchTableException
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
                rowDef = ddlFunctions.getRowDef(currRowDefId);
            }
            converted.add(NiceRow.fromRowData(rowData, rowDef));
        }
        return converted;
    }

    @Override
    public Long writeRow(Session session, NewRow row)
            throws NoSuchTableException, UnsupportedModificationException,
            TableDefinitionMismatchException, DuplicateKeyException,
            GenericInvalidOperationException
    {
        logger.trace("writing a row");
        final RowData rowData = niceRowToRowData(row);
        try {
            store().writeRow(session, rowData);
            increment(StatisticsService.CountingStat.INSERTS);
            return null;
        } catch (Exception e) {
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(DuplicateKeyException.class, ioe);
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void deleteRow(Session session, NewRow row)
            throws NoSuchTableException, UnsupportedModificationException,
            ForeignKeyConstraintDMLException, NoSuchRowException,
            TableDefinitionMismatchException, GenericInvalidOperationException
    {
        logger.trace("deleting a row");
        final RowData rowData = niceRowToRowData(row);
        try {
            store().deleteRow(session, rowData);
            increment(StatisticsService.CountingStat.DELETES);
        } catch (Exception e) {
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(NoSuchRowException.class, ioe);
            throw new GenericInvalidOperationException(e);
        }
    }

    private RowData niceRowToRowData(NewRow row) throws NoSuchTableException,
            TableDefinitionMismatchException
    {;
        try {
            return row.toRowData();
        } catch (EncodingException e) {
            throw new TableDefinitionMismatchException(e);
        }
    }

    @Override
    public void updateRow(Session session, NewRow oldRow, NewRow newRow, ColumnSelector columnSelector)
            throws NoSuchTableException, DuplicateKeyException,
            TableDefinitionMismatchException, UnsupportedModificationException,
            ForeignKeyConstraintDMLException, NoSuchRowException,
            GenericInvalidOperationException
    {
        logger.trace("updating a row");
        final RowData oldData = niceRowToRowData(oldRow);
        final RowData newData = niceRowToRowData(newRow);

        LegacyUtils.matchRowDatas(oldData, newData);
        try {
            store().updateRow(session, oldData, newData, columnSelector);
            increment(StatisticsService.CountingStat.UPDATES);
        } catch (Exception e) {
            final InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(NoSuchRowException.class, ioe);
            throwIfInstanceOf(DuplicateKeyException.class, ioe);
            throw new GenericInvalidOperationException(ioe);
        }
    }

    @Override
    public void truncateTable(final Session session, final int tableId)
            throws NoSuchTableException, UnsupportedModificationException,
            ForeignKeyConstraintDMLException, GenericInvalidOperationException
    {
        logger.trace("truncating tableId={}", tableId);
        final Table table = ddlFunctions.getTable(session, tableId);
        final UserTable utable = table.isUserTable() ? (UserTable)table : null;

        final Collection<Index> indexes;

        // Reject a truncate that would create orphan rows
        if(utable != null) {
            for(Join join : ((UserTable)table).getChildJoins()) {
                final Table childTable = join.getChild();
                final TableStatistics stats = getTableStatistics(session, childTable.getTableId(), false);
                if(stats.getRowCount() > 0) {
                    String errorMsg = String.format("Child table %s has rows", childTable.getName().getTableName());
                    throw new ForeignKeyConstraintDMLException(ErrorCode.FK_CONSTRAINT_VIOLATION, errorMsg);
                }
            }

            indexes = utable.getIndexesIncludingInternal();
        }
        else {
            indexes = table.getIndexes();
        }

        // Store.deleteRow() requires all index columns to be in the passed RowData to properly clean everything up
        Set<Integer> keyColumns = new HashSet<Integer>();
        for(Index index : indexes) {
            for(IndexColumn col : index.getColumns()) {
                int pos = col.getColumn().getPosition();
                keyColumns.add(pos);
            }
        }

        // Store.truncate() gets rid of the entire group, so roll our own by doing a table scan
        RowDataLegacyOutputRouter output = new RowDataLegacyOutputRouter();
        output.addHandler(new RowDataLegacyOutputRouter.Handler() {
            private LegacyRowWrapper rowWrapper = new LegacyRowWrapper();

            @Override
            public void handleRow(RowData rowData) throws RowOutputException {
                try {
                    rowWrapper.setRowData(rowData);
                    deleteRow(session, rowWrapper);
                } catch (InvalidOperationException e) {
                    throw new RowOutputException(e);
                }
            }
        });


        final CursorId cursorId;
        try {
            ScanRequest all = new ScanAllRequest(tableId, keyColumns);
            cursorId = openCursor(session, all);
        } catch (InvalidOperationException e) {
            throw new RuntimeException("Internal error", e);
        }

        InvalidOperationException thrown = null;
        try {
            while (scanSome(session, cursorId, output, -1)) {
            }
        } catch (InvalidOperationException e) {
            throw new RuntimeException("Internal error", e);
        } catch (BufferFullException e) {
            throw new RuntimeException("Internal error, buffer full: " + e);
        } finally {
            try {
                closeCursor(session, cursorId);
            } catch (CursorIsUnknownException e) {
                thrown = e;
            }
        }
        if (thrown != null) {
            throw new RuntimeException("Internal error", thrown);
        }
    }

    private void increment(StatisticsService.CountingStat which) {
        serviceManager().getServiceByClass(StatisticsService.class).incrementCount(which);
    }
}
