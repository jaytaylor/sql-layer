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

package com.akiban.cserver.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.*;
import com.akiban.cserver.api.dml.scan.ColumnSet;
import com.akiban.cserver.api.dml.scan.Cursor;
import com.akiban.cserver.api.dml.scan.CursorId;
import com.akiban.cserver.api.dml.scan.CursorIsFinishedException;
import com.akiban.cserver.api.dml.scan.CursorIsUnknownException;
import com.akiban.cserver.api.dml.scan.CursorState;
import com.akiban.cserver.api.dml.scan.LegacyOutputConverter;
import com.akiban.cserver.api.dml.scan.LegacyOutputRouter;
import com.akiban.cserver.api.dml.scan.LegacyRowOutput;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.NiceRow;
import com.akiban.cserver.api.dml.scan.RowOutput;
import com.akiban.cserver.api.dml.scan.RowOutputException;
import com.akiban.cserver.api.dml.scan.ScanAllRequest;
import com.akiban.cserver.api.dml.scan.ScanRequest;
import com.akiban.cserver.encoding.EncodingException;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.store.RowCollector;
import com.akiban.cserver.util.RowDefNotFoundException;
import com.akiban.util.ArgumentValidation;
import org.apache.log4j.Logger;

public class DMLFunctionsImpl extends ClientAPIBase implements DMLFunctions {

    private static final String MODULE_NAME = DMLFunctionsImpl.class
            .getCanonicalName();
    private static final AtomicLong cursorsCount = new AtomicLong();
    private static final Object OPEN_CURSORS = new Object();

    private final static Logger logger = Logger.getLogger(DMLFunctionsImpl.class);

    @Override
    public TableStatistics getTableStatistics(Session session, TableId tableId,
            boolean updateFirst) throws NoSuchTableException,
            GenericInvalidOperationException {
        final int tableIdInt = tableId.getTableId(idResolver());
        try {
            if (updateFirst) {
                store().analyzeTable(session, tableIdInt);
            }
            return store().getTableStatistics(session, tableIdInt);
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
        private Set<ColumnId> scanColumnsUnpacked;

        ScanData(ScanRequest request, Cursor cursor) {
            scanColumns = request.getColumnBitMap();
            scanAll = request.scanAllColumns();
            this.cursor = cursor;
        }

        public Set<ColumnId> getScanColumns() {
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
            NoSuchIndexException, GenericInvalidOperationException {
        if (request.scanAllColumns()) {
            request = scanAllColumns(session, request);
        }
        final RowCollector rc = getRowCollector(session, request);
        final CursorId cursorId = newUniqueCursor(rc.getTableId());
        final Cursor cursor = new Cursor(rc);
        Object old = session.put(MODULE_NAME, cursorId, new ScanData(request, cursor));
        assert old == null : old;

        Set<CursorId> cursors = session.get(MODULE_NAME, OPEN_CURSORS);
        if (cursors == null) {
            cursors = new HashSet<CursorId>();
            session.put(MODULE_NAME, OPEN_CURSORS, cursors);
        }
        boolean addWorked = cursors.add(cursorId);
        assert addWorked : String.format("%s -> %s", cursor, cursors);
        return cursorId;
    }

    private ScanRequest scanAllColumns(final Session session,
            final ScanRequest request) throws NoSuchTableException {
        Table table = schemaManager().getAis(session).getTable(
                request.getTableId().getTableName(idResolver()));
        final int colsCount = table.getColumns().size();
        Set<ColumnId> allColumns = new HashSet<ColumnId>(colsCount);
        for (int i = 0; i < colsCount; ++i) {
            allColumns.add(ColumnId.of(i));
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
            public RowData getStart(IdResolver idResolver)
                    throws NoSuchTableException {
                return request.getStart(idResolver);
            }

            @Override
            public RowData getEnd(IdResolver idResolver)
                    throws NoSuchTableException {
                return request.getEnd(idResolver);
            }

            @Override
            public byte[] getColumnBitMap() {
                return allColumnsBytes;
            }

            @Override
            public int getTableIdInt(IdResolver idResolver)
                    throws NoSuchTableException {
                return request.getTableIdInt(idResolver);
            }

            @Override
            public TableId getTableId() {
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
            final IdResolver idr = idResolver();
            return store().newRowCollector(session, request.getTableIdInt(idr),
                    request.getIndexId(), request.getScanFlags(),
                    request.getStart(idr), request.getEnd(idr),
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
                   GenericInvalidOperationException

    {
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
        private final LegacyOutputRouter router;

        public PooledConverter(DMLFunctions dmlFunctions) {
            router = new LegacyOutputRouter(1024 * 1024, true);
            converter = new LegacyOutputConverter(dmlFunctions);
            router.addHandler(converter);
        }

        public LegacyRowOutput getLegacyOutput() {
            return router;
        }

        public void setConverter(RowOutput output, Set<ColumnId> columns)
                throws NoSuchTableException {
            converter.setOutput(output);
            converter.setColumnsToScan(columns);
        }
    }

    private final BlockingQueue<PooledConverter> convertersPool = new LinkedBlockingDeque<PooledConverter>();

    private PooledConverter getPooledConverter(RowOutput output, Set<ColumnId> columns)
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
               GenericInvalidOperationException {
        final ScanData scanData = session.get(MODULE_NAME, cursorId);
        assert scanData != null;
        Set<ColumnId> scanColumns = scanData.scanAll() ? null : scanData.getScanColumns();
        final PooledConverter converter = getPooledConverter(output, scanColumns);
        try {
            return scanSome(session, cursorId, converter.getLegacyOutput(), limit);
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
     * @see #scanSome(Session, CursorId, LegacyRowOutput , int)
     */
    protected static boolean doScan(Cursor cursor,
                                    CursorId cursorId,
                                    LegacyRowOutput output,
                                    int limit)
            throws CursorIsFinishedException,
                   RowOutputException,
                   GenericInvalidOperationException {
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
                if (cursor.isScanning()) {
                    throw new CursorIsFinishedException(cursorId);
                }
                return false;
            }
            if (cursor.isScanning() && !(rc.hasMore())) {
                cursor.setFinished();
                return false;
            }
            cursor.setScanning();
            boolean limitReached = (limit == 0);
            final ByteBuffer buffer = output.getOutputBuffer();
            int bufferLastPos = buffer.position();

            boolean mayHaveMore = true;
            while (mayHaveMore && (!limitReached)) {
                mayHaveMore = rc.collectNextRow(buffer);

                final int bufferPos = buffer.position();
                assert bufferPos >= bufferLastPos : String.format(
                        "false: %d >= %d", bufferPos, bufferLastPos);
                if (bufferPos == bufferLastPos) {
                    // The previous iteration of rc.collectNextRow() said
                    // there'd be more, but there wasn't
                    break;
                }

                output.wroteRow();
                bufferLastPos = buffer.position(); // wroteRow() may have
                                                   // changed this, so we get it
                                                   // again
                if (limit > 0) {
                    limitReached = (--limit) == 0;
                }
            }

            final boolean hasMore = rc.hasMore();
            if (!hasMore) {
                cursor.setFinished();
            }
            return hasMore;
        } catch (Exception e) {
            cursor.setFinished();
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void closeCursor(Session session, CursorId cursorId)
            throws CursorIsUnknownException {
        ArgumentValidation.notNull("cursor ID", cursorId);
        final ScanData scanData = session.remove(MODULE_NAME, cursorId);
        if (scanData == null) {
            throw new CursorIsUnknownException(cursorId);
        }
        Set<CursorId> cursors = session.get(MODULE_NAME, OPEN_CURSORS);
        // cursors should not be null, since the cursor isn't null and creating
        // it guarantees a Set<Cursor>
        boolean removeWorked = cursors.remove(cursorId);
        assert removeWorked : String.format("%s %s -> %s", cursorId, scanData,
                cursors);
    }

    @Override
    public Set<CursorId> getCursors(Session session) {
        Set<CursorId> cursors = session.get(MODULE_NAME, OPEN_CURSORS);
        if (cursors == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(cursors);
    }

    @Override
    public RowData convertNewRow(NewRow row) throws NoSuchTableException {
        return row.toRowData();
    }

    @Override
    public NewRow convertRowData(RowData rowData) throws NoSuchTableException {
        RowDef rowDef = idResolver().getRowDef(
                TableId.of(rowData.getRowDefId()));
        return NiceRow.fromRowData(rowData, rowDef);
    }

    @Override
    public List<NewRow> convertRowDatas(List<RowData> rowDatas)
            throws NoSuchTableException {
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
                rowDef = idResolver().getRowDef(TableId.of(currRowDefId));
            }
            converted.add(NiceRow.fromRowData(rowData, rowDef));
        }
        return converted;
    }

    @Override
    public Long writeRow(Session session, NewRow row)
            throws NoSuchTableException, UnsupportedModificationException,
            TableDefinitionMismatchException, DuplicateKeyException,
            GenericInvalidOperationException {
        final RowData rowData = niceRowToRowData(row);
        try {
            store().writeRow(session, rowData);
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
            TableDefinitionMismatchException, GenericInvalidOperationException {
        final RowData rowData = niceRowToRowData(row);
        try {
            store().deleteRow(session, rowData);
        } catch (Exception e) {
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(NoSuchRowException.class, ioe);
            throw new GenericInvalidOperationException(e);
        }
    }

    private RowData niceRowToRowData(NewRow row) throws NoSuchTableException,
            TableDefinitionMismatchException {
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
            GenericInvalidOperationException {
        final RowData oldData = niceRowToRowData(oldRow);
        final RowData newData = niceRowToRowData(newRow);

        LegacyUtils.matchRowDatas(oldData, newData);
        try {
            store().updateRow(session, oldData, newData, columnSelector);
        } catch (Exception e) {
            final InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(NoSuchRowException.class, ioe);
            throwIfInstanceOf(DuplicateKeyException.class, ioe);
            throw new GenericInvalidOperationException(ioe);
        }
    }

    @Override
    public void truncateTable(final Session session, final TableId tableId)
            throws NoSuchTableException, UnsupportedModificationException,
            ForeignKeyConstraintDMLException, GenericInvalidOperationException {
        // Store.truncate doesn't work well, so we have to actually scan the
        // rows
        TableName tableName = tableId.getTableName(idResolver());
        Index pkIndex = schemaManager().getAis(session).getTable(tableName)
                .getIndex(Index.PRIMARY_KEY_CONSTRAINT);
        assert pkIndex.isPrimaryKey() : pkIndex;
        Set<ColumnId> pkColumns = new HashSet<ColumnId>();
        for (IndexColumn column : pkIndex.getColumns()) {
            int pos = column.getColumn().getPosition();
            pkColumns.add(ColumnId.of(pos));
        }
        ScanRequest all = new ScanAllRequest(tableId, pkColumns);

        RowOutput output = new RowOutput() {
            @Override
            public void output(NewRow row) throws RowOutputException {
                try {
                    deleteRow(session, row);
                } catch (InvalidOperationException e) {
                    throw new RowOutputException(e);
                }
            }
        };

        final CursorId cursorId;
        try {
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
}
