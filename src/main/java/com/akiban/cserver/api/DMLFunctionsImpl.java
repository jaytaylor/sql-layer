package com.akiban.cserver.api;

import com.akiban.ais.model.Table;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.*;
import com.akiban.cserver.api.dml.scan.*;
import com.akiban.cserver.encoding.EncodingException;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.store.RowCollector;
import com.akiban.cserver.store.Store;
import com.akiban.cserver.util.RowDefNotFoundException;
import com.akiban.util.ArgumentValidation;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("deprecation")
public class DMLFunctionsImpl extends ClientAPIBase implements DMLFunctions {

    private static final String MODULE_NAME = DMLFunctionsImpl.class.getCanonicalName();
    private static final AtomicLong cursorsCount = new AtomicLong();
    private static final Object OPEN_CURSORS = new Object();

    public DMLFunctionsImpl(Store store) {
        super(store);
    }

    @Override
    public TableStatistics getTableStatistics(TableId tableId, boolean updateFirst)
    throws  NoSuchTableException,
            GenericInvalidOperationException
    {
        final int tableIdInt = tableId.getTableId(idResolver());
        try {
            if (updateFirst) {
                store().analyzeTable(tableIdInt);
            }
            return store().getTableStatistics(tableIdInt);
        } catch (Exception e) {
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
            return String.format("ScanData[cursor=%s, columns=%s]", cursor, getScanColumns() );
        }
    }

    @Override
    public CursorId openCursor(ScanRequest request, Session session)
    throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            GenericInvalidOperationException
    {
        if (request.scanAllColumns()) {
            request = scanAllColumns(request);
        }
        final RowCollector rc = getRowCollector(request);
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

    private ScanRequest scanAllColumns(final ScanRequest request) throws NoSuchTableException {
        Table table = store().getAis().getTable( request.getTableId().getTableName(idResolver()) );
        final int colsCount = table.getColumns().size();
        Set<ColumnId> allColumns = new HashSet<ColumnId>(colsCount);
        for (int i=0; i < colsCount; ++i) {
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
            public RowData getStart(IdResolver idResolver) throws NoSuchTableException {
                return request.getStart(idResolver);
            }

            @Override
            public RowData getEnd(IdResolver idResolver) throws NoSuchTableException {
               return request.getEnd(idResolver);
            }

            @Override
            public byte[] getColumnBitMap() {
                return allColumnsBytes;
            }

            @Override
            public int getTableIdInt(IdResolver idResolver) throws NoSuchTableException {
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

    protected RowCollector getRowCollector(ScanRequest request)
            throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            GenericInvalidOperationException
    {
        try {
            final IdResolver idr = idResolver();
            return store().newRowCollector(request.getTableIdInt(idr), request.getIndexId(), request.getScanFlags(),
                    request.getStart(idr), request.getEnd(idr), request.getColumnBitMap());
        }
        catch (RowDefNotFoundException e) {
            throw new NoSuchTableException(request.getTableId(), e);
        }
        catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    protected CursorId newUniqueCursor(int tableId) {
        return new CursorId(cursorsCount.incrementAndGet(), tableId);
    }

    @Override
    public CursorState getCursorState(CursorId cursorId, Session session) {
        final ScanData extraData = session.get(MODULE_NAME, cursorId);
        if (extraData == null || extraData.getCursor() == null) {
            return CursorState.UNKNOWN_CURSOR;
        }
        return extraData.getCursor().getState();
    }

    @Override
    public boolean scanSome(CursorId cursorId, Session session, LegacyRowOutput output, int limit)
    throws  CursorIsFinishedException,
            CursorIsUnknownException,
            RowOutputException,
            GenericInvalidOperationException

    {
        ArgumentValidation.notNull("cursor", cursorId);
        ArgumentValidation.notNull("output", output);

        final Cursor cursor = session.<ScanData>get(MODULE_NAME, cursorId).getCursor();
        if (cursor == null) {
            throw new CursorIsUnknownException(cursorId);
        }
        return doScan(cursor, cursorId, output, limit);
    }

    private static class PooledConverter {
        private final LegacyOutputConverter converter;
        private final LegacyOutputRouter router;
        private final Store store;

        public PooledConverter(Store store) {
            this.store = store;
            router = new LegacyOutputRouter(1024 * 1024, true);
            converter = new LegacyOutputConverter();
            router.addHandler(converter);
        }

        public LegacyRowOutput getLegacyOutput() {
            return router;
        }

        public void setConverter(int rowDefId, RowOutput output, Set<ColumnId> columns) throws NoSuchTableException {
            final RowDef rowDef;
            try {
                rowDef = store.getRowDefCache().getRowDef( rowDefId );
            } catch (RowDefNotFoundException e) {
                throw new NoSuchTableException( rowDefId );
            }
            converter.setRowDef(rowDef);
            converter.setOutput(output);
            converter.setColumnsToScan(columns);
        }
    }
    
    private final BlockingQueue<PooledConverter> convertersPool = new LinkedBlockingDeque<PooledConverter>();

    private PooledConverter getPooledConverter(int tableId, RowOutput output, Set<ColumnId> columns)
    throws NoSuchTableException
    {
        PooledConverter converter = convertersPool.poll();
        if (converter == null) {
            // TODO Log something here: we're allocating a new buffer, which shouldn't happen a lot
            converter = new PooledConverter(store());
        }
        try {
            converter.setConverter(tableId, output, columns);
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
            // TODO Log something here -- this is a warning of inefficient tuning
        }
    }

    @Override
    public boolean scanSome(CursorId cursorId, Session session, RowOutput output, int limit)
    throws  CursorIsFinishedException,
            CursorIsUnknownException,
            RowOutputException,
            NoSuchTableException,
            GenericInvalidOperationException
    {
        final ScanData scanData = session.get(MODULE_NAME, cursorId);
        assert scanData != null;
        Set<ColumnId> scanColumns = scanData.scanAll() ? null : scanData.getScanColumns();
        final PooledConverter converter = getPooledConverter(cursorId.getTableId(), output, scanColumns);
        try {
            return scanSome(cursorId, session, converter.getLegacyOutput(), limit);
        }
        finally {
            releasePooledConverter(converter);
        }
    }

    /**
     * Do the actual scan. Refactored out of scanSome for ease of unit testing.
     * @param cursor the cursor itself; used to check status and get a row collector
     * @param cursorId the cursor id; used only to report errors
     * @param output the output; see {@link #scanSome(CursorId, Session, LegacyRowOutput , int)}
     * @param limit the limit, or negative value if none;  ee {@link #scanSome(CursorId, Session, LegacyRowOutput , int)}
     * @return whether more rows remain to be scanned; see {@link #scanSome(CursorId, Session, LegacyRowOutput , int)}
     * @throws CursorIsFinishedException see {@link #scanSome(CursorId, Session, LegacyRowOutput , int)}
     * @throws RowOutputException see {@link #scanSome(CursorId, Session, LegacyRowOutput , int)}
     * @throws GenericInvalidOperationException see {@link #scanSome(CursorId, Session, LegacyRowOutput , int)}
     * @see #scanSome(CursorId, Session, LegacyRowOutput , int)
     */
    protected static boolean doScan(Cursor cursor, CursorId cursorId, LegacyRowOutput output, int limit)
    throws  CursorIsFinishedException,
            RowOutputException,
            GenericInvalidOperationException
    {
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
            while ( mayHaveMore && (!limitReached)) {
                mayHaveMore = rc.collectNextRow(buffer);

                final int bufferPos = buffer.position();
                assert bufferPos >= bufferLastPos : String.format("false: %d >= %d", bufferPos, bufferLastPos);
                if (bufferPos == bufferLastPos) {
                    // The previous iteration of rc.collectNextRow() said there'd be more, but there wasn't
                    break;
                }

                output.wroteRow();
                bufferLastPos = buffer.position(); // wroteRow() may have changed this, so we get it again
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
    public void closeCursor(CursorId cursorId, Session session) throws CursorIsUnknownException {
        ArgumentValidation.notNull("cursor ID", cursorId);
        final ScanData scanData = session.remove(MODULE_NAME, cursorId);
        if (scanData == null) {
            throw new CursorIsUnknownException(cursorId);
        }
        Set<CursorId> cursors = session.get(MODULE_NAME, OPEN_CURSORS);
        // cursors should not be null, since the cursor isn't null and creating it guarantees a Set<Cursor>
        boolean removeWorked = cursors.remove(cursorId);
        assert removeWorked : String.format("%s %s -> %s", cursorId, scanData, cursors);
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
    public NewRow convertRowData(RowData rowData) throws NoSuchTableException {
        RowDef rowDef = idResolver().getRowDef( TableId.of(rowData.getRowDefId()) );
        return NiceRow.fromRowData(rowData, rowDef);
    }

    @Override
    public List<NewRow> convertRowDatas(List<RowData> rowDatas) throws NoSuchTableException {
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
    public Long writeRow(NewRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            TableDefinitionMismatchException,
            DuplicateKeyException,
            GenericInvalidOperationException
    {
        final RowData rowData = niceRowToRowData(row);
        try {
            store().writeRow(rowData);
            return null;
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Deprecated
    public Long writeRow(RowData rowData) throws InvalidOperationException
    {
        try {
            store().writeRow(rowData);
            return null;
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }
    
    @Override
    public void deleteRow(NewRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            TableDefinitionMismatchException,
            GenericInvalidOperationException
    {
        final RowData rowData = niceRowToRowData(row);
        try {
            store().deleteRow(rowData);
        } catch (Exception e) {
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(NoSuchRowException.class, ioe);
            throw new GenericInvalidOperationException(e) ;
        }
    }

    private RowData niceRowToRowData(NewRow row) throws NoSuchTableException, TableDefinitionMismatchException {
        RowDef rowDef = null;
        if (row.needsRowDef()) {
            rowDef = store().getRowDefCache().getRowDef(row.getTableId().getTableId(idResolver()));
        }
        try {
            return row.toRowData(rowDef);
        } catch (EncodingException e) {
            throw new TableDefinitionMismatchException(e);
        }
    }

    @Override
    public void updateRow(NewRow oldRow, NewRow newRow)
    throws  NoSuchTableException,
            DuplicateKeyException,
            TableDefinitionMismatchException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            GenericInvalidOperationException
    {
        final RowData oldData = niceRowToRowData(oldRow);
        final RowData newData = niceRowToRowData(newRow);

        LegacyUtils.matchRowDatas(oldData, newData);
        try {
            store().updateRow(oldData, newData);
        } catch (Exception e) {
            final InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(NoSuchRowException.class, ioe);
            throw new GenericInvalidOperationException(ioe);
        }
    }

    @Override
    public void truncateTable(TableId tableId)
    throws NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            GenericInvalidOperationException
    {
        throw new UnsupportedOperationException("truncate not supported");
    }
}
