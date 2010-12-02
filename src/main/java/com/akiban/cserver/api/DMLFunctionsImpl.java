package com.akiban.cserver.api;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.*;
import com.akiban.cserver.api.dml.scan.*;
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
    @Deprecated
    public long countRowsExactly(ScanRange range)
            throws  NoSuchTableException,
            UnsupportedReadException,
            GenericInvalidOperationException
    {
        try {
            final IdResolver idr = idResolver();
            return store().getRowCount(true, range.getStart(idr), range.getEnd(idr), range.getColumnBitMap());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    @Deprecated
    public long countRowsApproximately(ScanRange range)
    throws  NoSuchTableException,
            UnsupportedReadException,
            GenericInvalidOperationException
    {
        try {
            final IdResolver idr = idResolver();
            return store().getRowCount(false, range.getStart(idr), range.getEnd(idr), range.getColumnBitMap());
        } catch (Exception e) {
            throw rethrow(e);
        }
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
            throw rethrow(e);
        }
    }

    @Override
    public CursorId openCursor(ScanRequest request, Session session)
    throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            GenericInvalidOperationException
    {
        final RowCollector rc = getRowCollector(request);
        final CursorId cursorId = newUniqueCursor(rc.getTableId());
        final Cursor cursor = new Cursor(rc);
        Object old = session.put(MODULE_NAME, cursorId, cursor);
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

    protected RowCollector getRowCollector(ScanRequest request)
            throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            GenericInvalidOperationException
    {
        try {
            final IdResolver idr = idResolver();
            return store().newRowCollector(request.getTableId(idr), request.getIndexId(), request.getScanFlags(),
                    request.getStart(idr), request.getEnd(idr), request.getColumnBitMap());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected CursorId newUniqueCursor(int tableId) {
        return new CursorId(cursorsCount.incrementAndGet(), tableId);
    }

    @Override
    public CursorState getCursorState(CursorId cursorId, Session session) {
        final Cursor cursor = session.get(MODULE_NAME, cursorId);
        if (cursor == null) {
            return CursorState.UNKNOWN_CURSOR;
        }
        return cursor.getState();
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

        final Cursor cursor = session.get(MODULE_NAME, cursorId);
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

        public void setConverter(int rowDefId, RowOutput output) throws NoSuchTableException {
            final RowDef rowDef;
            try {
                rowDef = store.getRowDefCache().getRowDef( rowDefId );
            } catch (RowDefNotFoundException e) {
                throw new NoSuchTableException( rowDefId );
            }
            converter.setRowDef(rowDef);
            converter.setOutput(output);
        }
    }
    
    private final BlockingQueue<PooledConverter> convertersPool = new LinkedBlockingDeque<PooledConverter>();

    private PooledConverter getPooledConverter(int tableId, RowOutput output) throws NoSuchTableException {
        PooledConverter converter = convertersPool.poll();
        if (converter == null) {
            // TODO Log something here: we're allocating a new buffer, which shouldn't happen a lot
            converter = new PooledConverter(store());
        }
        try {
            converter.setConverter(tableId, output);
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
        final PooledConverter converter = getPooledConverter(cursorId.getTableId(), output);
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
            throw rethrow(e);
        }
    }

    @Override
    public void closeCursor(CursorId cursorId, Session session) throws CursorIsUnknownException {
        ArgumentValidation.notNull("cursor ID", cursorId);
        final Cursor cursor = session.remove(MODULE_NAME, cursorId);
        if (cursor == null) {
            throw new CursorIsUnknownException(cursorId);
        }
        Set<CursorId> cursors = session.get(MODULE_NAME, OPEN_CURSORS);
        // cursors should not be null, since the cursor isn't null and creating it guarantees a Set<Cursor>
        boolean removeWorked = cursors.remove(cursorId);
        assert removeWorked : String.format("%s %s -> %s", cursorId, cursor, cursors);
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
            throw rethrow(e);
        }
    }

    @Deprecated
    public Long writeRow(RowData rowData) throws InvalidOperationException
    {
        try {
            store().writeRow(rowData);
            return null;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
    
    @Override
    public void deleteRow(NewRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            GenericInvalidOperationException
    {
        final RowData rowData = niceRowToRowData(row);
        try {
            store().deleteRow(rowData);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private RowData niceRowToRowData(NewRow row) throws NoSuchTableException {
        RowDef rowDef = null;
        if (row.needsRowDef()) {
            rowDef = store().getRowDefCache().getRowDef(row.getTableId().getTableId(idResolver()));
        }
        return row.toRowData(rowDef);
    }

    @Override
    public NewRow convertRowData(RowData rowData) {
        final RowDef rowDef = store().getRowDefCache().getRowDef( rowData.getRowDefId() );
        return NiceRow.fromRowData(rowData, rowDef);
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
            throw rethrow(e);
        }
    }

    @Override
    public void truncateTable(TableId tableId)
    throws NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            GenericInvalidOperationException
    {
        try {
            store().truncateTable(tableId.getTableId(idResolver()) );
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
