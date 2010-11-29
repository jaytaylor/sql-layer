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
import com.akiban.util.ArgumentValidation;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("deprecation")
public class DMLFunctionsImpl extends ClientAPIBase implements DMLFunctions {
    private final Session session;

    private static final String MODULE_NAME = DMLFunctionsImpl.class.getCanonicalName();
    private static final AtomicLong cursorsCount = new AtomicLong();

    public DMLFunctionsImpl(Store store, Session session) {
        super(store);
        this.session = session;
    }

    @Override
    public long getAutoIncrementValue(TableId tableId) throws NoSuchTableException, GenericInvalidOperationException {
        final int tableIdInt = tableId.getTableId(idResolver());
        try {
            return store().getAutoIncrementValue(tableIdInt);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public long countRowsExactly(ScanRange range)
    throws  NoSuchTableException,
            UnsupportedReadException,
            GenericInvalidOperationException
    {
        LegacyScanRange legacy = new LegacyScanRangeImpl(store(), idResolver(), range);
        return countRowsExactly(legacy);
    }

    @Override
    @Deprecated
    public long countRowsExactly(LegacyScanRange range)
            throws  NoSuchTableException,
            UnsupportedReadException,
            GenericInvalidOperationException
    {
        try {
            return store().getRowCount(true, range.getStart(), range.getEnd(), range.getColumnBitMap());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public long countRowsApproximately(ScanRange range)
            throws  NoSuchTableException,
            UnsupportedReadException,
            GenericInvalidOperationException
    {
        LegacyScanRange legacy = new LegacyScanRangeImpl(store(), idResolver(), range);
        return countRowsApproximately(legacy);
    }

    @Override
    @Deprecated
    public long countRowsApproximately(LegacyScanRange range)
    throws  NoSuchTableException,
            UnsupportedReadException,
            GenericInvalidOperationException
    {
        try {
            return store().getRowCount(false, range.getStart(), range.getEnd(), range.getColumnBitMap());
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
    public CursorId openCursor(ScanRequest request)
    throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            GenericInvalidOperationException
    {
        return openCursorForCollector( getRowCollector(request) );
    }

    @Override
    @Deprecated
    public CursorId openCursor(LegacyScanRequest request)
    throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            GenericInvalidOperationException
    {
        return openCursorForCollector( getRowCollector(request) );
    }

    private CursorId openCursorForCollector(RowCollector rc) throws GenericInvalidOperationException {
        final CursorId cursor = newUniqueCursor(rc.getTableId());
        Object old = session.put(MODULE_NAME, cursor, new Cursor(rc));
        assert old == null : old;
        return cursor;
    }

    protected CursorId newUniqueCursor(int tableId) {
        return new CursorId(cursorsCount.incrementAndGet(), tableId);
    }

    protected RowCollector getRowCollector(ScanRequest request) throws NoSuchTableException, GenericInvalidOperationException {
        ArgumentValidation.notNull("request", request);
        LegacyScanRequest legacy = new LegacyScanRequestImpl(store(), idResolver(), request);
        return getRowCollector(legacy);
    }

    private RowCollector getRowCollector(LegacyScanRequest legacy) throws GenericInvalidOperationException {
        try {
            return store().newRowCollector(legacy.getTableId(), legacy.getIndexId(), legacy.getScanFlags(),
                    legacy.getStart(), legacy.getEnd(), legacy.getColumnBitMap());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean scanSome(CursorId cursorId, LegacyRowOutput output, int limit)
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

    /**
     * Do the actual scan. Refactored out of scanSome for ease of unit testing.
     * @param cursor the cursor itself; used to check status and get a row collector
     * @param cursorId the cursor id; used only to report errors
     * @param output the output; see {@link #scanSome(CursorId, com.akiban.cserver.api.dml.scan.LegacyRowOutput , int)}
     * @param limit the limit, or negative value if none;  ee {@link #scanSome(CursorId, com.akiban.cserver.api.dml.scan.LegacyRowOutput , int)}
     * @return whether more rows remain to be scanned; see {@link #scanSome(CursorId, com.akiban.cserver.api.dml.scan.LegacyRowOutput , int)}
     * @throws CursorIsFinishedException see {@link #scanSome(CursorId, com.akiban.cserver.api.dml.scan.LegacyRowOutput , int)}
     * @throws RowOutputException see {@link #scanSome(CursorId, com.akiban.cserver.api.dml.scan.LegacyRowOutput , int)}
     * @throws GenericInvalidOperationException see {@link #scanSome(CursorId, com.akiban.cserver.api.dml.scan.LegacyRowOutput , int)}
     * @see #scanSome(CursorId, com.akiban.cserver.api.dml.scan.LegacyRowOutput , int)
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
    public void closeCursor(CursorId cursorId) throws CursorIsUnknownException {
        ArgumentValidation.notNull("cursor ID", cursorId);
        final Cursor cursor = session.remove(MODULE_NAME, cursorId);
        if (cursor == null) {
            throw new CursorIsUnknownException(cursorId);
        }
    }

    @Override
    public Set<CursorId> getCursors() {
        throw new UnsupportedOperationException("need to revisit whether we need this; "
                + "it may require modifications to Session to make this easy");
    }

    @Override
    public Long writeRow(TableId tableId, NiceRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            TableDefinitionMismatchException,
            DuplicateKeyException,
            GenericInvalidOperationException
    {
        final RowData rowData = niceRowToRowData(tableId, row);
        try {
            store().writeRow(rowData);
            return null;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
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
    public void deleteRow(TableId tableId, NiceRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            GenericInvalidOperationException
    {
        final RowData rowData = niceRowToRowData(tableId, row);
        deleteRow(rowData);
    }

    @Override
    @Deprecated
    public void deleteRow(RowData rowData) throws GenericInvalidOperationException
    {
        try {
            store().deleteRow(rowData);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private RowData niceRowToRowData(TableId tableId, NiceRow row) throws NoSuchTableException {
        final RowDef rowDef = store().getRowDefCache().getRowDef(tableId.getTableId(idResolver()));
        return row.toRowData(rowDef);
    }

    @Override
    public NiceRow convertRowData(RowData rowData) {
        final RowDef rowDef = store().getRowDefCache().getRowDef( rowData.getRowDefId() );
        return NiceRow.fromRowData(rowData, rowDef);
    }

    @Override
    public void updateRow(TableId tableId, NiceRow oldRow, NiceRow newRow)
    throws  NoSuchTableException,
            DuplicateKeyException,
            TableDefinitionMismatchException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            GenericInvalidOperationException
    {
        final RowData oldData = niceRowToRowData(tableId, oldRow);
        final RowData newData = niceRowToRowData(tableId, newRow);

        updateRow(oldData, newData);
    }

    @Override
    @Deprecated
    public void updateRow(RowData oldData, RowData newData)
    throws  GenericInvalidOperationException,
            TableDefinitionMismatchException
    {
        matchRowDatas(oldData, newData);
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
            rethrow(e);
        }
    }

    public static class LegacyScanRangeImpl implements DMLFunctions.LegacyScanRange {
        final RowData start;
        final RowData end;
        final byte[] columnBitMap;
        final int tableId;

        public LegacyScanRangeImpl(Integer tableId, RowData start, RowData end, byte[] columnBitMap)
                throws TableDefinitionMismatchException
        {
            Integer rowsTableId = matchRowDatas(start, end);
            if ( (rowsTableId != null) && (tableId != null) && (!rowsTableId.equals(tableId)) ) {
                throw new TableDefinitionMismatchException(String.format(
                        "ID<%d> from RowData didn't match given ID <%d>", rowsTableId, tableId));
            }
            this.tableId = tableId == null ? -1 : tableId;
            this.start = start;
            this.end = end;
            this.columnBitMap = columnBitMap;
        }

        public LegacyScanRangeImpl(Store store, IdResolver idResolver, ScanRange fromRange) throws NoSuchTableException {
            tableId = fromRange.getTableIdInt(idResolver);
            RowDef rowDef = store.getRowDefCache().getRowDef(tableId);
            start = fromRange.getPredicate().getStartRow().toRowData(rowDef);
            end = fromRange.getPredicate().getEndRow().toRowData(rowDef);
            columnBitMap = fromRange.getColumnSetBytes(idResolver);
        }

        @Override
        public RowData getStart() {
            return start;
        }

        @Override
        public RowData getEnd() {
            return end;
        }

        @Override
        public byte[] getColumnBitMap() {
            return columnBitMap;
        }

        @Override
        public int getTableId() {
            return tableId;
        }
    }

    public static class LegacyScanRequestImpl extends LegacyScanRangeImpl implements DMLFunctions.LegacyScanRequest {
        private final int indexId;
        private final int scanFlags;

        @Override
        public int getIndexId() {
            return indexId;
        }

        @Override
        public int getScanFlags() {
            return scanFlags;
        }

        public LegacyScanRequestImpl(int tableId, RowData start, RowData end, byte[] columnBitMap, int indexId, int scanFlags)
        throws TableDefinitionMismatchException
        {
            super(tableId, start, end, columnBitMap);
            this.indexId = indexId;
            this.scanFlags = scanFlags;
        }

        public LegacyScanRequestImpl(Store store, IdResolver idResolver, ScanRequest fromRequest)
        throws NoSuchTableException {
            super(store, idResolver, fromRequest);
            indexId = fromRequest.getIndexIdInt(idResolver);
            scanFlags = ScanFlag.toRowDataFormat( fromRequest.getPredicate().getScanFlags() );
        }
    }

    private static Integer matchRowDatas(RowData one, RowData two) throws TableDefinitionMismatchException {
        if (one == null) {
            return (two == null) ? null : two.getRowDefId();
        }
        if (two == null) {
            return one.getRowDefId();
        }
        if (one.getRowDefId() == two.getRowDefId()) {
            return one.getRowDefId();
        }
        throw new TableDefinitionMismatchException("Mismatched table ids: %d != %d",
                one.getRowDefId(), two.getRowDefId());
    }
}
