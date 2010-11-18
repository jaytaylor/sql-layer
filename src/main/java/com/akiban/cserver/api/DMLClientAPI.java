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

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class DMLClientAPI extends ClientAPIBase {
    private final Session session;

    private static final String MODULE_NAME = DMLClientAPI.class.getCanonicalName();
    private static final AtomicLong cursorsCount = new AtomicLong();

    DMLClientAPI(String confirmation, Session session) {
        super(confirmation);
        this.session = session;
    }

    public DMLClientAPI(Store store, Session session) {
        super(store);
        this.session = session;
    }


    public long getAutoIncrementValue(TableId tableId) throws NoSuchTableException, InvalidOperationException {
        final int tableIdInt = tableId.getTableId(idResolver());
        try {
            return store().getAutoIncrementValue(tableIdInt);
        } catch (Exception e) {
            throw rethrow(e);
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

    public static class LegacyScanRange {
        final RowData start;
        final RowData end;
        final byte[] columnBitMap;
        final int tableId;

        public LegacyScanRange(Integer tableId, RowData start, RowData end, byte[] columnBitMap)
        throws TableDefinitionMismatchException
        {
            Integer rowsTableId = matchRowDatas(start, end);
            if ( (rowsTableId != null) && (tableId != null) && (!rowsTableId.equals(tableId)) ) {
                throw new TableDefinitionMismatchException(String.format(
                        "ID<%d> from RowData didn't match given ID <%d>", rowsTableId, tableId));
            }
            this.tableId = tableId;
            this.start = start;
            this.end = end;
            this.columnBitMap = columnBitMap;
        }

        private LegacyScanRange(Store store, IdResolver idResolver, ScanRange fromRange) throws NoSuchTableException {
            tableId = fromRange.getTableIdInt(idResolver);
            RowDef rowDef = store.getRowDefCache().getRowDef(tableId);
            start = fromRange.getPredicate().getStartRow().toRowData(rowDef, idResolver);
            end = fromRange.getPredicate().getEndRow().toRowData(rowDef, idResolver);
            columnBitMap = fromRange.getColumnSetBytes(idResolver);
        }
    }

    public static class LegacyScanRequest extends LegacyScanRange {
        final private int indexId;
        final int scanFlags;

        public LegacyScanRequest(int tableId, RowData start, RowData end, byte[] columnBitMap, int indexId, int scanFlags)
        throws TableDefinitionMismatchException
        {
            super(tableId, start, end, columnBitMap);
            this.indexId = indexId;
            this.scanFlags = scanFlags;
        }

        private LegacyScanRequest(Store store, IdResolver idResolver, ScanRequest fromRequest) throws NoSuchTableException {
            super(store, idResolver, fromRequest);
            indexId = fromRequest.getIndexIdInt(idResolver);
            scanFlags = ScanFlag.toRowDataFormat( fromRequest.getPredicate().getScanFlags() );
        }
    }

    /**
     * Returns the exact number of rows in this table. This may take a while, as it could require a full
     * table scan. Group tables have an undefined row count, so this method will fail if the requested
     * table is a group table.
     * @param range the table, columns and range to count
     * @return the number of rows in the specified table
     * @throws NullPointerException if tableId is null
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table is unknown
     * @throws com.akiban.cserver.api.dml.UnsupportedReadException if the specified table is a group table
     */
    public long countRowsExactly(ScanRange range)
    throws  NoSuchTableException,
            UnsupportedReadException,
            InvalidOperationException
    {
        LegacyScanRange legacy = new LegacyScanRange(store(), idResolver(), range);
        return countRowsExactly(legacy);
    }

    /**
     * Returns the exact number of rows in this table. This may take a while, as it could require a full
     * table scan. Group tables have an undefined row count, so this method will fail if the requested
     * table is a group table.
     * @param range the table, columns and range to count
     * @return the number of rows in the specified table
     * @throws NullPointerException if tableId is null
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table is unknown
     * @throws com.akiban.cserver.api.dml.UnsupportedReadException if the specified table is a group table
     * @deprecated use {@link #countRowsExactly(ScanRange)}
     */
    @Deprecated
    public long countRowsExactly(LegacyScanRange range)
            throws  NoSuchTableException,
            UnsupportedReadException,
            InvalidOperationException
    {
        try {
            return store().getRowCount(true, range.start, range.end, range.columnBitMap);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * Returns the approximate number of rows in this table. This estimate may be <em>very</em> approximate. All
     * that is required is that the returned number be:
     * <ul>
     *  <li>0 iff the table has no rows</li>
     *  <li>1 iff the table has exactly one row</li>
     *  <li>&gt;= 2 iff the table has two or more rows</li>
     * </ul>
     *
     * Group tables have an undefined row count, so this method will fail if the requested table is a group table.
     * @param range the table, columns and range to count
     * @return the number of rows in the specified table
     * @throws NullPointerException if tableId is null
     * @throws NoSuchTableException if the specified table is unknown
     * @throws UnsupportedReadException if the specified table is a group table
     */
    public long countRowsApproximately(ScanRange range)
            throws  NoSuchTableException,
            UnsupportedReadException,
            InvalidOperationException
    {
        LegacyScanRange legacy = new LegacyScanRange(store(), idResolver(), range);
        return countRowsApproximately(legacy);
    }

    /**
     * Returns the approximate number of rows in this table. This estimate may be <em>very</em> approximate. All
     * that is required is that the returned number be:
     * <ul>
     *  <li>0 iff the table has no rows</li>
     *  <li>1 iff the table has exactly one row</li>
     *  <li>&gt;= 2 iff the table has two or more rows</li>
     * </ul>
     *
     * Group tables have an undefined row count, so this method will fail if the requested table is a group table.
     * @param range the table, columns and range to count
     * @return the number of rows in the specified table
     * @throws NullPointerException if tableId is null
     * @throws NoSuchTableException if the specified table is unknown
     * @throws UnsupportedReadException if the specified table is a group table
     * @deprecated use {@link #countRowsApproximately(ScanRange)}
     */
    @Deprecated
    public long countRowsApproximately(LegacyScanRange range)
            throws  NoSuchTableException,
            UnsupportedReadException,
            InvalidOperationException
    {
        try {
            return store().getRowCount(false, range.start, range.end, range.columnBitMap);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * Gets the table statistics for the specified table, optionally updating the statistics first. If you request
     * this update, the method may take significantly longer.
     * @param tableId the table for which to get (and possibly update) statistics
     * @param updateFirst whether to update the statistics before returning them
     * @return the table's statistics
     * @throws NullPointerException if tableId is null
     * @throws NoSuchTableException if the specified table doesn't exist
     */
    public TableStatistics getTableStatistics(TableId tableId, boolean updateFirst)
            throws  NoSuchTableException,
            InvalidOperationException
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

    /**
     * Opens a new cursor for scanning a table. This cursor will be stored in the current session, and a handle
     * to it will be returned for use in subsequent cursor-related methods. When you're finished with the cursor,
     * make sure to close it.
     * @param request the request specifications
     * @return a handle to the newly created cursor.
     * @throws NullPointerException if the request is null
     * @throws NoSuchTableException if the request is for an unknown table
     * @throws com.akiban.cserver.api.dml.NoSuchColumnException if the request includes a column that isn't defined for the requested table
     * @throws com.akiban.cserver.api.dml.NoSuchIndexException if the request is on an index that isn't defined for the requested table
     *
     */
    public CursorId openCursor(ScanRequest request)
            throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            InvalidOperationException
    {
        return openCursorForCollector( getRowCollector(request) );
    }

    /**
     * Opens a new cursor for scanning a table. This cursor will be stored in the current session, and a handle
     * to it will be returned for use in subsequent cursor-related methods. When you're finished with the cursor,
     * make sure to close it.
     * @param request the request specifications
     * @return a handle to the newly created cursor.
     * @throws NullPointerException if the request is null
     * @throws NoSuchTableException if the request is for an unknown table
     * @throws com.akiban.cserver.api.dml.NoSuchColumnException if the request includes a column that isn't defined for the requested table
     * @throws com.akiban.cserver.api.dml.NoSuchIndexException if the request is on an index that isn't defined for the requested table
     * @deprecated use {@link #openCursor(ScanRequest)}
     */
    @Deprecated
    public CursorId openCursor(LegacyScanRequest request)
            throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            InvalidOperationException
    {
        return openCursorForCollector( getRowCollector(request) );
    }

    private CursorId openCursorForCollector(RowCollector rc) throws InvalidOperationException {
        final CursorId cursor = newUniqueCursor(rc.getTableId());
        Object old = session.put(MODULE_NAME, cursor, new Cursor(rc));
        assert old == null : old;
        return cursor;
    }

    protected CursorId newUniqueCursor(int tableId) {
        return new CursorId(cursorsCount.incrementAndGet(), tableId);
    }

    protected RowCollector getRowCollector(ScanRequest request) throws NoSuchTableException, InvalidOperationException {
        ArgumentValidation.notNull("request", request);
        LegacyScanRequest legacy = new LegacyScanRequest(store(), idResolver(), request);
        return getRowCollector(legacy);
    }

    private RowCollector getRowCollector(LegacyScanRequest legacy) throws InvalidOperationException {
        try {
            return store().newRowCollector(legacy.tableId, legacy.indexId, legacy.scanFlags,
                    legacy.start, legacy.end, legacy.columnBitMap);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * <p>Performs a scan using the given cursor. This scan optionally limits the number of rows scanned, and passes
     * each row to the given RowOutput.</p>
     *
     * <p>This method returns whether there are more rows to be scanned; if it returns <tt>false</tt>, subsequent scans
     * on this cursor will raise a CursorIsFinishedException. The first invocation of this method on a cursor will never
     * throw a CursorIsFinishedException, even if there are now rows in the table.</p>
     *
     * <p>If the specified limit is <tt>&gt;= 0</tt>, this method will scan no more than that limit; it may scan
     * fewer, if the table has fewer remaining rows. If al limit is provided and this method returns <tt>true</tt>,
     * exactly <tt>limit</tt> rows will have been scanned; if a limit is provided and this method returns
     * <tt>false</tt>, the number of rows is <tt>&lt;=limit</tt>. If this is the case and you need to know how many
     * rows were actually scanned, using {@link RowOutput#getRowsCount()}.</p>
     *
     * <p>There is nothing special about a limit of 0; this method will scan no rows, and will return whether there
     * are more rows to be scanned. Note that passing a limit of 0 is essentially analogous to a "hasMore()" method.
     * As such, the Cursor will assume you now know there are no rows to scan, and any subsequent invocation of this
     * method will throw a CursorIsFinishedException -- even if that invocation uses a limit of 0. This is actually
     * a specific case of the general rule: if this method ever returns false, the next invocation using the same
     * cursor ID will throw a CursorIsFinishedException.</p>
     *
     * <p>The check for whether the cursor is finished is performed
     * before any limit tests; so if a previous invocation of this method returned <tt>false</tt> and you invoke
     * it on the same CursorId, even with a limit of 0, you will get a CursorIsFinishedException.</p>
     *
     * <p>Any negative limit will be regarded as infinity; this method will scan all remaining rows in the table.</p>
     *
     * <p>If the RowOutput throws an exception, it will be wrapped in a RowOutputException.</p>
     *
     * <p>If this method throws any exception, the cursor will be marked as finished.</p>
     * @param cursorId the cursor to scan
     * @param output the RowOutput to collect the given rows
     * @param limit if non-negative, the maximum number of rows to scan
     * @return whether more rows remain to be scanned
     * @throws NullPointerException if cursorId or output are null
     * @throws CursorIsFinishedException if a previous invocation of this method on the specified cursor returned
     * <tt>false</tt>
     * @throws CursorIsUnknownException if the given cursor is unknown (or has been closed)
     * @throws RowOutputException if the given RowOutput threw an exception while writing a row
     */
    public boolean scanSome(CursorId cursorId, RowOutput output, int limit)
            throws  CursorIsFinishedException,
            CursorIsUnknownException,
            RowOutputException,
            InvalidOperationException

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
     * @param output the output; see {@link #scanSome(CursorId, RowOutput, int)}
     * @param limit the limit, or negative value if none;  ee {@link #scanSome(CursorId, RowOutput, int)}
     * @return whether more rows remain to be scanned; see {@link #scanSome(CursorId, RowOutput, int)}
     * @throws CursorIsFinishedException see {@link #scanSome(CursorId, RowOutput, int)}
     * @throws RowOutputException see {@link #scanSome(CursorId, RowOutput, int)}
     * @throws InvalidOperationException see {@link #scanSome(CursorId, RowOutput, int)}
     * @see #scanSome(CursorId, RowOutput, int)
     */
    protected static boolean doScan(Cursor cursor, CursorId cursorId, RowOutput output, int limit)
            throws  CursorIsFinishedException,
            RowOutputException,
            InvalidOperationException
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
            }
            cursor.setScanning();
            boolean limitReached = (limit == 0);
            while ( (!limitReached) && rc.collectNextRow(output.getOutputBuffer()) ) {
                output.wroteRow();
                if (limit > 0) {
                    limitReached = (--limit) == 0;
                }
            }
            if (!limitReached) {
                output.wroteRow();
            }
            final boolean hasMore = rc.hasMore();
            if (!hasMore) {
                cursor.setFinished();
            }
            return limitReached || hasMore;
        } catch (Exception e) {
            cursor.setFinished();
            throw rethrow(e);
        }
    }

    /**
     * Closes the given cursor. This releases the relevant resources from the session.
     * @param cursorId the cursor to close
     * @throws NullPointerException if the cursor is null
     * @throws CursorIsUnknownException if the given cursor is unknown or has already been closed
     */
    public void closeCursor(CursorId cursorId) throws CursorIsUnknownException {
        ArgumentValidation.notNull("cursor ID", cursorId);
        final Cursor cursor = session.remove(MODULE_NAME, cursorId);
        if (cursor == null) {
            throw new CursorIsUnknownException(cursorId);
        }
    }

    /**
     * <p>Returns all open cursors. It is not necessarily safe to call {@linkplain #scanSome(CursorId, RowOutput, int)}
     * on all of these cursors, since some may have reached their end. But it is safe to close each of these cursors
     * (unless, of course, another thread closes them first).</p>
     *
     * <p>If this method returns an empty Set, it will be unmodifiable. Otherwise, it is safe to modify.</p>
     * @return the set of open (but possibly finished) cursors
     */
    public Set<CursorId> getCursors() {
        throw new UnsupportedOperationException("need to revisit whether we need this; "
                + "it may require modifications to Session to make this easy");
    }

    /**
     * Writes a row to the specified table. If the table contains an autoincrement column, and a value for that
     * column is not specified, the generated value will be returned.
     *
     * <p><strong>Note:</strong> The chunkserver doesn't yet support autoincrement, so for now, this method
     * will always return <tt>null</tt>. This is expected to change in the nearish future.</p>
     * @param tableId the table to write to
     * @param row the row to write
     * @return the generated autoincrement value, or <tt>null</tt> if none was generated
     * @throws NullPointerException if the given tableId or row are null
     * @throws DuplicateKeyException if the row would create a duplicate of a unique column
     * @throws NoSuchTableException if the specified table doesn't exist
     * @throws TableDefinitionMismatchException if the RowData provided doesn't match the definition of the table
     * @throws UnsupportedModificationException if the table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     */
    public Long writeRow(TableId tableId, NiceRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            TableDefinitionMismatchException,
            DuplicateKeyException,
            InvalidOperationException
    {
        final RowData rowData = niceRowToRowData(tableId, row);
        try {
            store().writeRow(rowData);
            return null;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * Writes a row
     * @param rowData the wrote to write
     * @return null
     * @throws InvalidOperationException see {@linkplain #writeRow(TableId, NiceRow)}
     * @deprecated use {@linkplain #writeRow(TableId, NiceRow)}
     */
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
    
    /**
     * <p>Deletes a row, possibly cascading the deletion to its children rows.</p>
     * @param tableId the table to delete from
     * @param row the row to delete
     * @throws NullPointerException if either the given table ID or row are null
     * @throws NoSuchTableException if the specified table is unknown
     * @throws UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws ForeignKeyConstraintDMLException if the deletion was blocked by at least one child table
     * @throws NoSuchRowException if the specified row doesn't exist
     */
    public void deleteRow(TableId tableId, NiceRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            InvalidOperationException
    {
        final RowData rowData = niceRowToRowData(tableId, row);
        deleteRow(rowData);
    }

    /**
     * <p>Deletes a row, possibly cascading the deletion to its children rows.</p>
     * @param rowData the row to delete
     * @throws NullPointerException if either the given table ID or row are null
     * @throws NoSuchTableException if the specified table is unknown
     * @throws UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws ForeignKeyConstraintDMLException if the deletion was blocked by at least one child table
     * @throws NoSuchRowException if the specified row doesn't exist
     * @deprecated use {@link #deleteRow(TableId, NiceRow)}
     */
    @Deprecated
    public void deleteRow(RowData rowData) throws InvalidOperationException
    {
        try {
            store().deleteRow(rowData);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private RowData niceRowToRowData(TableId tableId, NiceRow row) throws NoSuchTableException {
        final RowDef rowDef = store().getRowDefCache().getRowDef(tableId.getTableId(idResolver()));
        final RowData rowData = row.toRowData(rowDef, idResolver());
        return rowData;
    }

    /**
     * <p>Updates a row, possibly cascading updates to its PK to children rows.</p>
     * @param tableId the table to update
     * @param oldRow the row to update
     * @param newRow the row's new values
     * @throws NullPointerException if any of the arguments are <tt>null</tt>
     * @throws DuplicateKeyException if the update would create a duplicate of a unique column
     * @throws TableDefinitionMismatchException if either (or both) RowData objects don't match the specification
     * of the given TableId.
     * @throws NoSuchTableException if the given tableId doesn't exist
     * @throws UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws ForeignKeyConstraintDMLException if the update was blocked by at least one child table
     * @throws NoSuchRowException
     */
    public void updateRow(TableId tableId, NiceRow oldRow, NiceRow newRow)
            throws  NoSuchTableException,
            DuplicateKeyException,
            TableDefinitionMismatchException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            InvalidOperationException
    {
        final RowData oldData = niceRowToRowData(tableId, oldRow);
        final RowData newData = niceRowToRowData(tableId, newRow);

        updateRow(oldData, newData);
    }

    /**
     * Updates a row
     * @param oldData the old data
     * @param newData the new data
     * @throws InvalidOperationException if exception occurred
     * @deprecated use {@link #updateRow(TableId, NiceRow, NiceRow)}
     */
    @Deprecated
    public void updateRow(RowData oldData, RowData newData) throws InvalidOperationException {
        matchRowDatas(oldData, newData);
        try {
            store().updateRow(oldData, newData);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * Truncates the given table, possibly cascading the truncate to child tables.
     *
     * <p><strong>NOTE: IGNORE THE FOLLOWING. IT ISN'T VERIFIED, ALMOST DEFINITELY NOT TRUE, ETC. IT'S FOR
     * FUTURE POSSIBILITIES ONLY</strong></p>
     *
     * <p>Because truncating is intended to be fast, this method will simply truncate all child tables whose
     * relationship is CASCADE; it will not delete rows in those tables based on their existence in the parent table.
     * In particular, this means that orphan rows will also be deleted,</p>
     * @param tableId the table to truncate
     * @return a map of affected tables, as described above
     * @throws NullPointerException if the given tableId is null
     * @throws NoSuchTableException if the given table doesn't exist
     * @throws UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws ForeignKeyConstraintDMLException if the truncate was blocked by at least one child table
     */
    public void truncateTable(TableId tableId)
            throws NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            InvalidOperationException
    {
        try {
            store().truncateTable(tableId.getTableId(idResolver()) );
        } catch (Exception e) {
            rethrow(e);
        }
    }
}
