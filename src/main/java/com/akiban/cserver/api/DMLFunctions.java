package com.akiban.cserver.api;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.*;
import com.akiban.cserver.api.dml.scan.*;
import com.akiban.cserver.store.Store;

import java.util.Set;

public interface DMLFunctions {
    long getAutoIncrementValue(TableId tableId) throws NoSuchTableException, InvalidOperationException;

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
    long countRowsExactly(ScanRange range)
    throws  NoSuchTableException,
            UnsupportedReadException,
            InvalidOperationException;

    /**
     * Returns the exact number of rows in this table. This may take a while, as it could require a full
     * table scan. Group tables have an undefined row count, so this method will fail if the requested
     * table is a group table.
     * @param range the table, columns and range to count
     * @return the number of rows in the specified table
     * @throws NullPointerException if tableId is null
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table is unknown
     * @throws com.akiban.cserver.api.dml.UnsupportedReadException if the specified table is a group table
     * @deprecated use {@link #countRowsExactly(com.akiban.cserver.api.dml.scan.ScanRange)}
     */
    @Deprecated
    long countRowsExactly(LegacyScanRange range)
            throws  NoSuchTableException,
            UnsupportedReadException,
            InvalidOperationException;

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
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table is unknown
     * @throws com.akiban.cserver.api.dml.UnsupportedReadException if the specified table is a group table
     */
    long countRowsApproximately(ScanRange range)
            throws  NoSuchTableException,
            UnsupportedReadException,
            InvalidOperationException;

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
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table is unknown
     * @throws com.akiban.cserver.api.dml.UnsupportedReadException if the specified table is a group table
     * @deprecated use {@link #countRowsApproximately(com.akiban.cserver.api.dml.scan.ScanRange)}
     */
    @Deprecated
    long countRowsApproximately(LegacyScanRange range)
            throws  NoSuchTableException,
            UnsupportedReadException,
            InvalidOperationException;

    /**
     * Gets the table statistics for the specified table, optionally updating the statistics first. If you request
     * this update, the method may take significantly longer.
     * @param tableId the table for which to get (and possibly update) statistics
     * @param updateFirst whether to update the statistics before returning them
     * @return the table's statistics
     * @throws NullPointerException if tableId is null
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table doesn't exist
     */
    TableStatistics getTableStatistics(TableId tableId, boolean updateFirst)
            throws  NoSuchTableException,
            InvalidOperationException;

    /**
     * Opens a new cursor for scanning a table. This cursor will be stored in the current session, and a handle
     * to it will be returned for use in subsequent cursor-related methods. When you're finished with the cursor,
     * make sure to close it.
     * @param request the request specifications
     * @return a handle to the newly created cursor.
     * @throws NullPointerException if the request is null
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the request is for an unknown table
     * @throws com.akiban.cserver.api.dml.NoSuchColumnException if the request includes a column that isn't defined for the requested table
     * @throws com.akiban.cserver.api.dml.NoSuchIndexException if the request is on an index that isn't defined for the requested table
     *
     */
    CursorId openCursor(ScanRequest request)
            throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            InvalidOperationException;

    /**
     * Opens a new cursor for scanning a table. This cursor will be stored in the current session, and a handle
     * to it will be returned for use in subsequent cursor-related methods. When you're finished with the cursor,
     * make sure to close it.
     * @param request the request specifications
     * @return a handle to the newly created cursor.
     * @throws NullPointerException if the request is null
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the request is for an unknown table
     * @throws com.akiban.cserver.api.dml.NoSuchColumnException if the request includes a column that isn't defined for the requested table
     * @throws com.akiban.cserver.api.dml.NoSuchIndexException if the request is on an index that isn't defined for the requested table
     * @deprecated use {@link #openCursor(com.akiban.cserver.api.dml.scan.ScanRequest)}
     */
    @Deprecated
    CursorId openCursor(LegacyScanRequest request)
            throws  NoSuchTableException,
            NoSuchColumnException,
            NoSuchIndexException,
            InvalidOperationException;

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
     * rows were actually scanned, using {@link com.akiban.cserver.api.dml.scan.LegacyRowOutput#getRowsCount()}.</p>
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
     * @throws com.akiban.cserver.api.dml.scan.CursorIsFinishedException if a previous invocation of this method on the specified cursor returned
     * <tt>false</tt>
     * @throws com.akiban.cserver.api.dml.scan.CursorIsUnknownException if the given cursor is unknown (or has been closed)
     * @throws com.akiban.cserver.api.dml.scan.RowOutputException if the given RowOutput threw an exception while writing a row
     */
    boolean scanSome(CursorId cursorId, LegacyRowOutput output, int limit)
            throws  CursorIsFinishedException,
            CursorIsUnknownException,
            RowOutputException,
            InvalidOperationException;

    /**
     * Closes the given cursor. This releases the relevant resources from the session.
     * @param cursorId the cursor to close
     * @throws NullPointerException if the cursor is null
     * @throws com.akiban.cserver.api.dml.scan.CursorIsUnknownException if the given cursor is unknown or has already been closed
     */
    void closeCursor(CursorId cursorId) throws CursorIsUnknownException;

    /**
     * <p>Returns all open cursors. It is not necessarily safe to call {@linkplain #scanSome(com.akiban.cserver.api.dml.scan.CursorId, com.akiban.cserver.api.dml.scan.LegacyRowOutput , int)}
     * on all of these cursors, since some may have reached their end. But it is safe to close each of these cursors
     * (unless, of course, another thread closes them first).</p>
     *
     * <p>If this method returns an empty Set, it will be unmodifiable. Otherwise, it is safe to modify.</p>
     * @return the set of open (but possibly finished) cursors
     */
    Set<CursorId> getCursors();

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
     * @throws com.akiban.cserver.api.dml.DuplicateKeyException if the row would create a duplicate of a unique column
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table doesn't exist
     * @throws com.akiban.cserver.api.dml.TableDefinitionMismatchException if the RowData provided doesn't match the definition of the table
     * @throws com.akiban.cserver.api.dml.UnsupportedModificationException if the table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     */
    Long writeRow(TableId tableId, NiceRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            TableDefinitionMismatchException,
            DuplicateKeyException,
            InvalidOperationException;

    /**
     * Writes a row
     * @param rowData the wrote to write
     * @return null
     * @throws com.akiban.cserver.InvalidOperationException see {@linkplain #writeRow(com.akiban.cserver.api.common.TableId, com.akiban.cserver.api.dml.scan.NiceRow)}
     * @deprecated use {@linkplain #writeRow(com.akiban.cserver.api.common.TableId, com.akiban.cserver.api.dml.scan.NiceRow)}
     */
    @Deprecated
    Long writeRow(RowData rowData) throws InvalidOperationException;

    /**
     * <p>Deletes a row, possibly cascading the deletion to its children rows.</p>
     * @param tableId the table to delete from
     * @param row the row to delete
     * @throws NullPointerException if either the given table ID or row are null
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table is unknown
     * @throws com.akiban.cserver.api.dml.UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws com.akiban.cserver.api.dml.ForeignKeyConstraintDMLException if the deletion was blocked by at least one child table
     * @throws com.akiban.cserver.api.dml.NoSuchRowException if the specified row doesn't exist
     */
    void deleteRow(TableId tableId, NiceRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            InvalidOperationException;

    /**
     * <p>Deletes a row, possibly cascading the deletion to its children rows.</p>
     * @param rowData the row to delete
     * @throws NullPointerException if either the given table ID or row are null
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table is unknown
     * @throws com.akiban.cserver.api.dml.UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws com.akiban.cserver.api.dml.ForeignKeyConstraintDMLException if the deletion was blocked by at least one child table
     * @throws com.akiban.cserver.api.dml.NoSuchRowException if the specified row doesn't exist
     * @deprecated use {@link #deleteRow(com.akiban.cserver.api.common.TableId, com.akiban.cserver.api.dml.scan.NiceRow)}
     */
    @Deprecated
    void deleteRow(RowData rowData) throws InvalidOperationException;

    NiceRow convertRowData(RowData rowData);

    /**
     * <p>Updates a row, possibly cascading updates to its PK to children rows.</p>
     * @param tableId the table to update
     * @param oldRow the row to update
     * @param newRow the row's new values
     * @throws NullPointerException if any of the arguments are <tt>null</tt>
     * @throws com.akiban.cserver.api.dml.DuplicateKeyException if the update would create a duplicate of a unique column
     * @throws com.akiban.cserver.api.dml.TableDefinitionMismatchException if either (or both) RowData objects don't match the specification
     * of the given TableId.
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the given tableId doesn't exist
     * @throws com.akiban.cserver.api.dml.UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws com.akiban.cserver.api.dml.ForeignKeyConstraintDMLException if the update was blocked by at least one child table
     * @throws com.akiban.cserver.api.dml.NoSuchRowException
     */
    void updateRow(TableId tableId, NiceRow oldRow, NiceRow newRow)
            throws  NoSuchTableException,
            DuplicateKeyException,
            TableDefinitionMismatchException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            InvalidOperationException;

    /**
     * Updates a row
     * @param oldData the old data
     * @param newData the new data
     * @throws com.akiban.cserver.InvalidOperationException if exception occurred
     * @deprecated use {@link #updateRow(com.akiban.cserver.api.common.TableId, com.akiban.cserver.api.dml.scan.NiceRow, com.akiban.cserver.api.dml.scan.NiceRow)}
     */
    @Deprecated
    void updateRow(RowData oldData, RowData newData) throws InvalidOperationException;

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
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the given table doesn't exist
     * @throws com.akiban.cserver.api.dml.UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws com.akiban.cserver.api.dml.ForeignKeyConstraintDMLException if the truncate was blocked by at least one child table
     */
    void truncateTable(TableId tableId)
            throws NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            InvalidOperationException;

    public interface LegacyScanRange {
        RowData getStart();
        RowData getEnd();
        byte[] getColumnBitMap();
        int getTableId();
    }

    public interface LegacyScanRequest extends LegacyScanRange {
        int getIndexId();
        int getScanFlags();
    }
}
