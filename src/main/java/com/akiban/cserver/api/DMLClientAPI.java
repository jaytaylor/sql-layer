package com.akiban.cserver.api;

import com.akiban.cserver.RowData;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.*;
import com.akiban.cserver.api.dml.scan.CursorId;
import com.akiban.cserver.api.dml.scan.CursorIsFinishedException;
import com.akiban.cserver.api.dml.scan.CursorIsUnknownException;
import com.akiban.cserver.api.dml.scan.ScanRequest;

import java.util.Map;
import java.util.Set;

public final class DMLClientAPI {

    /**
     * Returns the exact number of rows in this table. This may take a while, as it could require a full
     * table scan. Group tables have an undefined row count, so this method will fail if the requested
     * table is a group table.
     * @param tableId the table to count
     * @return the number of rows in the specified table
     * @throws NullPointerException if tableId is null
     * @throws com.akiban.cserver.api.dml.NoSuchTableException if the specified table is unknown
     * @throws com.akiban.cserver.api.dml.UnsupportedReadException if the specified table is a group table
     */
    public int countRowsExactly(TableId tableId)
    throws NoSuchTableException,
            UnsupportedReadException
    {
        throw new UnsupportedOperationException();
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
     * @param tableId the table to count
     * @return the number of rows in the specified table
     * @throws NullPointerException if tableId is null
     * @throws NoSuchTableException if the specified table is unknown
     * @throws UnsupportedReadException if the specified table is a group table
     */
    public int countRowsApproximately(TableId tableId)
            throws  NoSuchTableException,
            UnsupportedReadException
    {
        throw new UnsupportedOperationException();
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
    public TableStatistics getTableStatistics(TableId tableId, boolean updateFirst) throws NoSuchTableException {
        throw new UnsupportedOperationException();
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
            NoSuchIndexException
    {
        throw new UnsupportedOperationException();
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
     * <tt>false</tt>, the number of rows is <tt.&lt;=limit</tt>. If this is the case and you need to know how many
     * rows were actually scanned, using {@link com.akiban.cserver.api.dml.RowOutput#getRowsCount()}.</p>
     *
     * <p>There is nothing special about a limit of 0; this method will scan no rows, and will return whether there
     * are more rows to be scanned. Any negative limit will be regarded as infinity; this method will scan
     * all remaining rows in the table.</p>
     *
     * <p>If the RowOutput throws an exception, it will be wrapped in a RowOutputException.</p>
     * @param cursorId the cursor to scan
     * @param output the RowOutput to collect the given rows
     * @param limit if non-negative, the maximum number of rows to scan
     * @return whether more rows remain to be scanned
     * @throws NullPointerException if cursorId or output are null
     * @throws com.akiban.cserver.api.dml.scan.CursorIsFinishedException if a previous invocation of this method on the specified cursor returned
     * <tt>false</tt>
     * @throws CursorIsUnknownException if the given cursor is unknown (or has been closed)
     * @throws com.akiban.cserver.api.dml.RowOutputException if the given RowOutput threw an exception while writing a row
     */
    public boolean scanSome(CursorId cursorId, RowOutput output, int limit)
    throws CursorIsFinishedException,
            CursorIsUnknownException,
            RowOutputException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes the given cursor. This releases the relevant resources from the session.
     * @param cursorId the cursor to close
     * @throws NullPointerException if the cursor is null
     * @throws CursorIsUnknownException if the given cursor is unknown or has already been closed
     */
    public void closeCursor(CursorId cursorId) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
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
    public Long writeRow(TableId tableId, RowData row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            TableDefinitionMismatchException,
            DuplicateKeyException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * <p>Deletes a row, possibly cascading the deletion to its children rows. This method returns a map whose keys
     * are all affected tables, and whose values are the number of rows in that table that were deleted.</p>
     *
     * <p>For instance, if you have a COI schema and delete <tt>customer_id=3</tt>, which has two orders, each of
     * which has three items, the returned map will be: <tt>{ customers : 1,  orders : 2,  items : 6}</tt>. If this
     * method returns without exception, the returned map is guaranteed to contain at least one entry, whose key
     * is equal to the tableId you pass in and whose value is <tt>1</tt></p>
     * @param tableId the table to delete from
     * @param row the row to delete
     * @return a map of affected tables, as described above
     * @throws NullPointerException if either the given table ID or row are null
     * @throws NoSuchTableException if the specified table is unknown
     * @throws UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws ForeignKeyConstraintDMLException if the deletion was blocked by at least one child table
     * @throws NoSuchRowException if the specified row doesn't exist
     */
    public Map<TableId,Integer> deleteRow(TableId tableId, RowData row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * <p>Updates a row, possibly cascading updates to its PK to children rows. This method returns a Map similar to
     * {@linkplain #deleteRow(TableId, RowData)}. In the example mentioned in that method, instead of deleting
     * <tt>customer_id=3</tt>, we would be updating its value to, say, <tt>4</tt>.</p>
     *
     * <p>If the update doesn't change the row's PKs, the resultant Map will contain exactly one entry, whose
     * key equals the given tableId.</p>
     * @param tableId the table to update
     * @param oldRow the row to update
     * @param newRow the row's new values
     * @return a map of affected tables, as described above and in {@link #deleteRow(TableId, RowData)}
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
    public Map<TableId,Integer> updateRow(TableId tableId, RowData oldRow, RowData newRow)
    throws  NoSuchTableException,
            DuplicateKeyException,
            TableDefinitionMismatchException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Truncates the given table, possibly cascading the truncate to child tables. This method returns a Map similar to
     * {@linkplain #deleteRow(TableId, RowData)}, except that the entry for <tt>tableId</tt> may be greater than 1.
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
    public Map<TableId,Integer> truncateTable(TableId tableId)
    throws NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException
    {
        throw new UnsupportedOperationException();
    }
}
