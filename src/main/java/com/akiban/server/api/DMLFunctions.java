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

import java.util.List;
import java.util.Set;

import com.akiban.server.RowData;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.DuplicateKeyException;
import com.akiban.server.api.dml.ForeignKeyConstraintDMLException;
import com.akiban.server.api.dml.NoSuchColumnException;
import com.akiban.server.api.dml.NoSuchIndexException;
import com.akiban.server.api.dml.NoSuchRowException;
import com.akiban.server.api.dml.TableDefinitionMismatchException;
import com.akiban.server.api.dml.UnsupportedModificationException;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.ConcurrentScanAndUpdateException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.CursorIsFinishedException;
import com.akiban.server.api.dml.scan.CursorIsUnknownException;
import com.akiban.server.api.dml.scan.CursorState;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.OldAISException;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.RowOutputException;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.api.dml.scan.TableDefinitionChangedException;
import com.akiban.server.service.session.Session;

@SuppressWarnings("unused")
public interface DMLFunctions {
    /**
     * Gets the table statistics for the specified table, optionally updating the statistics first. If you request
     * this update, the method may take significantly longer.
     * @param tableId the table for which to get (and possibly update) statistics
     * @param updateFirst whether to update the statistics before returning them
     * @return the table's statistics
     * @throws NullPointerException if tableId is null
     * @throws com.akiban.server.api.common.NoSuchTableException if the specified table doesn't exist
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    TableStatistics getTableStatistics(Session session, int tableId, boolean updateFirst)
    throws NoSuchTableException,
            GenericInvalidOperationException;

    /**
     * Opens a new cursor for scanning a table. This cursor will be stored in the current session, and a handle
     * to it will be returned for use in subsequent cursor-related methods. When you're finished with the cursor,
     * make sure to close it.
     *
     * @param session the context in which this cursor is opened
     * @param request the request specifications
     * @return a handle to the newly created cursor.
     * @throws NullPointerException if the request is null
     * @throws com.akiban.server.api.common.NoSuchTableException if the request is for an unknown table
     * @throws NoSuchColumnException if the request includes a column that isn't defined for the requested table
     * @throws NoSuchIndexException if the request is on an index that isn't defined for the requested table
     * @throws OldAISException if the given AIS generation is out of date
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    CursorId openCursor(Session session, int knownAIS, ScanRequest request)
            throws NoSuchTableException, NoSuchColumnException, NoSuchIndexException, GenericInvalidOperationException, OldAISException;

    /**
     * <p>Performs a scan using the given cursor. This scan optionally limits the number of rows scanned, and passes
     * each row to the given RowOutput.</p>
     *
     * <p>This method returns whether there are more rows to be scanned; if it returns <tt>false</tt>, subsequent scans
     * on this cursor will raise a CursorIsFinishedException. The first invocation of this method on a cursor will never
     * throw a CursorIsFinishedException, even if there are no rows in the table.</p>
     *
     * <p>If the specified limit is <tt>&gt;= 0</tt>, this method will scan no more than that limit; it may scan
     * fewer, if the table has fewer remaining rows. If al limit is provided and this method returns <tt>true</tt>,
     * exactly <tt>limit</tt> rows will have been scanned; if a limit is provided and this method returns
     * <tt>false</tt>, the number of rows is <tt>&lt;=limit</tt>. If this is the case and you need to know how many
     * rows were actually scanned, using {@link LegacyRowOutput#getRowsCount()}.</p>
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
     * @param session the context in which the cursor was opened
     * @param output the RowOutput to collect the given rows
     * @return whether more rows remain to be scanned
     * @throws NullPointerException if cursorId or output are null
     * @throws CursorIsFinishedException if a previous invocation of this method on the specified cursor returned
     * <tt>false</tt>
     * @throws CursorIsUnknownException if the given cursor is unknown (or has been closed)
     * @throws RowOutputException if the given RowOutput threw an exception while writing a row
     * @throws ConcurrentScanAndUpdateException if an update has happened since opening the scan and this call
     * @throws GenericInvalidOperationException if some other exception occurred
     * @throws BufferFullException if the output buffer couldn't fit the rows
     */
    boolean scanSome(Session session, CursorId cursorId, LegacyRowOutput output)
    throws  CursorIsFinishedException,
            CursorIsUnknownException,
            RowOutputException,
            BufferFullException,
            ConcurrentScanAndUpdateException,
            TableDefinitionChangedException,
            GenericInvalidOperationException;

    /**
     * <p>Performs a scan using the given cursor. This scan optionally limits the number of rows scanned, and passes
     * each row to the given RowOutput.</p>
     *
     * <p>This method is similar to its LegacyRowOutput-taking twin, but it takes a RowOutput instead. Converting
     * from legacy RowData instances to NewRows requires RowDef knowledge that the caller may not have, but
     * DMLFunctions implementations should have this information. Thus, this call acts as both a convenience
     * and a separator of concerns.</p> 
     *
     * <p>This method returns whether there are more rows to be scanned; if it returns <tt>false</tt>, subsequent scans
     * on this cursor will raise a CursorIsFinishedException. The first invocation of this method on a cursor will never
     * throw a CursorIsFinishedException, even if there are now rows in the table.</p>
     *
     * <p>If the specified limit is <tt>&gt;= 0</tt>, this method will scan no more than that limit; it may scan
     * fewer, if the table has fewer remaining rows. If al limit is provided and this method returns <tt>true</tt>,
     * exactly <tt>limit</tt> rows will have been scanned; if a limit is provided and this method returns
     * <tt>false</tt>, the number of rows is <tt>&lt;=limit</tt>. If this is the case and you need to know how many
     * rows were actually scanned, using {@link LegacyRowOutput#getRowsCount()}.</p>
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
     * @param session the context in which the cursor was opened
     * @param output the RowOutput to collect the given rows
     * @return whether more rows remain to be scanned
     * @throws NullPointerException if cursorId or output are null
     * @throws CursorIsFinishedException if a previous invocation of this method on the specified cursor returned
     * <tt>false</tt>
     * @throws CursorIsUnknownException if the given cursor is unknown (or has been closed)
     * @throws RowOutputException if the given RowOutput threw an exception while writing a row
     * @throws NoSuchTableException if the table ID specified by cursorId isn't recognized
     * @throws ConcurrentScanAndUpdateException if an update has happened since opening the scan and this call
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    boolean scanSome(Session session, CursorId cursorId, RowOutput output)
            throws  CursorIsFinishedException,
            CursorIsUnknownException,
            RowOutputException,
            NoSuchTableException,
            ConcurrentScanAndUpdateException,
            TableDefinitionChangedException,
            GenericInvalidOperationException;

    /**
     * Closes the given cursor. This releases the relevant resources from the session.
     * @param cursorId the cursor to close
     * @param session the context in which the cursor was created
     * @throws NullPointerException if the cursor is null
     * @throws CursorIsUnknownException if the given cursor is unknown or has already been closed
     */
    void closeCursor(Session session, CursorId cursorId) throws CursorIsUnknownException;

    /**
     * Gets the state of the given cursor. This method will never throw an exception; if the cursor is unknown,
     * it will return the special enum {@link CursorState#UNKNOWN_CURSOR}
     * @param cursorId the cursorID to get state for
     * @param session the session in which the cursor is defined
     * @return the cursor's state
     */
    CursorState getCursorState(Session session, CursorId cursorId);

    /**
     * <p>Returns all open cursors. It is not necessarily safe to call
     * {@linkplain #scanSome(Session, CursorId, LegacyRowOutput)}
     * on all of these cursors, since some may have reached their end. But it is safe to close each of these cursors
     * (unless, of course, another thread closes them first).</p>
     *
     * <p>If this method returns an empty Set, it will be unmodifiable. Otherwise, it is safe to modify.</p>
     * @param session the session whose cursors we should return
     * @return the set of open (but possibly finished) cursors
     * @throws NullPointerException if
     */
    Set<CursorId> getCursors(Session session);

    /**
     * Converts a NewRow to a RowData; mostly useful for debugging purposes.
     * @param row the row to convert
     * @return the converted row
     * @throws NoSuchTableException if the NewRow's TableId can't be resolved
     */
    RowData convertNewRow(NewRow row) throws NoSuchTableException;

    /**
     * Converts a RowData to a NewRow. This conversion requires a RowDef, which the caller may not have, but which
     * implementers of this interface should.
     * @param rowData the row to convert
     * @return a NewRow representation of the RowData
     * @throws NoSuchTableException if the rowData refers to a rowDefId that isn't known
     */
    NewRow convertRowData(RowData rowData) throws NoSuchTableException;

    /**
     * Converts several RowData objects at once. This is not just a convenience; it lets implementations of this
     * class cache RowDefs they need, which could save time.
     * @param rowDatas the rows to convert
     * @return a List of NewRows, each of which is a converted RowData
     * @throws NoSuchTableException if any rowDatas refer to a rowDefId that isn't known
     */
    List<NewRow> convertRowDatas(List<RowData> rowDatas) throws NoSuchTableException;

    /**
     * Writes a row to the specified table. If the table contains an autoincrement column, and a value for that
     * column is not specified, the generated value will be returned.
     *
     * <p><strong>Note:</strong> The chunkserver doesn't yet support autoincrement, so for now, this method
     * will always return <tt>null</tt>. This is expected to change in the nearish future.</p>
     * @param row the row to write
     * @return the generated autoincrement value, or <tt>null</tt> if none was generated
     * @throws NullPointerException if the given tableId or row are null
     * @throws DuplicateKeyException if the row would create a duplicate of a unique column
     * @throws NoSuchTableException if the specified table doesn't exist
     * @throws TableDefinitionMismatchException if the RowData provided doesn't match the definition of the table
     * @throws UnsupportedModificationException if the table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    Long writeRow(Session session, NewRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            TableDefinitionMismatchException,
            DuplicateKeyException,
            GenericInvalidOperationException;

    /**
     * <p>Deletes a row, possibly cascading the deletion to its children rows.</p>
     * @param row the row to delete
     * @throws NullPointerException if either the given table ID or row are null
     * @throws NoSuchTableException if the specified table is unknown
     * @throws UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws ForeignKeyConstraintDMLException if the deletion was blocked by at least one child table
     * @throws NoSuchRowException if the specified row doesn't exist
     * @throws TableDefinitionMismatchException if the row to delete isn't of the appropriate type
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    void deleteRow(Session session, NewRow row)
    throws  NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            TableDefinitionMismatchException,
            GenericInvalidOperationException;

    /**
     * <p>Updates a row, possibly cascading updates to its PK to children rows.</p>
     * @param oldRow the row to update
     * @param newRow the row's new values
     * @param columnSelector specifies which columns are being updated
     * @throws NullPointerException if any of the arguments are <tt>null</tt>
     * @throws DuplicateKeyException if the update would create a duplicate of a unique column
     * @throws TableDefinitionMismatchException if either (or both) RowData objects don't match the specification
     * of the given TableId.
     * @throws NoSuchTableException if the given tableId doesn't exist
     * @throws UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws ForeignKeyConstraintDMLException if the update was blocked by at least one child table
     * @throws NoSuchRowException if the specified oldRow doesn't exist
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    void updateRow(Session session, NewRow oldRow, NewRow newRow, ColumnSelector columnSelector)
    throws  NoSuchTableException,
            DuplicateKeyException,
            TableDefinitionMismatchException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            NoSuchRowException,
            GenericInvalidOperationException;

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
     * @throws NullPointerException if the given tableId is null
     * @throws com.akiban.server.api.common.NoSuchTableException if the given table doesn't exist
     * @throws UnsupportedModificationException if the specified table can't be modified (e.g., if it's a group table or
     * <tt>akiban_information_schema</tt> table)
     * @throws ForeignKeyConstraintDMLException if the truncate was blocked by at least one child table
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    void truncateTable(Session session, int tableId)
    throws NoSuchTableException,
            UnsupportedModificationException,
            ForeignKeyConstraintDMLException,
            GenericInvalidOperationException;

}
