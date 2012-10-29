/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.api;

import java.util.List;
import java.util.Set;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.CursorState;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.error.ConcurrentScanAndUpdateException;
import com.akiban.server.error.CursorIsFinishedException;
import com.akiban.server.error.CursorIsUnknownException;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.ForeignKeyConstraintDMLException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.error.NoSuchRowException;
import com.akiban.server.error.OldAISException;
import com.akiban.server.error.RowOutputException;
import com.akiban.server.error.TableDefinitionChangedException;
import com.akiban.server.error.TableDefinitionMismatchException;
import com.akiban.server.error.UnsupportedModificationException;
import com.akiban.server.service.session.Session;
import com.persistit.exception.PersistitException;

@SuppressWarnings("unused")
public interface DMLFunctions {
    /**
     * Gets the table statistics for the specified table, optionally updating the statistics first. If you request
     * this update, the method may take significantly longer.
     * @param tableId the table for which to get (and possibly update) statistics
     * @param updateFirst whether to update the statistics before returning them
     * @return the table's statistics
     * @throws Exception 
     * @throws NullPointerException if tableId is null
     */
    TableStatistics getTableStatistics(Session session, int tableId, boolean updateFirst);

    /**
     * Opens a new cursor for scanning a table. This cursor will be stored in the current session, and a handle
     * to it will be returned for use in subsequent cursor-related methods. When you're finished with the cursor,
     * make sure to close it.
     *
     * @param session the context in which this cursor is opened
     * @param request the request specifications
     * @return a handle to the newly created cursor.
     * @throws Exception 
     * @throws NullPointerException if the request is null
     */
    CursorId openCursor(Session session, int knownAIS, ScanRequest request);

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
     *
     * @param session the context in which the cursor was opened
     * @param cursorId the cursor to scan
     * @param output the RowOutput to collect the given rows
     * @return whether more rows remain to be scanned
     * @throws NullPointerException if cursorId or output are null
     * @throws BufferFullException if the output buffer couldn't fit the rows

     */
    void scanSome(Session session, CursorId cursorId, LegacyRowOutput output)
    throws  BufferFullException;

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
     * @throws Exception 
     * @throws NullPointerException if cursorId or output are null
     */
    void scanSome(Session session, CursorId cursorId, RowOutput output);

    /**
     * Closes the given cursor. This releases the relevant resources from the session.
     * @param cursorId the cursor to close
     * @param session the context in which the cursor was created
     * @throws NullPointerException if the cursor is null
     * @throws CursorIsUnknownException if the given cursor is unknown or has already been closed
     */
    void closeCursor(Session session, CursorId cursorId);

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
     */
    RowData convertNewRow(NewRow row);

    /**
     * Wraps a RowData in a NewRow. This conversion requires a RowDef, which the caller may not have, but which
     * implementers of this interface should.
     * @param rowData the row to wrap
     * @return a NewRow representation of the RowData
     */
    NewRow wrapRowData(Session session, RowData rowData);

    /**
     * Converts a RowData to a NewRow. This conversion requires a RowDef, which the caller may not have, but which
     * implementers of this interface should.
     * @param rowData the row to convert
     * @return a NewRow representation of the RowData
     */
    NewRow convertRowData(Session session, RowData rowData);

    /**
     * Converts several RowData objects at once. This is not just a convenience; it lets implementations of this
     * class cache RowDefs they need, which could save time.
     * @param rowDatas the rows to convert
     * @return a List of NewRows, each of which is a converted RowData
     */
    List<NewRow> convertRowDatas(Session session, List<RowData> rowDatas);

    /**
     * Writes a row to the specified table. If the table contains an autoincrement column, and a value for that
     * column is not specified, the generated value will be returned.
     *
     * <p><strong>Note:</strong> The chunkserver doesn't yet support autoincrement, so for now, this method
     * will always return <tt>null</tt>. This is expected to change in the nearish future.</p>
     * @param row the row to write
     * @return the generated autoincrement value, or <tt>null</tt> if none was generated
     * @throws PersistitException 
     * @throws Exception 
     * @throws NullPointerException if the given tableId or row are null
     */
    Long writeRow(Session session, NewRow row);

    /**
     * <p>Deletes a row, possibly cascading the deletion to its children rows.</p>
     * @param row the row to delete
     * @throws PersistitException 
     * @throws Exception 
     * @throws NullPointerException if either the given table ID or row are null
     */
    void deleteRow(Session session, NewRow row);

    /**
     * <p>Updates a row, possibly cascading updates to its PK to children rows.</p>
     * @param oldRow the row to update
     * @param newRow the row's new values
     * @param columnSelector specifies which columns are being updated
     * @throws Exception 
     * @throws NullPointerException if any of the arguments are <tt>null</tt>
     */
    void updateRow(Session session, NewRow oldRow, NewRow newRow, ColumnSelector columnSelector);

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
     * @throws Exception 
     * @throws NullPointerException if the given tableId is null
     */
    void truncateTable(Session session, int tableId);
}
