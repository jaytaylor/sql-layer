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

package com.akiban.server.itests.d_lfunctions;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.dml.NoSuchRowException;
import com.akiban.server.api.dml.TableDefinitionMismatchException;
import com.akiban.server.api.dml.scan.*;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static junit.framework.Assert.*;

public final class CBasicIT extends ApiTestBase {

    @Test
    public void simpleScan() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( session, createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);
        dml().writeRow( session, createNewRow(tableId, 1, "foo bear") );
        expectRowCount(tableId, 2);

        Session session = new SessionImpl();
        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        ListRowOutput output = new ListRowOutput();

        assertEquals("cursors", cursorSet(), dml().getCursors(session));
        CursorId cursorId = dml().openCursor(session, request);
        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session));
        assertEquals("state", CursorState.FRESH, dml().getCursorState(session, cursorId));

        boolean hasMore1 = dml().scanSome(session, cursorId, output, 1);
        assertTrue("more rows expected", hasMore1);
        assertEquals("state", CursorState.SCANNING, dml().getCursorState(session, cursorId));

        boolean hasMore2 = dml().scanSome(session, cursorId, output, -1);
        assertFalse("more rows found", hasMore2);
        assertEquals("state", CursorState.FINISHED, dml().getCursorState(session, cursorId));

        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session));
        dml().closeCursor(session, cursorId);
        assertEquals("cursors", cursorSet(), dml().getCursors(session));

        List<NewRow> expectedRows = new ArrayList<NewRow>();
        expectedRows.add( createNewRow(tableId, 0L, "hello world") );
        expectedRows.add( createNewRow(tableId, 1L, "foo bear") );
        assertEquals("rows scanned", expectedRows, output.getRows());
    }

    /*
     * There was a miscalculation in the ColumnSet pack to/from legacy conversions if the 8th bit
     * happened to be set. A bad if check would skip the byte all together and up to 8 columns would
     * no longer be in the set.
     */
    @Test
    public void simpleScanColumnMapConversionCheck() throws InvalidOperationException {
        final int tableId = createTable("test", "t",
                                        "c1 int key, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int");

        expectRowCount(tableId, 0);
        writeRows(createNewRow(tableId, 11, 12, 13, 14, 15, 16, 17, 18, 19),
                  createNewRow(tableId, 21, 22, 23, 24, 25, 26, 27, 28, 29),
                  createNewRow(tableId, 31, 32, 33, 34, 35, 36, 37, 38, 39));
        expectRowCount(tableId, 3);

        // Select 8th place in column map (index 7) which caused the byte to be <0 and skipped in a bad if check
        List<NewRow> rows = scanAll(new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 7, 8)));
        assertEquals("rows scanned", 3, rows.size());

        List<NewRow> expectedRows = new ArrayList<NewRow>();
        NiceRow r;
        r = new NiceRow(tableId); r.put(0, 11L); r.put(7, 18L); r.put(8, 19L); expectedRows.add(r);
        r = new NiceRow(tableId); r.put(0, 21L); r.put(7, 28L); r.put(8, 29L); expectedRows.add(r);
        r = new NiceRow(tableId); r.put(0, 31L); r.put(7, 38L); r.put(8, 39L); expectedRows.add(r);
        assertEquals("row content", expectedRows, rows);
    }

    @Test
    public void indexScan() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32), key name(name)");
        final int indexId = ddl().getTable(session, tableId).getIndex("name").getIndexId();

        expectRowCount(tableId, 0);
        dml().writeRow(session, createNewRow(tableId, 1, "foo"));
        dml().writeRow(session, createNewRow(tableId, 2, "bar"));
        dml().writeRow(session, createNewRow(tableId, 3, "zap"));
        expectRowCount(tableId, 3);

        List<NewRow> rows = scanAll(new ScanAllRequest(tableId, ColumnSet.ofPositions(1), indexId, null));
        assertEquals("rows scanned", 3, rows.size());

        List<NewRow> expectedRows = new ArrayList<NewRow>();
        expectedRows.add(createNewRow(tableId, 2L, "bar"));
        expectedRows.add(createNewRow(tableId, 1L, "foo"));
        expectedRows.add(createNewRow(tableId, 3L, "zap"));

        // Remove first column so toStrings match on assert below
        for(NewRow row : expectedRows) {
            row.remove(0);
        }

        assertEquals("row contents", expectedRows, rows);
    }

    @Test
    public void partialRowScan() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( session, createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0));
        expectRows(request, createNewRow(tableId, 0L) );
    }

    /**
     * Note that in legacy mode, even a partial scan request results in a full scan
     * @throws InvalidOperationException if something failed
     */
    @Test
    public void partialRowScanLegacy() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( session, createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        Session session = new SessionImpl();
        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0)); // partial scan requested
        LegacyRowOutput output = new WrappingRowOutput(ByteBuffer.allocate(1024 * 1024));
        CursorId cursorId = dml().openCursor(session, request);

        dml().scanSome(session, cursorId, output, 1);
        assertEquals("rows read", 1, output.getRowsCount());
        dml().closeCursor(session, cursorId);

        List<NewRow> expectedRows = new ArrayList<NewRow>();
        expectedRows.add( createNewRow(tableId, 0L, "hello world") ); // full scan expected
        RowData rowData = new RowData(output.getOutputBuffer().array(), 0, output.getOutputBuffer().position());
        rowData.prepareRow(0);
        assertEquals("table ID", tableId, rowData.getRowDefId());
        List<NewRow> converted = dml().convertRowDatas(Arrays.asList(rowData));
        assertEquals("rows scanned", expectedRows, converted);
    }
    
    @Test
    public void dropTable() throws InvalidOperationException {
        final int tableId1 = createTable("testSchema", "customer", "id int key");
        ddl().dropTable(session, tableName("testSchema", "customer"));

        AkibanInformationSchema ais = ddl().getAIS(session);
        assertNull("expected no table", ais.getUserTable("testSchema", "customer"));
        ddl().dropTable(session, tableName("testSchema", "customer")); // should be a no-op; testing it doesn't fail

        NoSuchTableException caught = null;
        try {
            dml().openCursor(session, new ScanAllRequest(tableId1, ColumnSet.ofPositions(0)));
        } catch (NoSuchTableException e) {
            caught = e;
        }
        assertNotNull("expected NoSuchTableException", caught);
    }

    @Test
    public void dropGroup() throws InvalidOperationException {
        final int tid = createTable("test", "t", "id int key");
        final String groupName = ddl().getAIS(session).getUserTable("test", "t").getGroup().getName();
        ddl().dropGroup(session, groupName);

        AkibanInformationSchema ais = ddl().getAIS(session);
        assertNull("expected no table", ais.getUserTable("test", "t"));
        assertNull("expected no group", ais.getGroup(groupName));

        ddl().dropGroup(session, groupName);

        NoSuchTableException caught = null;
        try {
            dml().openCursor(session, new ScanAllRequest(tid, ColumnSet.ofPositions(0)));
        } catch (NoSuchTableException e) {
            caught = e;
        }
        assertNotNull("expected NoSuchTableException", caught);
    }

    /*
     * Found from an actual case in the MTR test suite. Caused by recycled RowDefIDs and undeleted table statuses.
     * Really testing that table statuses get deleted, but about as direct as we can get from this level.
     */
    @Test
    public void dropThenCreateRowDefIDRecycled() throws InvalidOperationException {
        final int tidV1 = createTable("test", "t1", "id int key auto_increment, name varchar(255)");
        dml().writeRow(session, createNewRow(tidV1, 1, "hello world"));
        expectRowCount(tidV1, 1);
        ddl().dropTable(session, tableName(tidV1));

        // Easiest exception trigger was to toggle auto_inc column, failed when trying to update it
        final int tidV2 = createTable("test", "t2", "id int key, tag char(1), value decimal(10,2)");
        dml().writeRow(session, createNewRow(tidV2, "1", "a", "49.95"));
        expectRowCount(tidV2, 1);
        ddl().dropTable(session, tableName(tidV2));
    }

    @Test
    public void scanEmptyTable() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        Session session = new SessionImpl();
        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        ListRowOutput output = new ListRowOutput();

        assertEquals("cursors", cursorSet(), dml().getCursors(session));
        CursorId cursorId = dml().openCursor(session, request);
        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session));
        assertEquals("state", CursorState.FRESH, dml().getCursorState(session, cursorId));

        boolean hasMore = dml().scanSome(session, cursorId, output, 1);
        assertFalse("no more rows expected", hasMore);
        assertEquals("state", CursorState.FINISHED, dml().getCursorState(session, cursorId));

        CursorIsFinishedException caught = null;
        try {
            dml().scanSome(session, cursorId, output, 0);
        } catch (CursorIsFinishedException e) {
            caught = e;
        }
        assertNotNull("expected an exception", caught);

        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session));
        dml().closeCursor(session, cursorId);
        assertEquals("cursors", cursorSet(), dml().getCursors(session));

        assertEquals("rows scanned", Collections.<NewRow>emptyList(), output.getRows());
    }

    @Test
    public void updateNoChangeToHKey() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( session, createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request, createNewRow(tableId, 0L, "hello world") );

        dml().updateRow( session, createNewRow(tableId, 0, "hello world"), createNewRow(tableId, 0, "goodbye cruel world"), null);
        expectRowCount(tableId, 1);
        expectRows(request, createNewRow(tableId, 0L, "goodbye cruel world") );
    }

    @Test
    public void updateOldOnlyById() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow(session, createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request, createNewRow(tableId, 0L, "hello world") );

        dml().updateRow( session, createNewRow(tableId, 0), createNewRow(tableId, 1, "goodbye cruel world"), null);
        expectRowCount(tableId, 1);
        expectRows(request, createNewRow(tableId, 1L, "goodbye cruel world") );
    }

    @Test(expected=NoSuchRowException.class)
    public void updateOldNotById() throws InvalidOperationException {
        final int tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

            expectRowCount(tableId, 0);
            dml().writeRow( session, createNewRow(tableId, 0, "hello world") );
            expectRowCount(tableId, 1);

            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "hello world") );
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            NiceRow old = new NiceRow(tableId);
            old.put(1, "hello world");
            dml().updateRow( session, old, createNewRow(tableId, 1, "goodbye cruel world"), null );
        } catch (NoSuchRowException e) {
            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "hello world") );
            throw e;
        }
    }

    /**
     * We currently can't differentiate between null and unspecified, so not specifying a field is the same as
     * setting it null. Thus, by providing a truncated definition for both old and new rows, we're essentially
     * nulling some of the row as well as shortening it.
     * @throws InvalidOperationException if there's a failure
     */
    @Test
    public void updateRowPartially() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( session, createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request, createNewRow(tableId, 0L, "hello world") );

        dml().updateRow( session, createNewRow(tableId, 0), createNewRow(tableId, 1), null);
        expectRowCount(tableId, 1);
        expectRows(new ScanAllRequest(tableId, ColumnSet.ofPositions(0)), createNewRow(tableId, 1L) );
    }

    @Test(expected=TableDefinitionMismatchException.class)
    public void updateOldNewHasWrongType() throws InvalidOperationException {
        final int tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

            expectRowCount(tableId, 0);
            dml().writeRow( session, createNewRow(tableId, 0, "hello world") );
            expectRowCount(tableId, 1);

            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "hello world") );
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            dml().updateRow(
                    session, createNewRow(tableId, 0, "hello world"),
                    createNewRow(tableId, "zero", "1234"), null);
        } catch (TableDefinitionMismatchException e) {
            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "hello world") );
            throw e;
        }
        fail("expected exception. rows are now: " + scanAll(new ScanAllRequest(tableId, null)));
    }

    @Test(expected=TableDefinitionMismatchException.class)
    public void insertHasWrongType() throws InvalidOperationException {
        final int tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");
            expectRowCount(tableId, 0);
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            dml().writeRow( session, createNewRow(tableId, "zero", 123) );
        } catch (TableDefinitionMismatchException e) {
            expectRowCount(tableId, 0);
            expectRows(new ScanAllRequest(tableId, null));
            throw e;
        }
        fail("expected exception. rows are now: " + scanAll(new ScanAllRequest(tableId, null)));
    }

    @Test(expected=TableDefinitionMismatchException.class)
    public void insertStringTooLong() throws InvalidOperationException {
        final int tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int key, name varchar(5)");
            expectRowCount(tableId, 0);
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            dml().writeRow( session, createNewRow(tableId, 0, "this string is longer than five characters") );
        } catch (TableDefinitionMismatchException e) {
            expectRowCount(tableId, 0);
            expectRows(new ScanAllRequest(tableId, null));
            throw e;
        }
        fail("expected exception. rows are now: " + scanAll(new ScanAllRequest(tableId, null)));
    }

    @Test
    public void updateChangesHKey() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( session, createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request, createNewRow(tableId, 0L, "hello world") );

        dml().updateRow( session, createNewRow(tableId, 0), createNewRow(tableId, 1, "goodbye cruel world"), null);
        expectRowCount(tableId, 1);
        expectRows(request, createNewRow(tableId, 1L, "goodbye cruel world") );
    }

    @Test
    public void deleteRows() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( session, createNewRow(tableId, 0, "doomed row") );
        expectRowCount(tableId, 1);
        dml().writeRow( session, createNewRow(tableId, 1, "also doomed") );
        expectRowCount(tableId, 2);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request,
                createNewRow(tableId, 0L, "doomed row"),
                createNewRow(tableId, 1L, "also doomed"));

        dml().deleteRow( session, createNewRow(tableId, 0L, "doomed row") );
        expectRowCount(tableId, 1);
        expectRows(request,
                createNewRow(tableId, 1L, "also doomed"));

        dml().deleteRow( session, createNewRow(tableId, 1L) );
        expectRowCount(tableId, 0);
        expectRows(request);
    }

    @Test(expected=NoSuchRowException.class)
    public void deleteRowNotById() throws InvalidOperationException {
        final int tableId;
        try{
            tableId = createTable("theschema", "c", "id int key, name varchar(32)");

            expectRowCount(tableId, 0);
            dml().writeRow( session, createNewRow(tableId, 0, "the customer's name") );
            expectRowCount(tableId, 1);

            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "the customer's name"));
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            NiceRow deleteAttempt = new NiceRow(tableId);
            deleteAttempt.put(1, "the customer's name");
            dml().deleteRow(session, deleteAttempt);
        } catch (NoSuchRowException e) {
            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "the customer's name"));
            throw e;
        }
    }

    @Test(expected=NoSuchRowException.class)
    public void deleteMissingRow()  throws InvalidOperationException {
        final int tableId;
        try{
            tableId = createTable("theschema", "c", "id int key, name varchar(32)");
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            NiceRow deleteAttempt = new NiceRow(tableId);
            deleteAttempt.put(1, "the customer's name");
            dml().deleteRow( session, createNewRow(tableId, 0, "this row doesn't exist"));
        } catch (NoSuchRowException e) {
            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request);
            throw e;
        }
    }

    @Test
    public void schemaIdIncrements() throws Exception {
        int firstGen = ddl().getSchemaID().getGeneration();
        createTable("sch", "c1", "id int key");
        int secondGen = ddl().getSchemaID().getGeneration();
        assertTrue(String.format("failed %d > %d", secondGen, firstGen), secondGen > firstGen);
    }

    @Test
    public void truncate() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( session, createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);
        dml().truncateTable(session, tableId);
        expectRowCount(tableId, 0);
    }
}
