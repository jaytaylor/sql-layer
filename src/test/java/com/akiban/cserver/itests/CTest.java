package com.akiban.cserver.itests;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.NoSuchRowException;
import com.akiban.cserver.api.dml.TableDefinitionMismatchException;
import com.akiban.cserver.api.dml.scan.*;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static junit.framework.Assert.*;

public final class CTest extends ApiTestBase
{

    @Test
    public void simpleScan() throws InvalidOperationException {
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);
        dml().writeRow( createNewRow(tableId, 1, "foo bear") );
        expectRowCount(tableId, 2);

        Session session = new SessionImpl();
        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        ListRowOutput output = new ListRowOutput();

        assertEquals("cursors", cursorSet(), dml().getCursors(session));
        CursorId cursorId = dml().openCursor(request, session);
        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session));
        assertEquals("state", CursorState.FRESH, dml().getCursorState(cursorId, session));

        boolean hasMore1 = dml().scanSome(cursorId, session, output, 1);
        assertTrue("more rows expected", hasMore1);
        assertEquals("state", CursorState.SCANNING, dml().getCursorState(cursorId, session));

        boolean hasMore2 = dml().scanSome(cursorId, session, output, -1);
        assertFalse("more rows found", hasMore2);
        assertEquals("state", CursorState.FINISHED, dml().getCursorState(cursorId, session));

        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session));
        dml().closeCursor(cursorId, session);
        assertEquals("cursors", cursorSet(), dml().getCursors(session));

        List<NewRow> expectedRows = new ArrayList<NewRow>();
        expectedRows.add( createNewRow(tableId, 0L, "hello world") );
        expectedRows.add( createNewRow(tableId, 1L, "foo bear") );
        assertEquals("rows scanned", expectedRows, output.getRows());
    }

    @Test
    public void partialRowScan() throws InvalidOperationException {
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( createNewRow(tableId, 0, "hello world") );
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
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        Session session = new SessionImpl();
        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0)); // partial scan requested
        LegacyRowOutput output = new WrappingRowOutput(ByteBuffer.allocate(1024 * 1024));
        CursorId cursorId = dml().openCursor(request, session);

        dml().scanSome(cursorId, session, output, 1);
        assertEquals("rows read", 1, output.getRowsCount());
        dml().closeCursor(cursorId, session);

        List<NewRow> expectedRows = new ArrayList<NewRow>();
        expectedRows.add( createNewRow(tableId, 0L, "hello world") ); // full scan expected
        RowData rowData = new RowData(output.getOutputBuffer().array(), 0, output.getOutputBuffer().position());
        rowData.prepareRow(0);
        assertEquals("table ID", tableId.getTableId(null), rowData.getRowDefId());
        List<NewRow> converted = dml().convertRowDatas(Arrays.asList(rowData));
        assertEquals("rows scanned", expectedRows, converted);
    }
    
    @Test
    public void testDropTable() throws InvalidOperationException {
        final TableId tableId1 = createTable("testSchema", "customer", "id int key");
        ddl().dropTable(TableId.of("testSchema", "customer"));

        AkibaInformationSchema ais = ddl().getAIS();
        assertNull("expected no table", ais.getUserTable("testSchema", "customer"));
        ddl().dropTable(tableId1); // should be a no-op, just testing it doesn't fail

        NoSuchTableException caught = null;
        try {
            dml().openCursor(new ScanAllRequest(tableId1, ColumnSet.ofPositions(0)), new SessionImpl());
        } catch (NoSuchTableException e) {
            caught = e;
        }
        assertNotNull("expected NoSuchTableException", caught);
    }

    @Test
    public void scanEmptyTable() throws InvalidOperationException {
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        Session session = new SessionImpl();
        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        ListRowOutput output = new ListRowOutput();

        assertEquals("cursors", cursorSet(), dml().getCursors(session));
        CursorId cursorId = dml().openCursor(request, session);
        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session));
        assertEquals("state", CursorState.FRESH, dml().getCursorState(cursorId, session));

        boolean hasMore = dml().scanSome(cursorId, session, output, 1);
        assertFalse("no more rows expected", hasMore);
        assertEquals("state", CursorState.FINISHED, dml().getCursorState(cursorId, session));

        CursorIsFinishedException caught = null;
        try {
            dml().scanSome(cursorId, session, output, 0);
        } catch (CursorIsFinishedException e) {
            caught = e;
        }
        assertNotNull("expected an exception", caught);

        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session));
        dml().closeCursor(cursorId, session);
        assertEquals("cursors", cursorSet(), dml().getCursors(session));

        assertEquals("rows scanned", Collections.<NewRow>emptyList(), output.getRows());
    }

    @Test
    public void updateNoChangeToHKey() throws InvalidOperationException {
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request, createNewRow(tableId, 0L, "hello world") );

        dml().updateRow( createNewRow(tableId, 0, "hello world"), createNewRow(tableId, 0, "goodbye cruel world") );
        expectRowCount(tableId, 1);
        expectRows(request, createNewRow(tableId, 0L, "goodbye cruel world") );
    }

    @Test
    public void updateOldOnlyById() throws InvalidOperationException {
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request, createNewRow(tableId, 0L, "hello world") );

        dml().updateRow( createNewRow(tableId, 0), createNewRow(tableId, 1, "goodbye cruel world") );
        expectRowCount(tableId, 1);
        expectRows(request, createNewRow(tableId, 1L, "goodbye cruel world") );
    }

    @Test(expected=NoSuchRowException.class)
    public void updateOldNotById() throws InvalidOperationException {
        final TableId tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

            expectRowCount(tableId, 0);
            dml().writeRow( createNewRow(tableId, 0, "hello world") );
            expectRowCount(tableId, 1);

            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "hello world") );
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            NiceRow old = new NiceRow(tableId);
            old.put(ColumnId.of(1), "hello world");
            dml().updateRow( old, createNewRow(tableId, 1, "goodbye cruel world") );
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
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request, createNewRow(tableId, 0L, "hello world") );

        dml().updateRow( createNewRow(tableId, 0), createNewRow(tableId, 1) );
        expectRowCount(tableId, 1);
        expectRows(new ScanAllRequest(tableId, ColumnSet.ofPositions(0)), createNewRow(tableId, 1L) );
    }

    @Test(expected=TableDefinitionMismatchException.class)
    public void updateOldNewHasWrongType() throws InvalidOperationException {
        final TableId tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

            expectRowCount(tableId, 0);
            dml().writeRow( createNewRow(tableId, 0, "hello world") );
            expectRowCount(tableId, 1);

            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "hello world") );
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            dml().updateRow(
                    createNewRow(tableId, 0, "hello world"),
                    createNewRow(tableId, "zero", "1234") );
        } catch (TableDefinitionMismatchException e) {
            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "hello world") );
            throw e;
        }
        fail("expected exception. rows are now: " + scanAll(new ScanAllRequest(tableId, null)));
    }

    @Test(expected=TableDefinitionMismatchException.class)
    public void insertHasWrongType() throws InvalidOperationException {
        final TableId tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");
            expectRowCount(tableId, 0);
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            dml().writeRow( createNewRow(tableId, "zero", 123) );
        } catch (TableDefinitionMismatchException e) {
            expectRowCount(tableId, 0);
            expectRows(new ScanAllRequest(tableId, null));
            throw e;
        }
        fail("expected exception. rows are now: " + scanAll(new ScanAllRequest(tableId, null)));
    }

    @Test(expected=TableDefinitionMismatchException.class)
    public void insertStringTooLong() throws InvalidOperationException {
        final TableId tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int key, name varchar(5)");
            expectRowCount(tableId, 0);
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            dml().writeRow( createNewRow(tableId, 0, "this string is longer than five characters") );
        } catch (TableDefinitionMismatchException e) {
            expectRowCount(tableId, 0);
            expectRows(new ScanAllRequest(tableId, null));
            throw e;
        }
        fail("expected exception. rows are now: " + scanAll(new ScanAllRequest(tableId, null)));
    }

    @Test
    public void updateChangesHKey() throws InvalidOperationException {
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request, createNewRow(tableId, 0L, "hello world") );

        dml().updateRow( createNewRow(tableId, 0), createNewRow(tableId, 1, "goodbye cruel world") );
        expectRowCount(tableId, 1);
        expectRows(request, createNewRow(tableId, 1L, "goodbye cruel world") );
    }

    @Test
    public void deleteRows() throws InvalidOperationException {
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( createNewRow(tableId, 0, "doomed row") );
        expectRowCount(tableId, 1);
        dml().writeRow( createNewRow(tableId, 1, "also doomed") );
        expectRowCount(tableId, 2);

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
        expectRows(request,
                createNewRow(tableId, 0L, "doomed row"),
                createNewRow(tableId, 1L, "also doomed"));

        dml().deleteRow( createNewRow(tableId, 0L, "doomed row") );
        expectRowCount(tableId, 1);
        expectRows(request,
                createNewRow(tableId, 1L, "also doomed"));

        dml().deleteRow( createNewRow(tableId, 1L) );
        expectRowCount(tableId, 0);
        expectRows(request);
    }

    @Test(expected=NoSuchRowException.class)
    public void deleteRowNotById() throws InvalidOperationException {
        final TableId tableId;
        try{
            tableId = createTable("theschema", "c", "id int key, name varchar(32)");

            expectRowCount(tableId, 0);
            dml().writeRow( createNewRow(tableId, 0, "the customer's name") );
            expectRowCount(tableId, 1);

            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "the customer's name"));
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            NiceRow deleteAttempt = new NiceRow(tableId);
            deleteAttempt.put(ColumnId.of(1), "the customer's name");
            dml().deleteRow(deleteAttempt);
        } catch (NoSuchRowException e) {
            ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1));
            expectRows(request, createNewRow(tableId, 0L, "the customer's name"));
            throw e;
        }
    }

    @Test(expected=NoSuchRowException.class)
    public void deleteMissingRow()  throws InvalidOperationException {
        final TableId tableId;
        try{
            tableId = createTable("theschema", "c", "id int key, name varchar(32)");
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            NiceRow deleteAttempt = new NiceRow(tableId);
            deleteAttempt.put(ColumnId.of(1), "the customer's name");
            dml().deleteRow( createNewRow(tableId, 0, "this row doesn't exist"));
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
        final TableId tableId = createTable("testSchema", "customer", "id int key, name varchar(32)");

        expectRowCount(tableId, 0);
        dml().writeRow( createNewRow(tableId, 0, "hello world") );
        expectRowCount(tableId, 1);
        dml().truncateTable(tableId);
        expectRowCount(tableId, 0);
    }
}
