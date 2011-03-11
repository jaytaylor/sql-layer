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

package com.akiban.server.mttests.mtddl;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.EasyUseColumnSelector;
import com.akiban.server.api.dml.NoSuchIndexException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.RowOutputException;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.mttests.mtutil.TimePoints;
import com.akiban.server.mttests.mtutil.TimePointsComparison;
import com.akiban.server.mttests.mtutil.TimedCallable;
import com.akiban.server.mttests.mtutil.Timing;
import com.akiban.server.mttests.mtutil.TimedResult;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class ConcurrencyAtomicsMT extends ApiTestBase {

    private static final String SCHEMA = "cold";
    private static final String TABLE = "frosty";

    @Test
    public void dropTableWhileScanningPK() throws Exception {
        Map<Integer,Object> expectedFields = new HashMap<Integer, Object>();
        expectedFields.put(0, 1L);
        expectedFields.put(1, "the snowman");
        dropTableWhileScanning("PRIMARY", expectedFields);
    }

    @Test
    public void dropTableWhileScanningOnIndex() throws Exception {
        Map<Integer,Object> expectedFields = new HashMap<Integer, Object>();
        expectedFields.put(0, 2L);
        expectedFields.put(1, "mr melty");
        dropTableWhileScanning("name", expectedFields);
    }

    private void dropTableWhileScanning(String indexName, Map<Integer,Object> expectedFields) throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex(indexName).getIndexId();
        TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, indexId, SCAN_WAIT);
        TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                TableName table = new TableName(SCHEMA, TABLE);
                Timing.sleep(2000);
                timePoints.mark("TABLE: DROP>");
                ddl().dropTable(session, table);
                timePoints.mark("TABLE: <DROP");
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> dropIndexFuture = executor.submit(dropIndexCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> dropIndexResult = dropIndexFuture.get();

        new TimePointsComparison(scanResult, dropIndexResult).verify(
                "SCAN: START",
                "SCAN: PAUSE",
                "TABLE: DROP>",
                "TABLE: <DROP",
//                "SCAN: TABLE DROPPED", // TODO can't happen yet, but will need to
                "SCAN: FINISH"
        );

        List<NewRow> rowsScanned = scanResult.getItem();
        assertEquals("rows scanned size", 1, rowsScanned.size());
        assertEquals("rows[0] fields", expectedFields, rowsScanned.get(0).getFields());
    }

    @Test
    public void scanPKWhileDropping() throws Exception {
        scanWhileDropping("PRIMARY");
    }

    @Test @Ignore("bug 733003") // TODO
    public void scanIndexWhileDropping() throws Exception {
        scanWhileDropping("name");
    }

    @Test
    public void dropShiftsIndexIdWhileScanning() throws Exception {
        final int tableId = createTable(SCHEMA, TABLE, "id int key", "name varchar(32)", "age varchar(2)", "key(name)", "key(age)");
        writeRows(
                createNewRow(tableId, 2, "alpha", 3),
                createNewRow(tableId, 1, "bravo", 2),
                createNewRow(tableId, 3, "charlie", 1)
                // the above are listed in order of index #1 (the name index)
                // after that index is dropped, index #1 is age, and that will come in this order:
                // (3, charlie 1)
                // (1, bravo, 2)
                // (2, alpha, 3)
                // We'll get to the 2nd index (bravo) when we drop the index, and we want to make sure we don't
                // continue scanning with alpha (which would thus badly order name)
        );
        final TableName tableName = new TableName(SCHEMA, TABLE);
        Index nameIndex = ddl().getUserTable(session(), tableName).getIndex("name");
        Index ageIndex = ddl().getUserTable(session(), tableName).getIndex("age");
        assertTrue("age index's ID relative to name's", ageIndex.getIndexId() == nameIndex.getIndexId() + 1);
        final int nameIndexId = nameIndex.getIndexId();
        TimedCallable<List<NewRow>> scanCallable = new TimedCallable<List<NewRow>>() {
            @Override
            protected List<NewRow> doCall(TimePoints timePoints, Session session) throws Exception {
                ScanAllRequest request = new ScanAllRequest(
                        tableId,
                        new HashSet<Integer>(Arrays.asList(0, 1)),
                        nameIndexId,
                        EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
                );
                final CursorId cursorId = dml().openCursor(session, request);
                CountingRowOutput output = new CountingRowOutput();
                timePoints.mark("SCAN: START");
                if (!dml().scanSome(session, cursorId, output, 2)) {
                    timePoints.mark("SCAN: EARLY FINISH");
                    return output.rows;
                }
                timePoints.mark("SCAN: PAUSE");
                Timing.sleep(5000);
                dml().scanSome(session, cursorId, output, -1);
                dml().closeCursor(session, cursorId);
                timePoints.mark("SCAN: FINISH");

                return output.rows;
            }
        };

        TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                Timing.sleep(2500);
                timePoints.mark("DROP: IN");
                ddl().dropIndexes(session, tableName, Collections.singleton("name"));
                timePoints.mark("DROP: OUT");
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> dropIndexFuture = executor.submit(dropIndexCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> dropIndexResult = dropIndexFuture.get();

        assertEquals("age's index ID",
                nameIndexId,
                ddl().getUserTable(session(), tableName).getIndex("age").getIndexId().intValue()
        );

        new TimePointsComparison(scanResult, dropIndexResult).verify(
                "SCAN: START",
                "SCAN: PAUSE",
                "DROP: IN",
                "DROP: OUT",
                "SCAN: FINISH"
        );

        newRowsOrdered(scanResult.getItem(), 1);
    }

    private void newRowsOrdered(List<NewRow> rows, final int fieldIndex) {
        assertTrue("not enough rows: " + rows, rows.size() > 1);
        List<NewRow> ordered = new ArrayList<NewRow>(rows);
        Collections.sort(ordered, new Comparator<NewRow>() {
            @Override @SuppressWarnings("unchecked")
            public int compare(NewRow o1, NewRow o2) {
                Object o1Field = o1.getFields().get(fieldIndex);
                Object o2Field = o2.getFields().get(fieldIndex);
                if (o1Field == null) {
                    return o2Field == null ? 0 : -1;
                }
                if (o2Field == null) {
                    return 1;
                }
                Comparable o1Comp = (Comparable)o1Field;
                Comparable o2Comp = (Comparable)o2Field;
                return o1Comp.compareTo(o2Comp);
            }
        });
    }

    private void scanWhileDropping(String indexName) throws InvalidOperationException, InterruptedException, ExecutionException {
        final int tableId = largeEnoughTable(5000);
        final TableName tableName = new TableName(SCHEMA, TABLE);
        final int indexId = ddl().getUserTable(session(), tableName).getIndex(indexName).getIndexId();

        TimedCallable<List<NewRow>> scanCallable = delayThenScanAll(tableId, indexId);
        TimedCallable<Void> dropCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                timePoints.mark("DROP: IN");
                ddl().dropTable(session, tableName);
                timePoints.mark("DROP: OUT");
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> updateFuture = executor.submit(dropCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> updateResult = updateFuture.get();

        new TimePointsComparison(scanResult, updateResult).verify(
                "DROP: IN",
                "SCAN: START",
                "SCAN: FIRST",
                "DROP: OUT"
        );

        List<NewRow> rows = scanResult.getItem();
        assertFalse("rows were empty!", rows.isEmpty());
        int nulls = 0;
        for (NewRow row : rows) {
            Map<Integer,Object> fields = row.getFields();
            Long id = (Long)fields.get(0);
            Object name = fields.get(1);
            assertEquals("fields size", 2, row.getFields().size());
            assertEquals("string component", id == null ? null : id.toString(), name == null ? null : name.toString());
            if (id == null) {
                ++nulls;
            }
        }
        assertTrue(String.format("saw %d nulls (out of %d rows)", nulls, rows.size()), nulls == 0);
    }

    private TimedCallable<List<NewRow>> delayThenScanAll(final int tableId, final int indexId) {
        return new TimedCallable<List<NewRow>>() {
            @Override
            protected List<NewRow> doCall(TimePoints timePoints, Session session) throws Exception {
                Timing.sleep(2500);
                ScanAllRequest request = new ScanAllRequest(
                        tableId,
                        new HashSet<Integer>(Arrays.asList(0, 1)),
                        indexId,
                        EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
                );
                final CursorId cursorId = dml().openCursor(session, request);
                CountingRowOutput output = new CountingRowOutput();
                timePoints.mark("SCAN: START");
                assertTrue(dml().scanSome(session, cursorId, output, 1));
                timePoints.mark("SCAN: FIRST");
                while (dml().scanSome(session, cursorId, output, -1)) {
                    // do nothing
                }
                dml().closeCursor(session, cursorId);

                return output.rows;
            }
        };
    }

    /**
     * Creates a table with enough rows that it takes a while to drop it
     * @param msForDropping how long (at least) it should take to drop this table
     * @return the table's id
     * @throws InvalidOperationException if ever encountered
     */
    private int largeEnoughTable(long msForDropping) throws InvalidOperationException {
        int rowCount;
        long dropTime;
        float factor = 1.5f; // after we write N rows, we'll write an additional (factor-1)*N rows as buffer
        do {
            int tableId = createTable(SCHEMA, TABLE, "id int key", "name varchar(32)", "key(name)");
            rowCount = 1;
            final long writeStart = System.currentTimeMillis();
            while (System.currentTimeMillis() - writeStart < msForDropping) {
                writeRows(
                        createNewRow(tableId, rowCount, Integer.toString(rowCount))
                );
                ++rowCount;
            }
            for(int i = rowCount; i < (int) factor * rowCount ; ++i) {
                writeRows(
                        createNewRow(tableId, i, Integer.toString(i))
                );
            }
            final long dropStart = System.currentTimeMillis();
            ddl().dropTable(session(), new TableName(SCHEMA, TABLE));
            dropTime = System.currentTimeMillis() - dropStart;
            factor += 0.2;
        } while(dropTime < msForDropping);

        int tableId = createTable(SCHEMA, TABLE, "id int key", "name varchar(32)", "key(name)");
        for(int i = 1; i < rowCount ; ++i) {
            writeRows(
                    createNewRow(tableId, i, Integer.toString(i))
            );
        }

        return tableId;
    }

    @Test @Ignore // TODO bug 732950
    public void dropIndexWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("name").getIndexId();
        TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, indexId, SCAN_WAIT);
        TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                TableName table = new TableName(SCHEMA, TABLE);
                Timing.sleep(2000);
                timePoints.mark("INDEX: DROP>");
                ddl().dropIndexes(new SessionImpl(), table, Collections.singleton("name"));
                timePoints.mark("INDEX: <DROP");
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> dropIndexFuture = executor.submit(dropIndexCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> dropIndexResult = dropIndexFuture.get();

        new TimePointsComparison(scanResult, dropIndexResult).verify(
                "SCAN: START",
                "SCAN: PAUSE",
                "INDEX: DROP>",
                "INDEX: <DROP",
                "SCAN: INDEX DROPPED" // TODO can't happen yet, but will need to
        );

        List<NewRow> rowsScanned = scanResult.getItem();
        List<NewRow> rowsExpected = Arrays.asList(
                createNewRow(tableId, 2L, "mr melty")
        );
        assertEquals("rows scanned (in order)", rowsExpected, rowsScanned);
    }

    @Test(timeout=60000) @Ignore("bug 732950") // TODO
    public void scanWhileDroppingIndex() throws Exception {
        final int NUMBER_OF_ROWS = 10000;
        final int initialTableId = createTable(SCHEMA, TABLE, "id int key", "age int", "key(age)");
        final TableName tableName = new TableName(SCHEMA, TABLE);
        for(int i=0; i < NUMBER_OF_ROWS; ++i) {
            writeRows(createNewRow(initialTableId, i, i + 1));
        }

        while(true) {
            final Index index = ddl().getUserTable(session(), tableName).getIndex("age");
            final Collection<String> indexNameCollection = Collections.singleton(index.getIndexName().getName());
            final int tableId = ddl().getTableId(session(), tableName);
            assertEquals("table id changed", initialTableId, tableId);
            
            TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
                @Override
                protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                    timePoints.mark("DROP: IN");
                    ddl().dropIndexes(new SessionImpl(), tableName, indexNameCollection);
                    timePoints.mark("DROP: OUT");
                    return null;
                }
            };
            TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, index.getIndexId(), 0);

            ExecutorService executor = Executors.newFixedThreadPool(2);
            Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
            Future<TimedResult<Void>> dropIndexFuture = executor.submit(dropIndexCallable);

            TimedResult<List<NewRow>> scanResult = scanFuture.get();
            TimedResult<Void> dropIndexResult = dropIndexFuture.get();

            TimePointsComparison comparison = new TimePointsComparison(scanResult, dropIndexResult);
            if (comparison.matches( // drop came before scan
                    "DROP: IN",
                    "SCAN: NO SUCH INDEX",
                    "DROP: OUT"
            )) {
                return; // this is what we wanted to find
            }
            else if (comparison.matches("SCAN: START")) { // scan came before drop
                if (comparison.getMarkNames().contains("SCAN: INDEX DROPPED")) {
                    // TODO can't happen yet, but will need to
                    assertTrue(String.format("rows scanned >= %d", NUMBER_OF_ROWS),
                            scanResult.getItem().size() < NUMBER_OF_ROWS);
                }
                else {
                    assertEquals("number of rows scanned", NUMBER_OF_ROWS, scanResult.getItem().size());
                }
            }
            else {
                fail(comparison.getMarkNames().toString());
            }
            ddl().createIndexes(session(), Collections.singleton(index));
        }
    }

    @Test
    public void updatePKColumnWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("PRIMARY").getIndexId();
        TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, indexId, SCAN_WAIT);
        TimedCallable<Void> updateCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                NewRow old = new NiceRow(tableId);
                old.put(0, 1L);
                NewRow updated = new NiceRow(tableId);
                updated.put(0, 5L);
                Timing.sleep(2000);
                timePoints.mark("UPDATE: IN");
                dml().updateRow(new SessionImpl(), old, updated, new EasyUseColumnSelector(0));
                timePoints.mark("UPDATE: OUT");
                return null;
            }
        };

        scanUpdateConfirm(
                tableId,
                scanCallable,
                updateCallable,
                Arrays.asList(
                        createNewRow(tableId, 1L, "the snowman"),
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 5L, "the snowman")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 5L, "the snowman")
                )
        );
    }

    @Test @Ignore("bug 732871") // TODO
    public void updateIndexedColumnWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("name").getIndexId();
        TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, indexId, SCAN_WAIT);
//        scanCallable = TransactionalTimedCallable.withRunnable( scanCallable, 10, 1000 );
//        scanCallable = TransactionalTimedCallable.withoutRunnable(scanCallable);

        TimedCallable<Void> updateCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                NewRow oldSnowman = new NiceRow(tableId);
                oldSnowman.put(0, 2L);
                NewRow updatedSnowman = new NiceRow(tableId);
                updatedSnowman.put(0, 2L);
                updatedSnowman.put(1, "xtreme weather");

                NewRow oldMr = new NiceRow(tableId);
                oldMr.put(0, 1L);
                NewRow updatedMr = new NiceRow(tableId);
                updatedMr.put(1, "a snowman");

                Timing.sleep(2000);
                timePoints.mark("UPDATE: IN");
                dml().updateRow(new SessionImpl(), oldSnowman, updatedSnowman, new EasyUseColumnSelector(1));
                dml().updateRow(new SessionImpl(), oldMr, updatedMr, new EasyUseColumnSelector(1));
                timePoints.mark("UPDATE: OUT");
                return null;
            }
        };

        scanUpdateConfirm(
                tableId,
                scanCallable,
                updateCallable,
                Arrays.asList(
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 2L, "xtreme weather")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "a snowman"),
                        createNewRow(tableId, 2L, "xtreme weather")
                )
        );
    }

    @Test @Ignore("bug 732871") // TODO
    public void updateIndexedColumnAndPKWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("name").getIndexId();
        TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, indexId, SCAN_WAIT);
//        scanCallable = TransactionalTimedCallable.withRunnable( scanCallable, 10, 1000 );
//        scanCallable = TransactionalTimedCallable.withoutRunnable(scanCallable);

        TimedCallable<Void> updateCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                NewRow oldSnowman = new NiceRow(tableId);
                oldSnowman.put(0, 2L);
                NewRow updatedSnowman = new NiceRow(tableId);
                updatedSnowman.put(0, 2L);
                updatedSnowman.put(1, "xtreme weather");

                NewRow oldMr = new NiceRow(tableId);
                oldMr.put(0, 1L);
                NewRow updatedMr = new NiceRow(tableId);
                updatedMr.put(0, 10L);
                updatedMr.put(1, "a snowman");

                Timing.sleep(2000);
                timePoints.mark("UPDATE: IN");
                dml().updateRow(new SessionImpl(), oldSnowman, updatedSnowman, new EasyUseColumnSelector(1));
                dml().updateRow(new SessionImpl(), oldMr, updatedMr, new EasyUseColumnSelector(0, 1));
                timePoints.mark("UPDATE: OUT");
                return null;
            }
        };

        scanUpdateConfirm(
                tableId,
                scanCallable,
                updateCallable,
                Arrays.asList(
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 2L, "xtreme weather")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "xtreme weather"),
                        createNewRow(tableId, 10L, "a snowman")
                )
        );
    }

    @Test
    public void updateUnIndexedColumnWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("PRIMARY").getIndexId();
        TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, indexId, SCAN_WAIT);
        TimedCallable<Void> updateCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints, Session session) throws Exception {
                NewRow old = new NiceRow(tableId);
                old.put(0, 2L);
                NewRow updated = new NiceRow(tableId);
                updated.put(0, 2L);
                updated.put(1, "icebox");
                Timing.sleep(2000);
                timePoints.mark("UPDATE: IN");
                dml().updateRow(new SessionImpl(), old, updated, new EasyUseColumnSelector(1));
                timePoints.mark("UPDATE: OUT");
                return null;
            }
        };

        scanUpdateConfirm(
                tableId,
                scanCallable,
                updateCallable,
                Arrays.asList(
                        createNewRow(tableId, 1L, "the snowman"),
                        createNewRow(tableId, 2L, "icebox")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "the snowman"),
                        createNewRow(tableId, 2L, "icebox")
                )
        );
    }

    private void scanUpdateConfirm(int tableId,
                                   TimedCallable<List<NewRow>> scanCallable,
                                   TimedCallable<Void> updateCallable,
                                   List<NewRow> scanCallableExpected,
                                   List<NewRow> endStateExpected)
            throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> updateFuture = executor.submit(updateCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> updateResult = updateFuture.get();

        new TimePointsComparison(scanResult, updateResult).verify(
                "SCAN: START",
                "SCAN: PAUSE",
                "UPDATE: IN",
                "UPDATE: OUT",
                "SCAN: FINISH"
        );

        assertEquals("rows scanned (in order)", scanCallableExpected, scanResult.getItem());
        expectFullRows(tableId, endStateExpected.toArray(new NewRow[endStateExpected.size()]));
    }

    private TimedCallable<List<NewRow>> getScanCallable(final int tableId, final int indexId, final int sleepBetween) {
        return new TimedCallable<List<NewRow>>() {
            @Override
            protected List<NewRow> doCall(TimePoints timePoints, Session session) throws Exception {
                ScanAllRequest request = new ScanAllRequest(
                        tableId,
                        new HashSet<Integer>(Arrays.asList(0, 1)),
                        indexId,
                        EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
                );
                final CursorId cursorId;
                try {
                     cursorId = dml().openCursor(session, request);
                } catch (NoSuchIndexException e) {
                    timePoints.mark("SCAN: NO SUCH INDEX");
                    return Collections.emptyList();
                }
                CountingRowOutput output = new CountingRowOutput();
                timePoints.mark("SCAN: START");
                if (!dml().scanSome(session, cursorId, output, 1)) {
                    timePoints.mark("SCAN: EARLY FINISH");
                    return output.rows;
                }
                if (sleepBetween > 0) {
                    timePoints.mark("SCAN: PAUSE");
                    Timing.sleep(sleepBetween);
                }
                dml().scanSome(session, cursorId, output, -1);
                dml().closeCursor(session, cursorId);
                timePoints.mark("SCAN: FINISH");

                return output.rows;
            }
        };
    }

    int tableWithTwoRows() throws InvalidOperationException {
        int id = createTable(SCHEMA, TABLE, "id int key", "name varchar(32)", "key(name)");
        writeRows(
            createNewRow(id, 1L, "the snowman"),
            createNewRow(id, 2L, "mr melty")
        );
        return id;
    }


    private static class CountingRowOutput implements RowOutput {
        private final List<NewRow> rows = new ArrayList<NewRow>();
        @Override
        public void output(NewRow row) throws RowOutputException {
           rows.add(row);
        }
    }
}
