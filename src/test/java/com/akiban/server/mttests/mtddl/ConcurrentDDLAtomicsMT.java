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
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.mttests.mtutil.TimePoints;
import com.akiban.server.mttests.mtutil.TimePointsComparison;
import com.akiban.server.mttests.mtutil.TimedCallable;
import com.akiban.server.mttests.mtutil.Timing;
import com.akiban.server.mttests.mtutil.TimedResult;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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

public final class ConcurrentDDLAtomicsMT extends ConcurrentAtomicsBase {

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

        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(0, SCAN_WAIT, "SCAN: PAUSE").get(ddl());

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
                "SCAN: FINISH"
        );

        List<NewRow> rowsScanned = scanResult.getItem();
        assertEquals("rows scanned size", 1, rowsScanned.size());
        assertEquals("rows[0] fields", expectedFields, rowsScanned.get(0).getFields());
    }

    @Test
    public void rowConvertedAfterTableDrop() throws Exception {
        final String index = "PRIMARY";
        final int tableId = tableWithTwoRows();
        final int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex(index).getIndexId();

        DelayScanCallableBuilder callableBuilder = new DelayScanCallableBuilder(tableId, indexId)
            .markFinish(false)
            .beforeConversionDelayer(new DelayerFactory() {
                @Override
                public Delayer delayer(TimePoints timePoints) {
                    return new Delayer(timePoints, 0, 5000)
                            .markBefore(1, "SCAN: PAUSE")
                            .markAfter(1, "SCAN: CONVERTED");
                }
            });
        TimedCallable<List<NewRow>> scanCallable = callableBuilder.get(ddl());

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
                "SCAN: CONVERTED"
        );

        List<NewRow> rowsScanned = scanResult.getItem();
        List<NewRow> rowsExpected = Arrays.asList(
                createNewRow(tableId, 1L, "the snowman"),
                createNewRow(tableId, 2L, "mr melty")
        );
        assertEquals("rows scanned (in order)", rowsExpected, rowsScanned);
    }

    @Test
    public void scanPKWhileDropping() throws Exception {
        scanWhileDropping("PRIMARY");
    }

    @Test
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

        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, nameIndexId).topOfLoopDelayer(2, 5000, "SCAN: PAUSE").get(ddl());

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

        DelayScanCallableBuilder callableBuilder = new DelayScanCallableBuilder(tableId, indexId)
            .topOfLoopDelayer(1, 100, "SCAN: FIRST")
            .initialDelay(2500)
            .markFinish(false);
        TimedCallable<List<NewRow>> scanCallable = callableBuilder.get(ddl());
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

    @Test
    public void dropIndexWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("name").getIndexId();

        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(0, SCAN_WAIT, "SCAN: PAUSE").get(ddl());

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
                "SCAN: FINISH"
        );

        List<NewRow> rowsScanned = scanResult.getItem();
        List<NewRow> rowsExpected = Arrays.asList(
                createNewRow(tableId, 2L, "mr melty")
        );
        assertEquals("rows scanned (in order)", rowsExpected, rowsScanned);
    }

    @Test(timeout=60000)
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
            TimedCallable<List<NewRow>> scanCallable = new DelayScanCallableBuilder(tableId, index.getIndexId()).get(ddl());

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
                assertEquals("number of rows scanned", NUMBER_OF_ROWS, scanResult.getItem().size());
            }
            else {
                fail(comparison.getMarkNames().toString());
            }
            ddl().createIndexes(session(), Collections.singleton(index));
        }
    }
}
