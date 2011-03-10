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

import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.EasyUseColumnSelector;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.RowOutputException;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.mttests.Util;
import com.akiban.server.mttests.Util.TimedCallable;
import com.akiban.server.mttests.Util.TimedResult;
import com.akiban.server.mttests.Util.TimePoints;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class DdlDmlMT extends ApiTestBase {

    private static final String SCHEMA = "cold";
    private static final String TABLE = "frosty";

    @Test
    public void dropIndexWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 10000;

        int indexId = ddl().getUserTable(session, new TableName(SCHEMA, TABLE)).getIndex("name").getIndexId();
        TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, indexId, SCAN_WAIT);
        TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints) throws Exception {
                TableName table = new TableName(SCHEMA, TABLE);
                Util.sleep(2000);
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

        new Util.TimePointsComparison(scanResult, dropIndexResult).verify(
                "SCAN: START",
                "SCAN: PAUSE",
                "INDEX: DROP>",
                "INDEX: <DROP",
                "SCAN: FINISH"
        );

        List<NewRow> rowsScanned = scanResult.getItem();
        List<NewRow> rowsExpected = Arrays.asList(
                createNewRow(tableId, 2L, "mr melty"),
                createNewRow(tableId, 1L, "the snowman")
        );
        assertEquals("rows scanned (in order)", rowsExpected, rowsScanned);

        assertTrue("time took " + scanResult.getTime(), scanResult.getTime() >= SCAN_WAIT);
        assertTrue("time took " + dropIndexResult.getTime(), dropIndexResult.getTime() >= SCAN_WAIT);
    }

    @Test
    public void updateRowWhileScanning() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 10000;

        int indexId = ddl().getUserTable(session, new TableName(SCHEMA, TABLE)).getIndex("PRIMARY").getIndexId();
        TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, indexId, SCAN_WAIT);
        TimedCallable<Void> updateCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall(TimePoints timePoints) throws Exception {
                NewRow old = new NiceRow(tableId);
                old.put(0, 2L);
                NewRow updated = new NiceRow(tableId);
                updated.put(0, 2L);
                updated.put(1, "icebox");
                Util.sleep(2000);
                timePoints.mark("UPDATE: IN");
                dml().updateRow(new SessionImpl(), old, updated, new EasyUseColumnSelector(1));
                timePoints.mark("UPDATE: OUT");
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> updateFuture = executor.submit(updateCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> updateResult = updateFuture.get();

        new Util.TimePointsComparison(scanResult, updateResult).verify(
                "SCAN: START",
                "SCAN: PAUSE",
                "UPDATE: IN",
                "UPDATE: OUT",
                "SCAN: FINISH"
        );

        List<NewRow> rowsScanned = scanResult.getItem();
        List<NewRow> rowsExpected = Arrays.asList(
                createNewRow(tableId, 1L, "the snowman"),
                createNewRow(tableId, 2L, "mr melty")
        );
        assertEquals("rows scanned (in order)", rowsExpected, rowsScanned);

        expectFullRows(tableId,
                createNewRow(tableId, 1L, "the snowman"),
                createNewRow(tableId, 2L, "icebox")
        );
        
        assertTrue("time took " + scanResult.getTime(), scanResult.getTime() >= SCAN_WAIT);
        assertTrue("time took " + updateResult.getTime(), updateResult.getTime() >= SCAN_WAIT);

    }

    private TimedCallable<List<NewRow>> getScanCallable(final int tableId, final int indexId, final int sleepBetween) {
        return new TimedCallable<List<NewRow>>() {
            @Override
            protected List<NewRow> doCall(TimePoints timePoints) throws Exception {
                Session session = new SessionImpl();
                ScanAllRequest request = new ScanAllRequest(
                        tableId,
                        new HashSet<Integer>(Arrays.asList(0, 1)),
                        indexId,
                        EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
                );
                CursorId cursorId = dml().openCursor(session, request);
                CountingRowOutput output = new CountingRowOutput();
                timePoints.mark("SCAN: START");
                dml().scanSome(session, cursorId, output, 1);
                timePoints.mark("SCAN: PAUSE");
                Util.sleep(sleepBetween);
                dml().scanSome(session, cursorId, output, -1);
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
