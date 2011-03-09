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
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.RowOutputException;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.mttests.Util;
import com.akiban.server.mttests.Util.TimedCallable;
import com.akiban.server.mttests.Util.TimedResult;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class DdlDmlMT extends ApiTestBase {

    /**
     * Tests dropping an index while a scan is going on. This is done with some guesswork, but with enough leeway
     * that it should provide consistent results.
     *
     * The control flow is:
     * <pre>
     * Thread A    |  Thread B
     * ============|============
     * start scan  |  wait 5 sec
     * ------------|    4...
     * wait 10 sec |    3...
     *   9...      |    2...
     *   8...      |    1...
     *   7...      |------------
     *   6...      | drop index
     *   5...      |------------
     *   4...      |
     *   3...      |
     *   2...      |
     *   1...      |
     * ------------|
     * finish scan |
     * ============|============
     * </pre>
     * @throws Exception not expected
     */
    @Test
    public void dropIndexWaitsForScan() throws Exception {
        final int tableId = tableWithTwoRows();
        final int SCAN_WAIT = 10000;

        int indexId = ddl().getUserTable(session, new TableName("cold", "frosty")).getIndex("name").getIndexId();
        TimedCallable<List<NewRow>> scanCallable = getScanCallable(tableId, indexId, SCAN_WAIT);
        TimedCallable<Void> dropIndexCallable = new TimedCallable<Void>() {
            @Override
            protected Void doCall() throws Exception {
                TableName table = new TableName("cold", "frosty");
                Util.sleep(2000);
                ddl().dropIndexes(new SessionImpl(), table, Collections.singleton("name"));
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> dropIndexFuture = executor.submit(dropIndexCallable);

        TimedResult<List<NewRow>> scanResult = scanFuture.get();
        TimedResult<Void> dropIndexResult = dropIndexFuture.get();

        List<NewRow> rowsScanned = scanResult.getItem();
        List<NewRow> rowsExpected = Arrays.asList(
                createNewRow(tableId, 2L, "mr melty"),
                createNewRow(tableId, 1L, "the snowman")
        );
        assertEquals("rows scanned (in order)", rowsExpected, rowsScanned);

        assertTrue("time took " + scanResult.getTime(), scanResult.getTime() >= SCAN_WAIT);
        assertTrue("time took " + dropIndexResult.getTime(), dropIndexResult.getTime() >= SCAN_WAIT);
    }

    private TimedCallable<List<NewRow>> getScanCallable(final int tableId, final int indexId, final int sleepBetween) {
        return new TimedCallable<List<NewRow>>() {
            @Override
            protected List<NewRow> doCall() throws Exception {
                Session session = new SessionImpl();
                ScanAllRequest request = new ScanAllRequest(
                        tableId,
                        new HashSet<Integer>(Arrays.asList(0, 1)),
                        indexId,
                        EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
                );
                CursorId cursorId = dml().openCursor(session, request);
                CountingRowOutput output = new CountingRowOutput();
                dml().scanSome(session, cursorId, output, 1);
                Util.sleep(sleepBetween);
                dml().scanSome(session, cursorId, output, -1);

                return output.rows;
            }
        };
    }

    int tableWithTwoRows() throws InvalidOperationException {
        int id = createTable("cold", "frosty", "id int key", "name varchar(32)", "key(name)");
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
