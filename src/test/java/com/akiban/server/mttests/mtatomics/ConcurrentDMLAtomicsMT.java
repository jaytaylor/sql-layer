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

package com.akiban.server.mttests.mtatomics;

import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.FixedCountLimit;
import com.akiban.server.api.dml.EasyUseColumnSelector;
import com.akiban.server.api.dml.NoSuchIndexException;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.BufferedLegacyOutputRouter;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.LegacyOutputConverter;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.api.dml.scan.WrappingRowOutput;
import com.akiban.server.mttests.mtutil.TimePoints;
import com.akiban.server.mttests.mtutil.TimedCallable;
import com.akiban.server.mttests.mtutil.TimedResult;
import com.akiban.server.mttests.mtutil.Timing;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Test;

import java.nio.ByteBuffer;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class ConcurrentDMLAtomicsMT extends ConcurrentAtomicsBase {
    @Test
    public void updateUnIndexedColumnWhileScanning() throws Exception {
        final int tableId = tableWithThreeRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("PRIMARY").getIndexId();
        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(1, SCAN_WAIT, "SCAN: PAUSE").get(ddl());
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
                        createNewRow(tableId, 2L, "icebox"),
                        createNewRow(tableId, 99L, "zebras in snow")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "the snowman"),
                        createNewRow(tableId, 2L, "icebox"),
                        createNewRow(tableId, 99L, "zebras in snow")
                )
        );
    }

    @Test
    public void updatePKColumnWhileScanning() throws Exception {
        final int tableId = tableWithThreeRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("PRIMARY").getIndexId();
        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(1, SCAN_WAIT, "SCAN: PAUSE").get(ddl());
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
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 5L, "the snowman"),
                        createNewRow(tableId, 99L, "zebras in snow")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "mr melty"),
                        createNewRow(tableId, 5L, "the snowman"),
                        createNewRow(tableId, 99L, "zebras in snow")
                )
        );
    }

    @Test
    public void updateIndexedColumnWhileScanning() throws Exception {
        final int tableId = tableWithThreeRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("name").getIndexId();
        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(1, SCAN_WAIT, "SCAN: PAUSE").get(ddl());
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
                        createNewRow(tableId, 1L, "a snowman"),
                        createNewRow(tableId, 2L, "xtreme weather"),
                        createNewRow(tableId, 99L, "zebras in snow")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "a snowman"),
                        createNewRow(tableId, 2L, "xtreme weather"),
                        createNewRow(tableId, 99L, "zebras in snow")
                )
        );
    }

    @Test
    public void updateIndexedColumnAndPKWhileScanning() throws Exception {
        final int tableId = tableWithThreeRows();
        final int SCAN_WAIT = 5000;

        int indexId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE)).getIndex("name").getIndexId();
        TimedCallable<List<NewRow>> scanCallable
                = new DelayScanCallableBuilder(tableId, indexId).topOfLoopDelayer(1, SCAN_WAIT, "SCAN: PAUSE").get(ddl());
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
                        createNewRow(tableId, 2L, "xtreme weather"),
                        createNewRow(tableId, 10L, "a snowman"),
                        createNewRow(tableId, 99L, "zebras in snow")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "xtreme weather"),
                        createNewRow(tableId, 10L, "a snowman"),
                        createNewRow(tableId, 99L, "zebras in snow")
                )
        );
    }

    @Test//(expected=ConcurrentScanAndUpdateException.class) // TODO uncomment after merge
    public void multipleScanSomeCalls() throws Throwable {
        final int SCAN_WAIT = 5000;
        final int tableId = tableWithTwoRows();
        final int pkId = ddl().getUserTable(session(), new TableName(SCHEMA, TABLE))
                .getPrimaryKey().getIndex().getIndexId();
        final int size = findOneRowBufferSize(tableId, pkId);

        TimedCallable<List<NewRow>> scanCallable = new MultiScanSomeCallable(size, tableId, pkId, SCAN_WAIT, dml());
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

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<TimedResult<List<NewRow>>> scanFuture = executor.submit(scanCallable);
        Future<TimedResult<Void>> updateFuture = executor.submit(updateCallable);

        updateFuture.get();
        Throwable scanFutureException = null;
        try {
            scanFuture.get();
        } catch (ExecutionException e) {
            scanFutureException = e.getCause();
        }

        expectFullRows(
                tableId,
                createNewRow(tableId, 2L, "xtreme weather"),
                createNewRow(tableId, 10L, "a snowman")
        );

        if (scanFutureException != null) {
            throw scanFutureException;
        }
    }

    private int findOneRowBufferSize(int tableId, int indexId) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1024); // should be plenty
        buffer.mark();
        LegacyRowOutput output = new WrappingRowOutput(buffer);

        ScanAllRequest request = new ScanAllRequest(
                tableId,
                new HashSet<Integer>(Arrays.asList(0, 1)),
                indexId,
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END),
                new FixedCountLimit(1)
        );
        CursorId cursorId = dml().openCursor(session(), request);
        dml().scanSome(session(), cursorId, output);
        dml().closeCursor(session(), cursorId);
        assertFalse("buffer pos still 0", buffer.position() == 0);
        return buffer.position();
    }

    /**
     * This class duplicates some of DelayScanCallable, but it adds enough that I felt it would over-complicate
     * DelayableScanCallable too much to put that logic there.
     */
    private static class MultiScanSomeCallable extends TimedCallable<List<NewRow>> {
        private final int tableId;
        private final int indexId;
        private final long delayTime;
        private final int size;
        private final DMLFunctions dml;

        private MultiScanSomeCallable(int size, int tableId, int indexId, long delayTime, DMLFunctions dml) {
            this.tableId = tableId;
            this.indexId = indexId;
            this.delayTime = delayTime;
            this.size = size;
            this.dml = dml;
        }

        @Override
        protected List<NewRow> doCall(TimePoints timePoints, Session session) throws Exception {
            BufferedLegacyOutputRouter smallRouter = new BufferedLegacyOutputRouter(size, false);
            LegacyOutputConverter converter = new LegacyOutputConverter(dml);
            ListRowOutput output = new ListRowOutput();
            converter.setOutput(output);
            converter.setColumnsToScan(new HashSet<Integer>(Arrays.asList(0, 1)));
            smallRouter.addHandler(converter);


            ScanAllRequest request = new ScanAllRequest(
                    tableId,
                    new HashSet<Integer>(Arrays.asList(0, 1)),
                    indexId,
                    EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END),
                    ScanLimit.NONE
            );

            final CursorId cursorId;
            try {
                cursorId = dml.openCursor(session, request);
            } catch (NoSuchIndexException e) {
                timePoints.mark("SCAN: NO SUCH INDEX");
                return Collections.emptyList();
            }

            timePoints.mark("SCAN: START");
            boolean bufferFilled = false;
            try {
                dml.scanSome(session, cursorId, smallRouter);
            } catch (BufferFullException e) {
                bufferFilled = true;
            }
            assertTrue("should have had more!", bufferFilled);
            assertEquals("rows scanned", 1, output.getRows().size());
            NewRow row = output.getRows().get(0);
            assertTrue("row isn't NiceRow: " + row.getClass(), row instanceof NiceRow);

            timePoints.mark("(SCAN: PAUSE)>");
            Timing.sleep(delayTime);
            timePoints.mark("<(SCAN: PAUSE)");

            BufferedLegacyOutputRouter bigRouter = new BufferedLegacyOutputRouter(1024, false);
            bigRouter.addHandler(converter);
            timePoints.mark("SCAN: RETRY");
            dml.scanSome(session, cursorId, bigRouter);

            dml.closeCursor(session, cursorId);
            timePoints.mark("SCAN: FINISH");

            return output.getRows();
        }
    }

    protected int tableWithThreeRows() throws InvalidOperationException {
        int tableId = tableWithTwoRows();
        writeRows(
                createNewRow(tableId, 99L, "zebras in snow")
        );
        return tableId;
    }
}
