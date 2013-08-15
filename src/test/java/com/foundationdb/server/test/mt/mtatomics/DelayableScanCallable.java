/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.test.mt.mtatomics;

import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.ScanAllRequest;
import com.foundationdb.server.api.dml.scan.ScanFlag;
import com.foundationdb.server.api.dml.scan.ScanLimit;
import com.foundationdb.server.service.ServiceManagerImpl;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.test.ApiTestBase;
import com.foundationdb.server.test.mt.mtutil.TimePoints;
import com.foundationdb.server.test.mt.mtutil.TimedCallable;
import com.foundationdb.server.test.mt.mtutil.Timing;
import com.foundationdb.server.service.dxl.ConcurrencyAtomicsDXLService;
import com.foundationdb.server.service.session.Session;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;

class DelayableScanCallable extends TimedCallable<List<NewRow>> {
    private final int tableId;
    private final int indexId;
    private final DelayerFactory topOfLoopDelayer;
    private final DelayerFactory beforeConversionDelayer;
    private final boolean markFinish;
    private final long finishDelay;
    private final long initialDelay;
    private final int aisGeneration;
    private final boolean markOpenCursor;
    private final boolean fullRowOutput;
    private final boolean explicitTransaction;
    private volatile ApiTestBase.TestRowOutput output;

    DelayableScanCallable(int aisGeneration,
                          int tableId, int indexId,
                          DelayerFactory topOfLoopDelayer, DelayerFactory beforeConversionDelayer,
                          boolean markFinish, long initialDelay, long finishDelay, boolean markOpenCursor,
                          boolean fullRowOutput, boolean explicitTransaction)
    {
        this.aisGeneration = aisGeneration;
        this.tableId = tableId;
        this.indexId = indexId;
        this.topOfLoopDelayer = topOfLoopDelayer;
        this.beforeConversionDelayer = beforeConversionDelayer;
        this.markFinish = markFinish;
        this.initialDelay = initialDelay;
        this.finishDelay = finishDelay;
        this.markOpenCursor = markOpenCursor;
        this.fullRowOutput = fullRowOutput;
        this.explicitTransaction = explicitTransaction;
    }

    private Delayer topOfLoopDelayer(TimePoints timePoints) {
        return topOfLoopDelayer == null ? null : topOfLoopDelayer.delayer(timePoints);
    }

    private Delayer beforeConversionDelayer(TimePoints timePoints){
        return beforeConversionDelayer == null ? null : beforeConversionDelayer.delayer(timePoints);
    }

    @Override
    protected final List<NewRow> doCall(final TimePoints timePoints, Session session) throws Exception {
        if(!explicitTransaction) {
            return doCallInternal(timePoints, session);
        }
        txn().beginTransaction(session);
        timePoints.mark("TXN: BEGAN");
        boolean success = false;
        try {
            List<NewRow> rows = doCallInternal(timePoints, session);
            txn().commitTransaction(session);
            timePoints.mark("TXN: COMMITTED");
            success = true;
            return rows;
        } finally {
            if(!success) {
                txn().rollbackTransaction(session);
                timePoints.mark("TXN: ROLLEDBACK");
            }
        }
    }

    private List<NewRow> doCallInternal(final TimePoints timePoints, Session session) throws Exception {
        Timing.sleep(initialDelay);
        final Delayer topOfLoopDelayer = topOfLoopDelayer(timePoints);
        final Delayer beforeConversionDelayer = beforeConversionDelayer(timePoints);

        ConcurrencyAtomicsDXLService.ScanHooks scanHooks = new ConcurrencyAtomicsDXLService.ScanHooks() {
            @Override
            public void loopStartHook() {
                if (topOfLoopDelayer != null) {
                    topOfLoopDelayer.delay();
                }
            }

            @Override
            public void preWroteRowHook() {
                if (beforeConversionDelayer != null) {
                    beforeConversionDelayer.delay();
                }
            }

            @Override
            public void scanSomeFinishedWellHook() {
                if (markFinish) {
                    timePoints.mark("SCAN: FINISH");
                }
                Timing.sleep(finishDelay);
            }
        };
        ScanAllRequest request = new ScanAllRequest(
                tableId,
                new HashSet<>(Arrays.asList(0, 1)),
                indexId,
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END),
                ScanLimit.NONE
        );

        assertNull("previous scanhook defined!", ConcurrencyAtomicsDXLService.installScanHook(session, scanHooks));
        assertTrue("scanhook not installed correctly", ConcurrencyAtomicsDXLService.isScanHookInstalled(session));
        try {
            final CursorId cursorId;
            DMLFunctions dml = dml();
            try {
                if (markOpenCursor) {
                    timePoints.mark("(SCAN: OPEN CURSOR)>");
                }
                cursorId = dml.openCursor(session, aisGeneration, request);
                if (markOpenCursor) {
                    timePoints.mark("<(SCAN: OPEN CURSOR)");
                }
            } catch (Exception e) {
                ConcurrencyAtomicsDXLService.ScanHooks removed = ConcurrencyAtomicsDXLService.removeScanHook(session);
                if (removed != scanHooks) {
                    throw new RuntimeException("hook not removed correctly", e);
                }
                throw e;
            }
            output = fullRowOutput ? new ApiTestBase.ListRowOutput() : new ApiTestBase.CountingRowOutput();
            timePoints.mark("SCAN: START");
            dml.scanSome(session, cursorId, output);
            dml.closeCursor(session, cursorId);

            if (ConcurrencyAtomicsDXLService.isScanHookInstalled(session)) {
                throw new ScanHooksNotRemovedException();
            }
            return fullRowOutput ? getRows() : null;
        } catch (Exception e) {
            timePoints.mark("SCAN: exception " + e.getClass().getSimpleName());
            if (ConcurrencyAtomicsDXLService.isScanHookInstalled(session)) {
                throw new ScanHooksNotRemovedException(e);
            }
            else throw e;
        }
    }

    public int getRowCount() {
        return (output != null) ? output.getRowCount() : 0;
    }

    public List<NewRow> getRows() {
        if(!fullRowOutput) {
            throw new IllegalArgumentException("No rows to get (constructed without fullRowOutput)");
        }
        ApiTestBase.ListRowOutput outputLocal = (ApiTestBase.ListRowOutput)output;
        return outputLocal == null ? Collections.<NewRow>emptyList() : outputLocal.getRows();
    }

    private static class ScanHooksNotRemovedException extends RuntimeException {
        private ScanHooksNotRemovedException() {
            super("scanhooks not removed!");
        }

        private ScanHooksNotRemovedException(Throwable cause) {
            super("scanhooks not removed!", cause);
        }
    }

    private static DMLFunctions dml() {
        return ServiceManagerImpl.get().getDXL().dmlFunctions();
    }

    private static TransactionService txn() {
        return ServiceManagerImpl.get().getServiceByClass(TransactionService.class);
    }
}
