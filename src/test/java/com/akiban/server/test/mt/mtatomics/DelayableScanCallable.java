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

package com.akiban.server.test.mt.mtatomics;

import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.test.ApiTestBase;
import com.akiban.server.test.mt.mtutil.TimePoints;
import com.akiban.server.test.mt.mtutil.TimedCallable;
import com.akiban.server.test.mt.mtutil.Timing;
import com.akiban.server.service.dxl.ConcurrencyAtomicsDXLService;
import com.akiban.server.service.session.Session;

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
    private volatile ApiTestBase.ListRowOutput output;

    DelayableScanCallable(int aisGeneration,
                          int tableId, int indexId,
                          DelayerFactory topOfLoopDelayer, DelayerFactory beforeConversionDelayer,
                          boolean markFinish, long initialDelay, long finishDelay, boolean markOpenCursor)
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
    }

    private Delayer topOfLoopDelayer(TimePoints timePoints) {
        return topOfLoopDelayer == null ? null : topOfLoopDelayer.delayer(timePoints);
    }

    private Delayer beforeConversionDelayer(TimePoints timePoints){
        return beforeConversionDelayer == null ? null : beforeConversionDelayer.delayer(timePoints);
    }

    @Override
    protected final List<NewRow> doCall(final TimePoints timePoints, Session session) throws Exception {
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
            public void retryHook() {
                timePoints.mark("SCAN: RETRY");
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
                new HashSet<Integer>(Arrays.asList(0, 1)),
                indexId,
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END),
                ScanLimit.NONE
        );

        assertNull("previous scanhook defined!", ConcurrencyAtomicsDXLService.installScanHook(session, scanHooks));
        assertTrue("scanhook not installed correctly", ConcurrencyAtomicsDXLService.isScanHookInstalled(session));
        try {
            final CursorId cursorId;
            DMLFunctions dml = ServiceManagerImpl.get().getDXL().dmlFunctions();
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
            output = new ApiTestBase.ListRowOutput();
            timePoints.mark("SCAN: START");
            dml.scanSome(session, cursorId, output);
            dml.closeCursor(session, cursorId);

            if (ConcurrencyAtomicsDXLService.isScanHookInstalled(session)) {
                throw new ScanHooksNotRemovedException();
            }
            return output.getRows();
        } catch (Exception e) {
            timePoints.mark("SCAN: exception " + e.getClass().getSimpleName());
            if (ConcurrencyAtomicsDXLService.isScanHookInstalled(session)) {
                throw new ScanHooksNotRemovedException(e);
            }
            else throw e;
        }
    }

    public List<NewRow> getRows() {
        ApiTestBase.ListRowOutput outputLocal = output;
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
}
