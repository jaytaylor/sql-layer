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

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.NoSuchIndexException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.mttests.mtutil.TimePoints;
import com.akiban.server.mttests.mtutil.TimedCallable;
import com.akiban.server.mttests.mtutil.Timing;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.d_l.DStarLService;
import com.akiban.server.service.d_l.ScanhooksDStarLService;
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
    private final DDLFunctions ddl;
    private final DelayerFactory topOfLoopDelayer;
    private final DelayerFactory beforeConversionDelayer;
    private final boolean markFinish;
    private final long finishDelay;
    private final long initialDelay;

    DelayableScanCallable(int tableId, int indexId, DDLFunctions ddl,
                          DelayerFactory topOfLoopDelayer, DelayerFactory beforeConversionDelayer,
                          boolean markFinish, long initialDelay, long finishDelay)
    {
        this.tableId = tableId;
        this.indexId = indexId;
        this.ddl = ddl;
        this.topOfLoopDelayer = topOfLoopDelayer;
        this.beforeConversionDelayer = beforeConversionDelayer;
        this.markFinish = markFinish;
        this.initialDelay = initialDelay;
        this.finishDelay = finishDelay;
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

        ScanhooksDStarLService.ScanHooks scanHooks = new ScanhooksDStarLService.ScanHooks() {
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
        DStarLService dstarLService = ServiceManagerImpl.get().getDStarL();
        ScanhooksDStarLService scanhooksService = (ScanhooksDStarLService) dstarLService;
        assertNull("previous scanhook defined!", scanhooksService.installHook(session, scanHooks));
        try {
            final CursorId cursorId;
            DMLFunctions dml = ServiceManagerImpl.get().getDStarL().dmlFunctions();
            try {
                cursorId = dml.openCursor(session, request);
            } catch (NoSuchIndexException e) {
                timePoints.mark("SCAN: NO SUCH INDEX");
                return Collections.emptyList();
            }
            ApiTestBase.ListRowOutput output = new ApiTestBase.ListRowOutput();
            timePoints.mark("SCAN: START");
            if (dml.scanSome(session, cursorId, output)) {
                timePoints.mark("SCAN: EARLY FINISH");
                return output.getRows();
            }
            dml.closeCursor(session, cursorId);

            if (scanhooksService.isHookInstalled(session)) {
                throw new ScanHooksNotRemovedException();
            }
            return output.getRows();
        } catch (Exception e) {
            if (scanhooksService.isHookInstalled(session)) {
                throw new ScanHooksNotRemovedException(e);
            }
            else throw e;
        }
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
