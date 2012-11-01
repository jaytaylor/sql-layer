/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
    private final boolean fullRowOutput;
    private volatile ApiTestBase.TestRowOutput output;

    DelayableScanCallable(int aisGeneration,
                          int tableId, int indexId,
                          DelayerFactory topOfLoopDelayer, DelayerFactory beforeConversionDelayer,
                          boolean markFinish, long initialDelay, long finishDelay, boolean markOpenCursor, boolean fullRowOutput)
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
}
