
package com.akiban.server.test.mt.mtatomics;

import com.akiban.server.test.mt.mtutil.TimePoints;

class DelayScanCallableBuilder {

    private static final long DEFAULT_FINISH_DELAY = 750;

    private final int tableId;
    private final int indexId;
    private final int aisGeneration;

    private boolean markFinish = true;
    private boolean markOpenCursor = false;
    private long initialDelay = 0;
    private DelayerFactory topOfLoopDelayer;
    private DelayerFactory beforeConversionDelayer;
    private boolean fullRowOutput = true;
    private boolean explicitTxn = false;

    DelayScanCallableBuilder(int aisGeneration, int tableId, int indexId) {
        this.aisGeneration = aisGeneration;
        this.tableId = tableId;
        this.indexId = indexId;
    }

    DelayScanCallableBuilder topOfLoopDelayer(DelayerFactory delayer) {
        assert topOfLoopDelayer == null;
        topOfLoopDelayer = delayer;
        return this;
    }

    DelayScanCallableBuilder topOfLoopDelayer(int beforeRow, long delay, String message) {
        return topOfLoopDelayer(beforeRow, delay, String.format("(%s)>", message), String.format("<(%s)", message));
    }

    DelayScanCallableBuilder topOfLoopDelayer(int beforeRow, long delay, String messageIn, String messageOut) {
        return topOfLoopDelayer(singleDelayFactory(beforeRow, delay, messageIn, messageOut));
    }

    private DelayerFactory singleDelayFactory(final int beforeRow, final long delay,
                                              final String messageIn, final String messageOut)
    {
        return new DelayerFactory() {
            @Override
            public Delayer delayer(TimePoints timePoints) {
                long[] delays = new long[beforeRow+1];
                delays[beforeRow] = delay;
                return new Delayer(timePoints, delays)
                        .markBefore(beforeRow, messageIn)
                        .markAfter(beforeRow, messageOut);
            }
        };
    }

    DelayScanCallableBuilder initialDelay(long delay) {
        this.initialDelay = delay;
        return this;
    }

    DelayScanCallableBuilder markFinish(boolean markFinish) {
        this.markFinish = markFinish;
        return this;
    }

    DelayScanCallableBuilder markOpenCursor(boolean markOpenCursor) {
        this.markOpenCursor = markOpenCursor;
        return this;
    }

    DelayScanCallableBuilder beforeConversionDelayer(DelayerFactory delayer) {
        assert beforeConversionDelayer == null;
        beforeConversionDelayer = delayer;
        return this;
    }

    DelayScanCallableBuilder withFullRowOutput(boolean fullRowOutput) {
        this.fullRowOutput = fullRowOutput;
        return this;
    }

    DelayScanCallableBuilder withExplicitTxn(boolean explicitTxn) {
        this.explicitTxn = explicitTxn;
        return this;
    }

    DelayableScanCallable get() {
        return new DelayableScanCallable(
                aisGeneration,
                tableId, indexId,
                topOfLoopDelayer, beforeConversionDelayer,
                markFinish, initialDelay, DEFAULT_FINISH_DELAY,
                markOpenCursor,
                fullRowOutput,
                explicitTxn
        );
    }
}
