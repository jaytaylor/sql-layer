package com.foundationdb.util;

import java.sql.SQLException;

public abstract class MicroBenchmark
{
    public void beforeAction() throws Exception
    {}

    public void afterAction() throws Exception
    {}

    public abstract void action() throws Exception;

    public String name()
    {
        return getClass().getSimpleName();
    }

    public final double run() throws Exception
    {
        while (!converged()) {
            beforeAction();
            long start = System.nanoTime();
            action();
            long stop = System.nanoTime();
            afterAction();
            history[cycles++ % history.length] = stop - start;
        }
        return historyAverage;
    }

    public final void profile() throws Exception
    {
        while (true) {
            beforeAction();
            action();
            afterAction();
        }
    }

    protected MicroBenchmark(int historySize, double maxVariation)
    {
        this.maxVariation = maxVariation;
        this.history = new long[historySize];
    }

    private boolean converged()
    {
        boolean converged = false;
        if (cycles >= history.length) {
            long sum = 0;
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (int i = 0; i < history.length; i++) {
                sum += history[i];
                min = Math.min(min, history[i]);
                max = Math.max(max, history[i]);
            }
            historyAverage = (double) sum / history.length;
            converged =
                min >= historyAverage * (1 - maxVariation) &&
                max <= historyAverage * (1 + maxVariation);
        }
        if (converged) {
            // In case this benchmark is rerun
            cycles = 0;
        }
        return converged;
    }

    private final double maxVariation;
    private long[] history;
    private double historyAverage;
    private int cycles = 0;
}
