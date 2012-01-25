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

package com.akiban.util.tap;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tap implementation that aggregates information on a per-thread basis. An
 * instance of this class maintains a collection of subordinate Taps, one
 * per thread. These are created on demand as new threads invoke
 * {@link #in()} or {@link #out()}.
 * <p />
 * By default each subordinate Tap is a {@link TimeAndCount}, but the
 * two-argument constructor provides a way to override that default.
 *
 */
class PerThread extends Tap {

    private static final Comparator<Thread> THREAD_COMPARATOR = new Comparator<Thread>() {
        @Override
        public int compare(Thread o1, Thread o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };
    private final ThreadLocal<Tap> threadLocal = new ThreadLocal<Tap>() {
        @Override
        protected Tap initialValue() {
            Tap tap;
            try {
                tap = clazz.getConstructor(String.class).newInstance(name);
            } catch (Exception e) {
                tap = new Null(name);
                LOG.warn("Unable to create tap of class " + clazz.getSimpleName(), e);
            }
            threadMap.put(Thread.currentThread(), tap);
            return tap;
        }
    };

    private final Map<Thread, Tap> threadMap = new ConcurrentHashMap<Thread, Tap>();

    private final Class<? extends Tap> clazz;

    /**
     * {@link com.akiban.util.tap.TapReport} subclass that contains map of subordinate TapReport
     * instances, one per thread. The map key is threadName for simple
     * correlation with
     *
     * @author peter
     *
     */
    public static class PerThreadTapReport extends TapReport {

        private final Map<String, TapReport> reportMap = new HashMap<String, TapReport>();

        PerThreadTapReport(final String name, final long inCount,
                final long outCount, final long elapsedTime,
                Map<Thread, Tap> threadMap) {
            super(name, inCount, outCount, elapsedTime);
            for (final Map.Entry<Thread, Tap> entry : threadMap.entrySet()) {
                this.reportMap.put(
                        entry.getKey().getName(),
                        entry.getValue().getReport()
                );
            }
        }

        public Map<String, TapReport> getTapReportMap() {
            return new TreeMap<String, TapReport>(reportMap);
        }
    }

    /**
     * Create a PerThread instance the adds a new instance of the supplied
     * subclass of {@link com.akiban.util.tap.Tap} for each Thread. Note: the class
     * {@link com.akiban.util.tap.PerThread} may not itself be added.
     *
     * @param name
     *            Name of the Tap
     * @param clazz
     *            Class from which new Tap instances are created.
     */
    public PerThread(final String name, final Class<? extends Tap> clazz) {
        super(name);
        if (clazz == this.getClass()) {
            throw new IllegalArgumentException("May not add a "
                    + clazz.getName() + " to " + this);
        }
        this.clazz = clazz;
    }

    @Override
    public void appendReport(StringBuilder sb) {
        final Map<Thread, Tap> threadMap = new TreeMap<Thread, Tap>(THREAD_COMPARATOR);
        threadMap.putAll(this.threadMap);
        for (final Map.Entry<Thread, Tap> entry : threadMap.entrySet()) {
            sb.append(NEW_LINE);
            sb.append("==");
            sb.append(entry.getKey().getName());
            sb.append("==");
            sb.append(NEW_LINE);
            entry.getValue().appendReport(sb);
        }
    }

    @Override
    public TapReport getReport() {
        long inCount = 0;
        long outCount = 0;
        long elapsedTime = 0;
        for (final Tap tap : threadMap.values()) {
            final TapReport threadTapReport = tap.getReport();
            inCount += threadTapReport.getInCount();
            outCount += threadTapReport.getOutCount();
            elapsedTime += threadTapReport.getCumulativeTime();
        }
        return new PerThreadTapReport(name, inCount, outCount, elapsedTime, threadMap);
    }

    @Override
    public void in() {
        final Tap tap = getTap();
        tap.in();
    }

    @Override
    public void out() {
        final Tap tap = getTap();
        tap.out();
    }

    @Override
    public long getDuration() {
        final Tap tap = getTap();
        return tap.getDuration();
    }

    @Override
    public void reset() {
        for (final Tap tap : threadMap.values()) {
            tap.reset();
        }
    }

    @Override
    public String toString() {
        return "PerThread(" + clazz.getName() + ")";
    }

    private Tap getTap() {
        return threadLocal.get();
    }
}
